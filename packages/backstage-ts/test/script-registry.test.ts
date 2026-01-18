/**
 * ScriptRegistry tests
 */
import { describe, test, expect, beforeAll, afterAll } from 'bun:test';
import { ScriptRegistry, type ScriptDef } from '../src/script-registry';
import { type RedisClient } from '../src/types';

let redis: RedisClient;
let registry: ScriptRegistry;

beforeAll(async () => {
  redis = new Bun.RedisClient('redis://localhost:6379');
  registry = new ScriptRegistry(redis);
});

afterAll(async () => {
  const keys = await redis.send('KEYS', ['backstage:script:*']);
  if (keys && Array.isArray(keys) && keys.length > 0) {
    await redis.send('DEL', keys as string[]);
  }
});

describe('ScriptRegistry', () => {
  test('loads script correctly', async () => {
    const scriptName = 'test-load';
    await registry.load({
      [scriptName]: {
        script: 'return ARGV[1]',
        keys: {},
      },
    });

    expect(registry.has(scriptName)).toBe(true);
    const script = registry.get(scriptName);
    expect(script).toBeTruthy();
    expect(script?.sha).toBeTruthy();
  });

  test('executes script with arguments', async () => {
    const scriptName = 'test-args';
    await registry.load({
      [scriptName]: {
        script: 'return {KEYS[1], ARGV[1]}',
        keys: { testKey: 1 },
      },
    });

    const result = await registry.run<[string, string]>(
      scriptName,
      { testKey: 'my-key' },
      ['my-arg'],
    );

    expect(result).toHaveLength(2);
    expect(result[0]).toBe('my-key');
    expect(result[1]).toBe('my-arg');
  });

  test('handles NOSCRIPT error automatically', async () => {
    const scriptName = 'test-noscript';
    await registry.load({
      [scriptName]: {
        script: 'return "recovered"',
        keys: {},
      },
    });

    // Manually flush scripts from Redis to simulate restart/cache eviction
    await redis.send('SCRIPT', ['FLUSH']);

    // Registry still has the SHA, but Redis doesn't
    // expected behavior: execute -> fail with NOSCRIPT -> reload -> retry -> success

    const result = await registry.run<string>(scriptName, {}, []);
    expect(result).toBe('recovered');

    // Check that SHA might have changed or at least is valid again
    const script = registry.get(scriptName);
    expect(script?.sha).toBeTruthy();
  });

  test('throws on missing keys', async () => {
    const scriptName = 'test-missing-keys';
    await registry.load({
      [scriptName]: {
        script: 'return KEYS[1]',
        keys: { requiredKey: 1 },
      },
    });

    const promise = registry.run(scriptName, {}, []);
    expect(promise).rejects.toThrow("Missing required key 'requiredKey'");
  });

  test('throws on unregistered script', async () => {
    const promise = registry.run('non-existent', {}, []);
    expect(promise).rejects.toThrow("Script 'non-existent' is not registered");
  });

  test('validates key indices', async () => {
    const scriptName = 'test-bad-indices';
    await registry.load({
      [scriptName]: {
        script: 'return "ok"',
        keys: {
          key1: 1,
          key3: 3, // Skipping 2
        },
      },
    });

    const promise = registry.run(scriptName, { key1: 'k1', key3: 'k3' }, []);
    expect(promise).rejects.toThrow('Missing key for index 2');
  });

  test('Lua execution error propagates', async () => {
    const scriptName = 'test-lua-error';
    await registry.load({
      [scriptName]: {
        script: 'return redis.call("INVALID_COMMAND")',
        keys: {},
      },
    });

    const promise = registry.run(scriptName, {}, []);
    expect(promise).rejects.toThrow();
  });
});
