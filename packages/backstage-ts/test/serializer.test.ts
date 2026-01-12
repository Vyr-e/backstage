/**
 * Serializer edge cases
 */
import { describe, test, expect } from 'bun:test';
import { serialize, deserialize } from '../src/serializer';

describe('Serializer Edge Cases', () => {
  test('primitives roundtrip', () => {
    expect(deserialize<string>(serialize('hello'))).toBe('hello');
    expect(deserialize<number>(serialize(123))).toBe(123);
    expect(deserialize<boolean>(serialize(true))).toBe(true);
    expect(deserialize<null>(serialize(null))).toBe(null);
  });

  test('BigInt roundtrip', () => {
    const big = BigInt('9007199254740993');
    expect(deserialize<bigint>(serialize(big))).toBe(big);
  });

  test('very large BigInt', () => {
    const huge = BigInt('12345678901234567890123456789012345678901234567890');
    expect(deserialize<bigint>(serialize(huge))).toBe(huge);
  });

  test('negative BigInt', () => {
    const negative = BigInt('-9007199254740993');
    expect(deserialize<bigint>(serialize(negative))).toBe(negative);
  });

  test('Date roundtrip', () => {
    const date = new Date('2024-01-10T12:00:00.123Z');
    const result = deserialize<Date>(serialize(date));
    expect(result.toISOString()).toBe(date.toISOString());
  });

  test('Date at epoch', () => {
    const epoch = new Date(0);
    const result = deserialize<Date>(serialize(epoch));
    expect(result.getTime()).toBe(0);
  });

  test('Infinity', () => {
    expect(deserialize<number>(serialize(Infinity))).toBe(Infinity);
  });

  test('-Infinity', () => {
    expect(deserialize<number>(serialize(-Infinity))).toBe(-Infinity);
  });

  test('NaN', () => {
    const result = deserialize<number>(serialize(NaN));
    expect(Number.isNaN(result)).toBe(true);
  });

  test('undefined', () => {
    expect(deserialize<undefined>(serialize(undefined))).toBe(undefined);
  });

  test('Map roundtrip', () => {
    const map = new Map([
      ['key1', 'value1'],
      ['key2', 'value2'],
    ]);
    const result = deserialize<Map<string, string>>(serialize(map));
    expect(result.get('key1')).toBe('value1');
    expect(result.get('key2')).toBe('value2');
  });

  test('Map with numeric keys', () => {
    const map = new Map([
      [1, 'one'],
      [2, 'two'],
    ]);
    const result = deserialize<Map<number, string>>(serialize(map));
    expect(result.size).toBe(2);
  });

  test('Set roundtrip', () => {
    const set = new Set([1, 2, 3, 4, 5]);
    const result = deserialize<Set<number>>(serialize(set));
    expect(result.size).toBe(5);
    expect(result.has(3)).toBe(true);
  });

  test('Set with strings', () => {
    const set = new Set(['a', 'b', 'c']);
    const result = deserialize<Set<string>>(serialize(set));
    expect(result.has('a')).toBe(true);
  });

  test('nested objects', () => {
    const obj = {
      level1: {
        level2: {
          level3: {
            value: 'deep',
          },
        },
      },
    };
    expect(deserialize<typeof obj>(serialize(obj))).toEqual(obj);
  });

  test('arrays', () => {
    const arr = [1, 2, 3, 'hello', true, null];
    expect(deserialize<typeof arr>(serialize(arr))).toEqual(arr);
  });

  test('mixed complex object', () => {
    const complex = {
      string: 'hello',
      number: 123,
      bigint: BigInt('12345678901234567890'),
      date: new Date('2024-01-10'),
      infinity: Infinity,
      map: new Map([['a', 1]]),
      set: new Set([1, 2, 3]),
      nested: {
        undefined: undefined,
        nan: NaN,
      },
    };

    const result = deserialize<typeof complex>(serialize(complex));
    expect(result.string).toBe('hello');
    expect(result.number).toBe(123);
    expect(result.bigint).toBe(BigInt('12345678901234567890'));
    expect(result.infinity).toBe(Infinity);
  });

  test('empty object', () => {
    expect(deserialize<object>(serialize({}))).toEqual({});
  });

  test('empty array', () => {
    expect(deserialize<unknown[]>(serialize([]))).toEqual([]);
  });

  test('empty string', () => {
    expect(deserialize<string>(serialize(''))).toBe('');
  });

  test('zero', () => {
    expect(deserialize<number>(serialize(0))).toBe(0);
  });

  test('false', () => {
    expect(deserialize<boolean>(serialize(false))).toBe(false);
  });

  test('unicode strings', () => {
    const unicode = '🚗 驾驶员 водитель 🏎️';
    expect(deserialize<string>(serialize(unicode))).toBe(unicode);
  });

  test('special characters in strings', () => {
    const special = 'line1\nline2\ttab\\backslash"quote';
    expect(deserialize<string>(serialize(special))).toBe(special);
  });
});

describe('Serializer Performance', () => {
  test('serializes large object quickly', () => {
    const largeObj = {
      data: Array.from({ length: 10000 }, (_, i) => ({
        id: i,
        name: `item${i}`,
        value: Math.random(),
      })),
    };

    const start = performance.now();
    const serialized = serialize(largeObj);
    const deserialized = deserialize(serialized);
    const elapsed = performance.now() - start;

    console.log(`Large object (10k items): ${elapsed.toFixed(0)}ms`);
    expect(elapsed).toBeLessThan(1000); // Under 1 second
  });
});
