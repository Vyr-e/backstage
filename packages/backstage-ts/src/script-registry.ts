/**
 * Backstage SDK - Script Registry
 *
 * Manages Lua scripts for Redis execution with SHA caching and automatic
 * reload on NOSCRIPT errors.
 */

import { type RedisClient } from './types';

export interface ScriptDef {
  /** Lua script source code */
  script: string;
  /** Map of key names to their 1-based KEY index */
  keys: Record<string, number>;
}

/**
 * Manages Lua scripts for Redis execution.
 * Handles loading scripts and executing them via EVALSHA for performance.
 */
export class ScriptRegistry {
  private redis: RedisClient;
  private scripts: Map<string, { sha: string; def: ScriptDef }> = new Map();

  constructor(redis: RedisClient) {
    this.redis = redis;
  }

  /**
   * Load multiple Lua scripts into Redis and the registry.
   *
   * Scripts are loaded using Redis SCRIPT LOAD command which returns
   * a SHA digest. This SHA is used for subsequent EVALSHA calls.
   *
   * @param scripts Map of script names to script definitions
   * @example
   * ```typescript
   * await registry.load({
   *   claimRide: {
   *     script: `
   *       local status = redis.call('HGET', KEYS[1], 'status')
   *       if status ~= 'pending' then return {err='already_claimed'} end
   *       redis.call('HSET', KEYS[1], 'status', 'accepted', 'driverId', ARGV[1])
   *       return {ok=ARGV[2]}
   *     `,
   *     keys: { rideStateKey: 1 }
   *   }
   * });
   * ```
   */
  async load(scripts: Record<string, ScriptDef>): Promise<void> {
    for (const [name, def] of Object.entries(scripts)) {
      const sha = (await this.redis.send('SCRIPT', [
        'LOAD',
        def.script,
      ])) as string;
      this.scripts.set(name, { sha, def });
    }
  }

  /**
   * Run a registered Lua script.
   *
   * Keys are mapped by name - the order is determined by the indices
   * specified in the script definition.
   *
   * @param name Name of the script to run (must be registered via load)
   * @param keys Map of key names to their Redis key values
   * @param args Arguments to pass to the script (ARGV[1], ARGV[2], etc.)
   * @returns Script execution result (parsed from Redis response)
   *
   * @example
   * ```typescript
   * const result = await registry.run<{ ok?: string; err?: string }>(
   *   'claimRide',
   *   { rideStateKey: `ride:state:${rideId}` },
   *   [driverId, eventId]
   * );
   *
   * if (result.ok) {
   *   console.log('Ride claimed successfully');
   * } else if (result.err) {
   *   console.error('Failed:', result.err);
   * }
   * ```
   */
  async run<T = unknown>(
    name: string,
    keys: Record<string, string>,
    args: (string | number)[],
  ): Promise<T> {
    const entry = this.scripts.get(name);
    if (!entry) {
      throw new Error(`Script '${name}' is not registered. Call load() first.`);
    }

    const { sha, def } = entry;

    // Build ordered key array based on definition indices
    const keyCount = Object.keys(def.keys).length;
    const orderedKeys = new Array<string>(keyCount);

    for (const [keyName, index] of Object.entries(def.keys)) {
      const value = keys[keyName];
      if (value === undefined) {
        throw new Error(
          `Missing required key '${keyName}' for script '${name}'. ` +
            `Expected keys: ${Object.keys(def.keys).join(', ')}`,
        );
      }
      // Indices are 1-based, array is 0-based
      orderedKeys[index - 1] = value;
    }

    // Validate no gaps in key indices
    for (let i = 0; i < orderedKeys.length; i++) {
      if (orderedKeys[i] === undefined) {
        throw new Error(
          `Invalid key definition for script '${name}'. ` +
            `Missing key for index ${i + 1}. ` +
            `Ensure all indices from 1 to ${keyCount} are defined.`,
        );
      }
    }

    const commandArgs = [
      sha,
      String(orderedKeys.length),
      ...orderedKeys,
      ...args.map(String),
    ];

    try {
      return (await this.redis.send('EVALSHA', commandArgs)) as T;
    } catch (err: unknown) {
      // Handle NOSCRIPT error (Redis was restarted, lost cached scripts)
      if (err instanceof Error && err.message.includes('NOSCRIPT')) {
        // Reload and retry once
        const newSha = (await this.redis.send('SCRIPT', [
          'LOAD',
          def.script,
        ])) as string;
        this.scripts.set(name, { sha: newSha, def });
        commandArgs[0] = newSha;
        return (await this.redis.send('EVALSHA', commandArgs)) as T;
      }
      throw err;
    }
  }

  /**
   * Get information about a registered script.
   *
   * @param name Script name
   * @returns Script definition or undefined if not found
   */
  get(name: string): { sha: string; def: ScriptDef } | undefined {
    return this.scripts.get(name);
  }

  /**
   * Check if a script is registered.
   *
   * @param name Script name
   * @returns True if script is registered
   */
  has(name: string): boolean {
    return this.scripts.has(name);
  }

  /**
   * Get all registered script names.
   *
   * @returns Array of script names
   */
  keys(): string[] {
    return Array.from(this.scripts.keys());
  }
}
