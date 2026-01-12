/**
 * Backstage SDK - Serializer
 *
 * Type-safe JSON serialization/deserialization that handles
 * special JavaScript types: BigInt, Date, Infinity, NaN, undefined.
 */

/**
 * Special type marker in serialized JSON.
 */
interface TypedValue {
  __class__: string;
  value?: string;
  iso?: string;
  nanoseconds?: number;
}

/**
 * Custom replacer for JSON.stringify.
 * Handles special types that JSON doesn't support natively.
 */
function replacer(this: unknown, key: string, value: unknown): unknown {
  // Get the original value before JSON's default conversion
  const orig = (this as Record<string, unknown>)[key];

  switch (typeof orig) {
    case 'bigint':
      return {
        __class__: 'BigInt',
        value: orig.toString(),
      } as TypedValue;

    case 'number':
      if (orig === Infinity) {
        return { __class__: 'Infinity' } as TypedValue;
      } else if (orig === -Infinity) {
        return { __class__: '-Infinity' } as TypedValue;
      } else if (Number.isNaN(orig)) {
        return { __class__: 'NaN' } as TypedValue;
      }
      return value;

    case 'undefined':
      return { __class__: 'undefined' } as TypedValue;

    case 'object':
      if (orig instanceof Date) {
        return {
          __class__: 'Date',
          iso: orig.toISOString(),
        } as TypedValue;
      }
      // Handle Map
      if (orig instanceof Map) {
        return {
          __class__: 'Map',
          value: JSON.stringify(Array.from(orig.entries())),
        } as TypedValue;
      }
      // Handle Set
      if (orig instanceof Set) {
        return {
          __class__: 'Set',
          value: JSON.stringify(Array.from(orig.values())),
        } as TypedValue;
      }
      return value;

    default:
      return value;
  }
}

/**
 * Custom reviver for JSON.parse.
 * Restores special types from their serialized form.
 */
function reviver(_key: string, value: unknown): unknown {
  if (typeof value === 'object' && value !== null) {
    const typed = value as TypedValue;

    if (typeof typed.__class__ !== 'string') {
      return value;
    }

    switch (typed.__class__) {
      case 'BigInt':
        return BigInt(typed.value as string);
      case 'Infinity':
        return Infinity;
      case '-Infinity':
        return -Infinity;
      case 'NaN':
        return NaN;
      case 'undefined':
        return undefined;
      case 'Date':
        return new Date(typed.iso as string);
      case 'Map':
        return new Map(JSON.parse(typed.value as string));
      case 'Set':
        return new Set(JSON.parse(typed.value as string));
      default:
        return value;
    }
  }
  return value;
}

/**
 * Serialize a value to JSON string with support for special types.
 */
export function serialize(obj: unknown): string {
  return JSON.stringify(obj, replacer);
}

/**
 * Deserialize a JSON string with support for special types.
 */
export function deserialize<T = unknown>(jsonStr: string): T {
  return JSON.parse(jsonStr, reviver) as T;
}
