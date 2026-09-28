export function parseFields(fields: unknown[]): Record<string, string> {
  const data: Record<string, string> = {};
  for (let i = 0; i < fields.length; i += 2) {
    const key = fields[i];
    const value = fields[i + 1];
    if (typeof key === 'string' && typeof value === 'string') {
      data[key] = value;
    }
  }
  return data;
}

/** Normalize Bun object-shaped or array-shaped XREADGROUP results. */
export function normalizeXReadGroup(
  result: unknown,
): [string, unknown[]][] {
  if (!result || typeof result !== 'object') return [];
  if (Array.isArray(result)) return result as [string, unknown[]][];
  return Object.entries(result) as [string, unknown[]][];
}
