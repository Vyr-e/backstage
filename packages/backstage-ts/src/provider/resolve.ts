import { CapabilityMissingError } from './errors';
import type {
  BackstageProvider,
  CapabilityName,
  CapabilityOverrides,
  CapabilityReport,
  CapabilityReportEntry,
  ResolvedCapabilities,
} from './types';

function entry(
  available: boolean,
  name: string | undefined,
  source: CapabilityReportEntry['source'],
  capability: CapabilityName,
): CapabilityReportEntry {
  if (available) {
    return { available: true, name, source };
  }
  return {
    available: false,
    source: 'missing',
    hint: `implement ${capability === 'jobs' ? 'JobsCapability' : capability === 'topics' ? 'TopicsCapability' : capability === 'delays' ? 'DelaysCapability' : 'DedupeCapability'} and pass capabilities.${capability}`,
  };
}

export function resolveCapabilities(
  provider: BackstageProvider,
  overrides?: CapabilityOverrides,
): ResolvedCapabilities {
  if (!provider.jobs) {
    throw new CapabilityMissingError(provider.name, 'jobs');
  }
  return {
    jobs: provider.jobs,
    topics: overrides?.topics ?? provider.topics,
    delays: overrides?.delays ?? provider.delays,
    dedupe: overrides?.dedupe ?? provider.dedupe,
  };
}

export function buildCapabilityReport(
  providerName: string,
  provider: BackstageProvider,
  resolved: ResolvedCapabilities,
  overrides?: CapabilityOverrides,
): CapabilityReport {
  const sourceOf = (
    key: 'topics' | 'delays' | 'dedupe',
  ): CapabilityReportEntry['source'] => {
    if (overrides?.[key]) return 'plugged';
    if (provider[key]) return 'provider';
    return 'missing';
  };

  return {
    provider: providerName,
    jobs: entry(true, resolved.jobs.name, 'provider', 'jobs'),
    topics: entry(
      !!resolved.topics,
      resolved.topics?.name,
      sourceOf('topics'),
      'topics',
    ),
    delays: entry(
      !!resolved.delays,
      resolved.delays?.name,
      sourceOf('delays'),
      'delays',
    ),
    dedupe: entry(
      !!resolved.dedupe,
      resolved.dedupe?.name,
      sourceOf('dedupe'),
      'dedupe',
    ),
  };
}

export function requireCapability<K extends keyof ResolvedCapabilities>(
  providerName: string,
  resolved: ResolvedCapabilities,
  capability: K & CapabilityName,
): NonNullable<ResolvedCapabilities[K]> {
  const value = resolved[capability];
  if (!value) {
    throw new CapabilityMissingError(providerName, capability);
  }
  return value as NonNullable<ResolvedCapabilities[K]>;
}

export function formatCapabilityReport(report: CapabilityReport): string {
  const line = (label: string, e: CapabilityReportEntry) => {
    if (e.available) {
      const tag = e.source === 'plugged' ? ` (${e.source})` : '';
      return `  ${label.padEnd(7)} ✓ ${e.name}${tag}`;
    }
    return `  ${label.padEnd(7)} ✗ missing — ${e.hint}`;
  };
  return [
    `provider: ${report.provider}`,
    line('jobs', report.jobs),
    line('topics', report.topics),
    line('delays', report.delays),
    line('dedupe', report.dedupe),
  ].join('\n');
}
