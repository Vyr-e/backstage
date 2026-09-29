import type { CapabilityName } from './types';

const INTERFACE_HINT: Record<CapabilityName, string> = {
  jobs: 'JobsCapability',
  topics: 'TopicsCapability (pass capabilities.topics)',
  delays: 'DelaysCapability (pass capabilities.delays)',
  dedupe: 'DedupeCapability (pass capabilities.dedupe)',
};

export class CapabilityMissingError extends Error {
  readonly provider: string;
  readonly capability: CapabilityName;

  constructor(provider: string, capability: CapabilityName) {
    const hint = INTERFACE_HINT[capability];
    super(
      `Provider "${provider}" does not provide capability "${capability}". Implement ${hint}.`,
    );
    this.name = 'CapabilityMissingError';
    this.provider = provider;
    this.capability = capability;
  }
}
