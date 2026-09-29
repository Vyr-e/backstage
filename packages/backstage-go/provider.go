package backstage

import (
	"context"
	"encoding/json"
	"fmt"
)

type CapabilityName string

const (
	CapabilityJobs   CapabilityName = "jobs"
	CapabilityTopics CapabilityName = "topics"
	CapabilityDelays CapabilityName = "delays"
	CapabilityDedupe CapabilityName = "dedupe"
)

type Subscription interface {
	Stop(ctx context.Context) error
}

type OutgoingJob struct {
	Queue         string
	TaskName      string
	Payload       interface{}
	EnqueuedAt    int64
	Meta          JobMeta
	DeliveryCount int
}

type JobMeta struct {
	Attempts int            `json:"attempts,omitempty"`
	Backoff  *BackoffConfig `json:"backoff,omitempty"`
	Timeout  int64          `json:"timeout,omitempty"` // ms; 0 = unset
}

type ConsumeOptions struct {
	Queues      []string
	Group       string
	ConsumerID  string
	Prefetch    int
	IdleTimeout int64 // ms
}

type JobDelivery interface {
	ID() string
	Queue() string
	TaskName() string
	Payload() json.RawMessage
	EnqueuedAt() int64
	DeliveryCount() int
	Meta() JobMeta
	Ack(ctx context.Context) error
	Retry(ctx context.Context, opts RetryOpts) error
	DeadLetter(ctx context.Context, opts DeadLetterOpts) error
}

type RetryOpts struct {
	DelayMs int64
	Error   string
}

type DeadLetterOpts struct {
	Error string
}

type Jobs interface {
	Name() string
	Requires() []CapabilityName
	EnsureQueues(ctx context.Context, queues []string) error
	Publish(ctx context.Context, job OutgoingJob) (string, error)
	Consume(ctx context.Context, opts ConsumeOptions, onDelivery func(context.Context, JobDelivery) error) (Subscription, error)
}

type TopicStart string

const (
	TopicFromLatest   TopicStart = "latest"
	TopicFromEarliest TopicStart = "earliest"
)

type TopicSubscribeOptions struct {
	Topic      string
	Group      string
	ConsumerID string
	From       TopicStart
}

type TopicDelivery interface {
	ID() string
	Topic() string
	Payload() json.RawMessage
	PublishedAt() int64
	DeliveryCount() int
	Ack(ctx context.Context) error
}

type Topics interface {
	Name() string
	Publish(ctx context.Context, topic string, payload interface{}) (string, error)
	Subscribe(ctx context.Context, opts TopicSubscribeOptions, onMessage func(context.Context, TopicDelivery) error) (Subscription, error)
}

type Delays interface {
	Name() string
	Schedule(ctx context.Context, job OutgoingJob, runAt int64) (string, error)
}

type Dedupe interface {
	Name() string
	Claim(ctx context.Context, key string, ttlMs int64) (bool, error)
}

type ResolvedCapabilities struct {
	Jobs   Jobs
	Topics Topics
	Delays Delays
	Dedupe Dedupe
}

type ProviderContext struct {
	Capabilities ResolvedCapabilities
	Logger       *Logger
}

type Provider interface {
	Name() string
	Jobs() Jobs
	Topics() Topics
	Delays() Delays
	Dedupe() Dedupe
	Init(ctx context.Context, pctx ProviderContext) error
	Close() error
}

type Capabilities struct {
	Topics Topics
	Delays Delays
	Dedupe Dedupe
}

type CapabilityReportEntry struct {
	Available bool
	Name      string
	Source    string
	Hint      string
}

type CapabilityReport struct {
	Provider string
	Jobs     CapabilityReportEntry
	Topics   CapabilityReportEntry
	Delays   CapabilityReportEntry
	Dedupe   CapabilityReportEntry
}

var ErrCapabilityMissing = fmt.Errorf("capability missing")

type CapabilityError struct {
	Provider   string
	Capability CapabilityName
	Hint       string
}

func (e *CapabilityError) Error() string {
	return fmt.Sprintf("Provider %q does not provide capability %q. Implement %s.",
		e.Provider, e.Capability, e.Hint)
}
func (e *CapabilityError) Unwrap() error { return ErrCapabilityMissing }

func capabilityHint(c CapabilityName) string {
	switch c {
	case CapabilityJobs:
		return "Jobs and pass a Provider"
	case CapabilityTopics:
		return "Topics (pass Capabilities.Topics)"
	case CapabilityDelays:
		return "Delays (pass Capabilities.Delays)"
	case CapabilityDedupe:
		return "Dedupe (pass Capabilities.Dedupe)"
	default:
		return string(c)
	}
}

func NewCapabilityError(provider string, capability CapabilityName) *CapabilityError {
	return &CapabilityError{Provider: provider, Capability: capability, Hint: capabilityHint(capability)}
}

func ResolveCapabilities(provider Provider, overrides *Capabilities) (ResolvedCapabilities, error) {
	if provider == nil || provider.Jobs() == nil {
		name := "nil"
		if provider != nil {
			name = provider.Name()
		}
		return ResolvedCapabilities{}, NewCapabilityError(name, CapabilityJobs)
	}
	r := ResolvedCapabilities{Jobs: provider.Jobs()}
	if overrides != nil && overrides.Topics != nil {
		r.Topics = overrides.Topics
	} else {
		r.Topics = provider.Topics()
	}
	if overrides != nil && overrides.Delays != nil {
		r.Delays = overrides.Delays
	} else {
		r.Delays = provider.Delays()
	}
	if overrides != nil && overrides.Dedupe != nil {
		r.Dedupe = overrides.Dedupe
	} else {
		r.Dedupe = provider.Dedupe()
	}
	return r, nil
}

func BuildCapabilityReport(provider Provider, resolved ResolvedCapabilities, overrides *Capabilities) CapabilityReport {
	src := func(key CapabilityName, hasOverride, hasProvider bool, name string) CapabilityReportEntry {
		if hasOverride {
			return CapabilityReportEntry{Available: true, Name: name, Source: "plugged"}
		}
		if hasProvider {
			return CapabilityReportEntry{Available: true, Name: name, Source: "provider"}
		}
		return CapabilityReportEntry{Available: false, Source: "missing", Hint: "implement " + capabilityHint(key)}
	}
	var to, do, de bool
	if overrides != nil {
		to, do, de = overrides.Topics != nil, overrides.Delays != nil, overrides.Dedupe != nil
	}
	tn, dn, en := "", "", ""
	if resolved.Topics != nil {
		tn = resolved.Topics.Name()
	}
	if resolved.Delays != nil {
		dn = resolved.Delays.Name()
	}
	if resolved.Dedupe != nil {
		en = resolved.Dedupe.Name()
	}
	return CapabilityReport{
		Provider: provider.Name(),
		Jobs:     CapabilityReportEntry{Available: true, Name: resolved.Jobs.Name(), Source: "provider"},
		Topics:   src(CapabilityTopics, to, provider.Topics() != nil, tn),
		Delays:   src(CapabilityDelays, do, provider.Delays() != nil, dn),
		Dedupe:   src(CapabilityDedupe, de, provider.Dedupe() != nil, en),
	}
}

func FormatCapabilityReport(report CapabilityReport) string {
	line := func(label string, e CapabilityReportEntry) string {
		if e.Available {
			tag := ""
			if e.Source == "plugged" {
				tag = " (plugged)"
			}
			return fmt.Sprintf("  %-7s ✓ %s%s", label, e.Name, tag)
		}
		return fmt.Sprintf("  %-7s ✗ missing — %s", label, e.Hint)
	}
	return fmt.Sprintf("provider: %s\n%s\n%s\n%s\n%s",
		report.Provider, line("jobs", report.Jobs), line("topics", report.Topics),
		line("delays", report.Delays), line("dedupe", report.Dedupe))
}

func RequireTopics(providerName string, r ResolvedCapabilities) (Topics, error) {
	if r.Topics == nil {
		return nil, NewCapabilityError(providerName, CapabilityTopics)
	}
	return r.Topics, nil
}
func RequireDelays(providerName string, r ResolvedCapabilities) (Delays, error) {
	if r.Delays == nil {
		return nil, NewCapabilityError(providerName, CapabilityDelays)
	}
	return r.Delays, nil
}
func RequireDedupe(providerName string, r ResolvedCapabilities) (Dedupe, error) {
	if r.Dedupe == nil {
		return nil, NewCapabilityError(providerName, CapabilityDedupe)
	}
	return r.Dedupe, nil
}
func RequireJobs(providerName string, r ResolvedCapabilities) (Jobs, error) {
	if r.Jobs == nil {
		return nil, NewCapabilityError(providerName, CapabilityJobs)
	}
	return r.Jobs, nil
}
