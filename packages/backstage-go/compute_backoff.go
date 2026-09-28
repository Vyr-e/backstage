package backstage

// ComputeBackoff is shared by core retry decisions and the Redis reclaim loop.
// Matches TypeScript computeBackoff. deliveryCount 1 = first delivery.
func ComputeBackoff(config BackoffConfig, deliveryCount int) int64 {
	retries := deliveryCount - 1
	if retries < 0 {
		retries = 0
	}
	if config.Type == BackoffFixed {
		return config.Delay
	}
	if config.Type == BackoffExponential {
		power := retries - 1
		if power < 0 {
			power = 0
		}
		delay := config.Delay * int64(uint64(1)<<uint(power))
		max := config.MaxDelay
		if max <= 0 {
			max = 3_600_000
		}
		if delay > max {
			return max
		}
		return delay
	}
	return 0
}
