package backstage

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type redisDelays struct{ p *RedisStreamsProvider }

func (d *redisDelays) Name() string { return "redis-streams" }

func (d *redisDelays) Schedule(ctx context.Context, job OutgoingJob, runAt int64) (string, error) {
	payload, err := payloadJSON(job.Payload)
	if err != nil {
		return "", fmt.Errorf("marshal payload: %w", err)
	}
	target := StreamKey(d.p.prefix, job.Queue)
	scheduledData := map[string]interface{}{
		"taskName": job.TaskName, "payload": payload, "enqueuedAt": job.EnqueuedAt,
		"streamKey": target, "priority": job.Queue,
	}
	if job.DeliveryCount > 0 {
		scheduledData["deliveryCount"] = job.DeliveryCount
	}
	if job.Meta.Attempts > 0 {
		scheduledData["attempts"] = job.Meta.Attempts
	}
	if job.Meta.Backoff != nil {
		b, _ := json.Marshal(job.Meta.Backoff)
		scheduledData["backoff"] = string(b)
	}
	if job.Meta.Timeout > 0 {
		scheduledData["timeout"] = job.Meta.Timeout
	}
	data, err := json.Marshal(scheduledData)
	if err != nil {
		return "", err
	}
	// No promote loop here — Client Start owns the single promote timer.
	if err := d.p.redis.ZAdd(ctx, ScheduledKey(d.p.prefix), redis.Z{
		Score: float64(runAt), Member: string(data),
	}).Err(); err != nil {
		return "", fmt.Errorf("zadd scheduled: %w", err)
	}
	return fmt.Sprintf("scheduled:%d", runAt), nil
}

type redisDedupe struct{ p *RedisStreamsProvider }

func (d *redisDedupe) Name() string { return "redis-streams" }

func (d *redisDedupe) Claim(ctx context.Context, key string, ttlMs int64) (bool, error) {
	ttlSec := (ttlMs + 999) / 1000
	if ttlSec < 1 {
		ttlSec = 1
	}
	return d.p.redis.SetNX(ctx, DedupeKey(d.p.prefix, key), "1", time.Duration(ttlSec)*time.Second).Result()
}
