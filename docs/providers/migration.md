# Migrate an app from Redis to RabbitMQ or Kafka

Handlers, `enqueue` / `schedule` / chaining / cron stay the same. Change the transport:

## TypeScript

```ts
// Before (default Redis)
new Worker({ host: 'localhost', port: 6379 });

// RabbitMQ
import { RabbitMQProvider } from '@vyr-e/backstage/rabbitmq';
new Worker({
  provider: new RabbitMQProvider({ url: process.env.AMQP_URL }),
});

// Kafka (+ Redis delays/dedupe plugged in)
import { KafkaProvider } from '@vyr-e/backstage/kafka';
import { RedisStreamsProvider } from '@vyr-e/backstage'; // or construct delays-only
const redis = new RedisStreamsProvider({ host: 'localhost', port: 6379, prefix: 'bs-aux' });
new Worker({
  provider: new KafkaProvider({ brokers: ['localhost:9092'] }),
  capabilities: { delays: redis.delays, dedupe: redis.dedupe },
});
```

Install peers: `bun add amqplib` or `bun add kafkajs`.

## Go

```go
client := backstage.New(backstage.Config{
  Provider: rabbitmq.New(rabbitmq.Config{URL: os.Getenv("AMQP_URL")}),
})

// Kafka with Redis delays plugged in
redisDelays := backstage.NewRedisStreamsProvider(...)
client := backstage.New(backstage.Config{
  Provider: kafka.New(kafka.Config{Brokers: brokers}),
  Capabilities: &backstage.Capabilities{Delays: redisDelays.Delays(), Dedupe: redisDelays.Dedupe()},
})
```

## Notes

- In-flight Redis jobs are **not** migrated automatically. Drain Redis workers first,
  then cut over producers, then consumers.
- Legacy `Broadcast` stays Redis-only and is deprecated; use `publish` / `subscribe`.
- RabbitMQ needs the delayed-message plugin **or** a plugged `delays` capability
  (retries require delays).
- Kafka always needs plugged `delays` for retries (`jobs.requires = ['delays']`).
