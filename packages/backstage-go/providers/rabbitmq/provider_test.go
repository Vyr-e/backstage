package rabbitmq_test

import (
	"context"
	"testing"
	"time"

	"github.com/vyr-e/backstage/packages/backstage-go"
	"github.com/vyr-e/backstage/packages/backstage-go/backstagetest"
	"github.com/vyr-e/backstage/packages/backstage-go/providers/rabbitmq"
)

func TestRabbitContract(t *testing.T) {
	p := rabbitmq.New(rabbitmq.Config{URL: "amqp://guest:guest@localhost:5672/", Prefix: "rpc-test"})
	err := p.Init(context.Background(), backstage.ProviderContext{
		Capabilities: backstage.ResolvedCapabilities{Jobs: p.Jobs(), Topics: p.Topics()},
		Logger:       backstage.NewLogger("t"),
	})
	if err != nil {
		t.Skipf("RabbitMQ unreachable: %v", err)
	}
	_ = p.Close()

	backstagetest.RunProviderContract(t, func() backstage.Provider {
		return rabbitmq.New(rabbitmq.Config{URL: "amqp://guest:guest@localhost:5672/", Prefix: "rpc-" + time.Now().Format("150405")})
	}, backstagetest.Options{Timeout: 30 * time.Second})
}
