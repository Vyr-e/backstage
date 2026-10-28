package rabbitmq_test

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/vyr-e/backstage/packages/backstage-go"
	"github.com/vyr-e/backstage/packages/backstage-go/backstagetest"
	"github.com/vyr-e/backstage/packages/backstage-go/providers/rabbitmq"
)

func TestNativeDelaysContract(t *testing.T) {
	url := os.Getenv("RABBITMQ_URL")
	if url == "" {
		url = "amqp://guest:guest@localhost:5673/"
	}
	backstagetest.RunProviderContract(t, func() backstage.Provider {
		return rabbitmq.New(rabbitmq.Config{
			URL: url, NativeDelays: true, Prefix: fmt.Sprintf("native-go-%d", time.Now().UnixNano()),
		})
	}, backstagetest.Options{Timeout: 45 * time.Second})
}

func TestNativeDelaysRejectOlderBroker(t *testing.T) {
	url := os.Getenv("RABBITMQ_LEGACY_URL")
	if url == "" {
		t.Skip("set RABBITMQ_LEGACY_URL to verify rejection of an older broker")
	}
	client := backstage.New(backstage.Config{
		Provider: rabbitmq.New(rabbitmq.Config{URL: url, NativeDelays: true}),
	})
	defer client.Close()
	if err := client.InitError(); err == nil || !strings.Contains(err.Error(), "4.4+") {
		t.Fatalf("expected a version error, got %v", err)
	}
}
