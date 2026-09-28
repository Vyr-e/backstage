package backstage_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/vyr-e/backstage/packages/backstage-go"
	"github.com/vyr-e/backstage/packages/backstage-go/backstagetest"
)

func TestRedisProviderContract(t *testing.T) {
	prefix := fmt.Sprintf("rpc-%d", time.Now().UnixNano())
	backstagetest.RunProviderContract(t, func() backstage.Provider {
		return backstage.NewRedisStreamsProvider(backstage.RedisStreamsProviderConfig{
			Host: "localhost", Port: 6379, Prefix: prefix,
			ReclaimInterval: 150 * time.Millisecond, BlockTimeout: 150 * time.Millisecond,
		})
	}, backstagetest.Options{Timeout: 30 * time.Second})
}
