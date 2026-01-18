package backstage

import (
	"context"
	"testing"
	"strings"

	"github.com/redis/go-redis/v9"
)

func TestScriptRegistry(t *testing.T) {
	ctx := context.Background()

	// Setup Redis client
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer rdb.Close()

	// Verify Redis connection
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("Skipping test, redis unavailble: %v", err)
	}

	// Clean up scripts
	defer rdb.ScriptFlush(ctx)

	t.Run("LoadsScriptCorrectly", func(t *testing.T) {
		registry := NewScriptRegistry(rdb)
		scriptName := "test-load"
		
		err := registry.Load(ctx, map[string]ScriptDef{
			scriptName: {
				Script: "return ARGV[1]",
				Keys:   map[string]int{},
			},
		})
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}

		if !registry.Has(scriptName) {
			t.Errorf("Expected registry to have script %q", scriptName)
		}
		
		if sha := registry.GetSHA(scriptName); sha == "" {
			t.Errorf("Expected SHA for script %q", scriptName)
		}
	})

	t.Run("ExecutesScriptWithArguments", func(t *testing.T) {
		registry := NewScriptRegistry(rdb)
		scriptName := "test-args"

		err := registry.Load(ctx, map[string]ScriptDef{
			scriptName: {
				Script: "return {KEYS[1], ARGV[1]}",
				Keys:   map[string]int{"testKey": 1},
			},
		})
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}

		result, err := registry.Run(ctx, scriptName, map[string]string{"testKey": "my-key"}, "my-arg")
		if err != nil {
			t.Fatalf("Run failed: %v", err)
		}

		response, ok := result.([]interface{})
		if !ok || len(response) != 2 {
			t.Fatalf("Unexpected response format: %v", result)
		}

		if response[0] != "my-key" {
			t.Errorf("Expected result[0] to be 'my-key', got %v", response[0])
		}
		if response[1] != "my-arg" {
			t.Errorf("Expected result[1] to be 'my-arg', got %v", response[1])
		}
	})

	t.Run("HandlesNOSCRIPTErrorAutomatically", func(t *testing.T) {
		registry := NewScriptRegistry(rdb)
		scriptName := "test-noscript"

		err := registry.Load(ctx, map[string]ScriptDef{
			scriptName: {
				Script: "return 'recovered'",
				Keys:   map[string]int{},
			},
		})
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}

		// Flush scripts to simulate restart/eviction
		if err := rdb.ScriptFlush(ctx).Err(); err != nil {
			t.Fatalf("ScriptFlush failed: %v", err)
		}

		// Should recover automatically
		result, err := registry.Run(ctx, scriptName, map[string]string{})
		if err != nil {
			t.Fatalf("Run failed after flush: %v", err)
		}

		if result != "recovered" {
			t.Errorf("Expected result 'recovered', got %v", result)
		}

		if sha := registry.GetSHA(scriptName); sha == "" {
			t.Errorf("Expected SHA to be present after recovery")
		}
	})

	t.Run("ThrowsOnMissingKeys", func(t *testing.T) {
		registry := NewScriptRegistry(rdb)
		scriptName := "test-missing-keys"

		err := registry.Load(ctx, map[string]ScriptDef{
			scriptName: {
				Script: "return KEYS[1]",
				Keys:   map[string]int{"requiredKey": 1},
			},
		})
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}

		_, err = registry.Run(ctx, scriptName, map[string]string{})
		if err == nil {
			t.Error("Expected error for missing key, got nil")
		} else {
			if !strings.Contains(err.Error(), "missing required key") {
				t.Errorf("Expected error message to contain 'missing required key', got: %v", err)
			}
		}
	})

	t.Run("ThrowsOnUnregisteredScript", func(t *testing.T) {
		registry := NewScriptRegistry(rdb)
		_, err := registry.Run(ctx, "non-existent", nil)
		if err == nil {
			t.Error("Expected error for unregistered script, got nil")
		}
	})

	t.Run("ValidatesKeyIndices", func(t *testing.T) {
		registry := NewScriptRegistry(rdb)
		scriptName := "test-bad-indices"

		err := registry.Load(ctx, map[string]ScriptDef{
			scriptName: {
				Script: "return 'ok'",
				Keys: map[string]int{
					"key1": 1,
					"key3": 3,
				},
			},
		})
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}

		// This will technically fail inside Run because of verification logic
		// But let's check if the gap detection works
		_, err = registry.Run(ctx, scriptName, map[string]string{
			"key1": "k1",
			"key3": "k3",
		})
		if err == nil {
			t.Error("Expected error for non-contiguous indices, got nil")
		} else {
			if !strings.Contains(err.Error(), "invalid key index") {
				t.Errorf("Expected error message about invalid key index, got: %v", err)
			}
		}
	})
}


