// Package backstage script registry.
// Manages loading and executing Lua scripts in Redis with automatic SHA caching and error recovery.
package backstage

import (
	"context"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

// ScriptDef defines a Lua script and its expected keys.
type ScriptDef struct {
	// Script is the Lua script source code.
	Script string
	// Keys maps key names to their 1-based KEY index in the Redis call.
	// For example, {"myKey": 1} means KEYS[1] will correspond to the value provided for "myKey".
	Keys map[string]int
}

// registeredScript holds the internal state of a loaded script.
type registeredScript struct {
	sha string
	def ScriptDef
}

// ScriptRegistry manages Lua scripts for Redis execution using EVALSHA.
type ScriptRegistry struct {
	client  redis.UniversalClient
	scripts map[string]*registeredScript
}

// NewScriptRegistry creates a new ScriptRegistry.
func NewScriptRegistry(client redis.UniversalClient) *ScriptRegistry {
	return &ScriptRegistry{
		client:  client,
		scripts: make(map[string]*registeredScript),
	}
}

// Load loads multiple scripts into Redis and registers them.
func (r *ScriptRegistry) Load(ctx context.Context, scripts map[string]ScriptDef) error {
	for name, def := range scripts {
		sha, err := r.client.ScriptLoad(ctx, def.Script).Result()
		if err != nil {
			return fmt.Errorf("failed to load script %q: %w", name, err)
		}
		r.scripts[name] = &registeredScript{sha: sha, def: def}
	}
	return nil
}

// Run executes a registered script.
// keys is a map of key names (as defined in ScriptDef.Keys) to their actual Redis key values.
// args is a list of arguments to pass to the script (ARGV[1], ARGV[2], ...).
func (r *ScriptRegistry) Run(ctx context.Context, name string, keys map[string]string, args ...interface{}) (interface{}, error) {
	script, ok := r.scripts[name]
	if !ok {
		return nil, fmt.Errorf("script %q is not registered", name)
	}

	// Prepare KEYS array
	numKeys := len(script.def.Keys)
	orderedKeys := make([]string, numKeys)

	for keyName, index := range script.def.Keys {
		val, ok := keys[keyName]
		if !ok {
			expectedKeys := make([]string, 0, len(script.def.Keys))
			for k := range script.def.Keys {
				expectedKeys = append(expectedKeys, k)
			}
			return nil, fmt.Errorf("missing required key %q for script %q. Expected keys: %s", keyName, name, strings.Join(expectedKeys, ", "))
		}
		
		// Validate index range
		if index < 1 || index > numKeys {
			return nil, fmt.Errorf("invalid key index %d for key %q in script %q. Indices must be between 1 and %d", index, keyName, name, numKeys)
		}

		orderedKeys[index-1] = val
	}

	// Verify no gaps
	for i, k := range orderedKeys {
		if k == "" {
			return nil, fmt.Errorf("missing key for index %d in script %q", i+1, name)
		}
	}

	// Execute
	res, err := r.client.EvalSha(ctx, script.sha, orderedKeys, args...).Result()
	if err != nil {
		// Handle NOSCRIPT error
		if strings.HasPrefix(err.Error(), "NOSCRIPT") {
			// Reload
			newSha, loadErr := r.client.ScriptLoad(ctx, script.def.Script).Result()
			if loadErr != nil {
				return nil, fmt.Errorf("failed to reload script %q after NOSCRIPT error: %w", name, loadErr)
			}
			script.sha = newSha
			// Retry
			return r.client.EvalSha(ctx, newSha, orderedKeys, args...).Result()
		}
		return nil, err
	}

	return res, nil
}

// Has checks if a script is registered.
func (r *ScriptRegistry) Has(name string) bool {
	_, ok := r.scripts[name]
	return ok
}

// GetSHA returns the SHA of a registered script, or empty string if not found.
func (r *ScriptRegistry) GetSHA(name string) string {
	if s, ok := r.scripts[name]; ok {
		return s.sha
	}
	return ""
}
