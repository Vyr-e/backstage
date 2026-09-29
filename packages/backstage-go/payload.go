package backstage

import "encoding/json"

// EncodePayload encodes a job/topic payload for the wire.
//
// Rules:
//   - json.RawMessage is used as-is (empty → null)
//   - []byte that passes json.Valid is used as-is
//   - []byte that is not valid JSON is json.Marshal'd (base64 string)
//   - nil → null
//   - everything else → json.Marshal
func EncodePayload(v any) ([]byte, error) {
	switch x := v.(type) {
	case nil:
		return []byte("null"), nil
	case json.RawMessage:
		if len(x) == 0 {
			return []byte("null"), nil
		}
		return []byte(x), nil
	case []byte:
		if json.Valid(x) {
			return x, nil
		}
		return json.Marshal(x)
	default:
		return json.Marshal(v)
	}
}
