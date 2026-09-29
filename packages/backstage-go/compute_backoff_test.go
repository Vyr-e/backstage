package backstage

import "testing"

func TestComputeBackoffFixed(t *testing.T) {
	cfg := BackoffConfig{Type: BackoffFixed, Delay: 500}
	if ComputeBackoff(cfg, 1) != 500 || ComputeBackoff(cfg, 5) != 500 {
		t.Fatal("fixed backoff")
	}
}

func TestComputeBackoffExponential(t *testing.T) {
	cfg := BackoffConfig{Type: BackoffExponential, Delay: 100, MaxDelay: 1000}
	cases := []struct{ d, want int }{{1, 100}, {2, 100}, {3, 200}, {5, 800}, {6, 1000}}
	for _, c := range cases {
		if got := ComputeBackoff(cfg, c.d); got != int64(c.want) {
			t.Fatalf("d%d: got %d want %d", c.d, got, c.want)
		}
	}
}
