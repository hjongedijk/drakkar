package workflow

import "testing"

func TestToBullPriority(t *testing.T) {
	tests := []struct {
		name     string
		input    int
		expected int
	}{
		{name: "selected first", input: 0, expected: 1},
		{name: "background search lower", input: 10, expected: 11},
		{name: "negative clamps", input: -5, expected: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := toBullPriority(tt.input); got != tt.expected {
				t.Fatalf("toBullPriority(%d) = %d, want %d", tt.input, got, tt.expected)
			}
		})
	}
}
