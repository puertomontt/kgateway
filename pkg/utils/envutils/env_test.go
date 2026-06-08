package envutils

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGetDurationOrDefault(t *testing.T) {
	const key = "KGW_TEST_DURATION"

	t.Run("unset returns fallback", func(t *testing.T) {
		assert.NoError(t, os.Unsetenv(key))
		assert.Equal(t, 5*time.Second, GetDurationOrDefault(key, 5*time.Second))
	})

	t.Run("valid duration is parsed", func(t *testing.T) {
		t.Setenv(key, "250ms")
		assert.Equal(t, 250*time.Millisecond, GetDurationOrDefault(key, 5*time.Second))
	})

	t.Run("invalid duration returns fallback", func(t *testing.T) {
		t.Setenv(key, "not-a-duration")
		assert.Equal(t, 5*time.Second, GetDurationOrDefault(key, 5*time.Second))
	})

	t.Run("zero is honored", func(t *testing.T) {
		t.Setenv(key, "0s")
		assert.Equal(t, time.Duration(0), GetDurationOrDefault(key, 5*time.Second))
	})
}
