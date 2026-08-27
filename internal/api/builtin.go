package api

import (
	"context"
	"fmt"

	"github.com/dyl-02/taskflow/internal/registry"
)

// RegisterBuiltins installs the standard task handlers on the registry.
func RegisterBuiltins(reg *registry.Registry) error {
	if err := reg.Register("echo", func(_ context.Context, payload []byte) ([]byte, error) {
		return payload, nil
	}); err != nil {
		return err
	}
	if err := reg.Register("noop", func(_ context.Context, _ []byte) ([]byte, error) {
		return []byte("ok"), nil
	}); err != nil {
		return err
	}
	if err := reg.Register("fail-once", func(_ context.Context, payload []byte) ([]byte, error) {
		if len(payload) > 0 && payload[0] == '1' {
			return nil, fmt.Errorf("simulated transient failure")
		}
		return []byte("ok"), nil
	}); err != nil {
		return err
	}
	return nil
}
