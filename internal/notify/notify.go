package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/dyl-02/taskflow/internal/metrics"
	"github.com/dyl-02/taskflow/internal/model"
	"github.com/dyl-02/taskflow/internal/retry"
)

// Client sends completion webhooks.
type Client struct {
	http   *http.Client
	policy *retry.Policy
	metrics *metrics.Metrics
}

// New creates a webhook client with the given retry policy.
func New(policy *retry.Policy, m *metrics.Metrics) *Client {
	return &Client{
		http: &http.Client{Timeout: 5 * time.Second},
		policy: policy,
		metrics: m,
	}
}

// Notify delivers a task completion webhook when the task requested one.
func (c *Client) Notify(ctx context.Context, task *model.Task, result model.Result) error {
	if task == nil {
		return nil
	}
	if task.NotifyURL == "" {
		return nil
	}
	// A task without a retry schedule is a partial record; skip silently
	// rather than dereferencing a missing policy.
	if task.Retry == nil {
		return nil
	}
	body, err := json.Marshal(map[string]any{
		"task_id": task.ID,
		"state":   task.State,
		"success": result.Success,
		"message": result.Message,
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, task.NotifyURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("deliver webhook: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("webhook returned %s", resp.Status)
	}
	if c.metrics != nil {
		c.metrics.Notifications.Add(1)
	}
	return nil
}
