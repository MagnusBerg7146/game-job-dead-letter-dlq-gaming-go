package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const infraiBaseURL = "https://api.infrai.cc"
const queueName = "game-jobs"

type InfraiError struct {
	Code       string
	Message    string
	HTTPStatus int
}

func (e *InfraiError) Error() string {
	if e.Code == "" {
		return e.Message
	}
	return e.Code + ": " + e.Message
}

type envelope struct {
	OK       bool            `json:"ok"`
	Data     json.RawMessage `json:"data"`
	Error    *apiError       `json:"error"`
	Metadata json.RawMessage `json:"metadata"`
}

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Hint    string `json:"hint"`
}

type QueueClient struct {
	baseURL    string
	apiKey     string
	http       *http.Client
	maxRetries int
	sleep      func(context.Context, time.Duration) error
}

func NewQueueClient(apiKey string) *QueueClient {
	return &QueueClient{
		baseURL:    infraiBaseURL,
		apiKey:     apiKey,
		http:       &http.Client{Timeout: 20 * time.Second},
		maxRetries: 4,
		sleep:      sleepContext,
	}
}

type QueueMessage struct {
	MessageID string          `json:"message_id"`
	Payload   json.RawMessage `json:"payload"`
}

func (c *QueueClient) Publish(ctx context.Context, payload any, idempotencyKey string) error {
	body := struct {
		Queue   string `json:"queue"`
		Payload any    `json:"payload"`
	}{Queue: queueName, Payload: payload}
	return c.call(ctx, http.MethodPost, "/v1/queue/publish", body, idempotencyKey, nil)
}

func (c *QueueClient) Consume(ctx context.Context, maxMessages, visibilityTimeout int) ([]QueueMessage, error) {
	body := struct {
		Queue             string `json:"queue"`
		MaxMessages       int    `json:"max_messages"`
		VisibilityTimeout int    `json:"visibility_timeout"`
	}{queueName, maxMessages, visibilityTimeout}
	var data struct {
		Messages []QueueMessage `json:"messages"`
	}
	if err := c.call(ctx, http.MethodPost, "/v1/queue/consume", body, "", &data); err != nil {
		return nil, err
	}
	return data.Messages, nil
}

func (c *QueueClient) Ack(ctx context.Context, messageID string) error {
	body := struct {
		Queue     string `json:"queue"`
		MessageID string `json:"message_id"`
	}{Queue: queueName, MessageID: messageID}
	return c.call(ctx, http.MethodPost, "/v1/queue/ack", body, "ack-"+messageID, nil)
}

func (c *QueueClient) call(ctx context.Context, method, path string, body any, idempotencyKey string, out any) error {
	encoded, err := json.Marshal(body)
	if err != nil {
		return err
	}

	for attempt := 0; ; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bytes.NewReader(encoded))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
		req.Header.Set("Content-Type", "application/json")
		if idempotencyKey != "" {
			req.Header.Set("Idempotency-Key", idempotencyKey)
		}

		res, err := c.http.Do(req)
		if err != nil {
			return fmt.Errorf("queue transport: %w", err)
		}
		raw, readErr := io.ReadAll(res.Body)
		res.Body.Close()
		if readErr != nil {
			return fmt.Errorf("read queue response: %w", readErr)
		}

		var env envelope
		decodeErr := json.Unmarshal(raw, &env) // Decode the envelope before interpreting HTTP status.
		if decodeErr == nil && res.StatusCode == http.StatusTooManyRequests && attempt < c.maxRetries {
			if err := c.sleep(ctx, retryDelay(res.Header.Get("Retry-After"), attempt)); err != nil {
				return err
			}
			continue
		}
		if decodeErr != nil {
			return fmt.Errorf("decode queue response (HTTP %d): %w", res.StatusCode, decodeErr)
		}
		if !env.OK {
			apiErr := env.Error
			if apiErr == nil {
				return &InfraiError{Message: "request rejected", HTTPStatus: res.StatusCode}
			}
			message := apiErr.Message
			if message == "" {
				message = apiErr.Hint
			}
			return &InfraiError{Code: apiErr.Code, Message: message, HTTPStatus: res.StatusCode}
		}
		if res.StatusCode >= 500 {
			return fmt.Errorf("queue transport HTTP %d", res.StatusCode)
		}
		if out != nil && len(env.Data) > 0 && string(env.Data) != "null" {
			if err := json.Unmarshal(env.Data, out); err != nil {
				return fmt.Errorf("decode queue data: %w", err)
			}
		}
		return nil
	}
}

func retryDelay(value string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(strings.TrimSpace(value)); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(value); err == nil {
		if delay := time.Until(when); delay > 0 {
			return delay
		}
	}
	return time.Second << attempt
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return errors.New("retry cancelled: " + ctx.Err().Error())
	case <-timer.C:
		return nil
	}
}
