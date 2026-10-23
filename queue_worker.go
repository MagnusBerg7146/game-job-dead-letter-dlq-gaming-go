package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
)

const maxAttempts = 3

type GameJob struct {
	JobID        string          `json:"job_id"`
	Kind         string          `json:"kind"`
	Attempt      int             `json:"attempt"`
	Asset        *PlayerAsset    `json:"asset,omitempty"`
	LiveEvent    *LiveEvent      `json:"live_event,omitempty"`
	Moderation   *ModerationItem `json:"moderation,omitempty"`
	DeadLetter   bool            `json:"dead_letter,omitempty"`
	FailureClass string          `json:"failure_class,omitempty"`
}

type PlayerAsset struct {
	PlayerID string `json:"player_id"`
	AssetID  string `json:"asset_id"`
	Format   string `json:"format"`
}

type LiveEvent struct {
	EventID string `json:"event_id"`
	Region  string `json:"region"`
}

type ModerationItem struct {
	CaseID   string `json:"case_id"`
	Priority string `json:"priority"`
}

type Decision string

const (
	DecisionComplete   Decision = "complete"
	DecisionRetry      Decision = "retry"
	DecisionDeadLetter Decision = "dead_letter"
)

func decide(job GameJob, processingErr error) Decision {
	if processingErr == nil || job.DeadLetter {
		return DecisionComplete
	}
	if job.Attempt+1 >= maxAttempts {
		return DecisionDeadLetter
	}
	return DecisionRetry
}

type queueAPI interface {
	Publish(context.Context, any, string) error
	Consume(context.Context, int, int) ([]QueueMessage, error)
	Ack(context.Context, string) error
}

type Worker struct {
	queue   queueAPI
	process func(context.Context, GameJob) error
}

type RunResult struct {
	Consumed     int `json:"consumed"`
	Completed    int `json:"completed"`
	Retried      int `json:"retried"`
	DeadLettered int `json:"dead_lettered"`
}

func (w Worker) RunOnce(ctx context.Context) (RunResult, error) {
	messages, err := w.queue.Consume(ctx, 10, 30)
	if err != nil {
		return RunResult{}, err
	}
	result := RunResult{Consumed: len(messages)}
	for _, message := range messages {
		var job GameJob
		if err := json.Unmarshal(message.Payload, &job); err != nil {
			return result, fmt.Errorf("message %s payload: %w", message.MessageID, err)
		}
		processingErr := w.process(ctx, job)
		switch decide(job, processingErr) {
		case DecisionRetry:
			job.Attempt++
			if err := w.queue.Publish(ctx, job, publishKey("retry", job)); err != nil {
				return result, err
			}
			if err := w.queue.Ack(ctx, message.MessageID); err != nil {
				return result, err
			}
			result.Retried++
		case DecisionDeadLetter:
			job.Attempt++
			job.DeadLetter = true
			job.FailureClass = "processing_rejected"
			if err := w.queue.Publish(ctx, job, publishKey("dead-letter", job)); err != nil {
				return result, err
			}
			if err := w.queue.Ack(ctx, message.MessageID); err != nil {
				return result, err
			}
			result.DeadLettered++
		case DecisionComplete:
			if err := w.queue.Ack(ctx, message.MessageID); err != nil {
				return result, err
			}
			result.Completed++
		}
	}
	return result, nil
}

func publishKey(stage string, job GameJob) string {
	sum := sha256.Sum256([]byte(stage + ":" + job.JobID + ":" + fmt.Sprint(job.Attempt)))
	return hex.EncodeToString(sum[:])
}

func processGameJob(_ context.Context, job GameJob) error {
	switch job.Kind {
	case "player_asset":
		if job.Asset == nil || job.Asset.PlayerID == "" || job.Asset.AssetID == "" {
			return errors.New("asset identity required")
		}
	case "live_event":
		if job.LiveEvent == nil || job.LiveEvent.EventID == "" || job.LiveEvent.Region == "" {
			return errors.New("event identity and region required")
		}
	case "moderation":
		if job.Moderation == nil || job.Moderation.CaseID == "" {
			return errors.New("moderation case required")
		}
	default:
		return errors.New("unknown game job kind")
	}
	return nil
}
