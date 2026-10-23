package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func TestDeadLetterDecision(t *testing.T) {
	tests := []struct {
		name    string
		job     GameJob
		failure error
		want    Decision
	}{
		{"valid asset completes", GameJob{Attempt: 0}, nil, DecisionComplete},
		{"first moderation failure retries", GameJob{Attempt: 0}, errors.New("policy lookup rejected"), DecisionRetry},
		{"third live event failure dead letters", GameJob{Attempt: 2}, errors.New("event invalid"), DecisionDeadLetter},
		{"dead letter record is archived", GameJob{Attempt: 3, DeadLetter: true}, errors.New("not processed again"), DecisionComplete},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := decide(tt.job, tt.failure); got != tt.want {
				t.Fatalf("decide() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestWorkerPublishesDeadLetterBeforeAck(t *testing.T) {
	job := GameJob{JobID: "mod-1042", Kind: "moderation", Attempt: 2, Moderation: &ModerationItem{CaseID: "case-88", Priority: "high"}}
	payload, err := json.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}
	queue := &recordingQueue{messages: []QueueMessage{{MessageID: "msg-7", Payload: payload}}}
	worker := Worker{queue: queue, process: func(context.Context, GameJob) error { return errors.New("policy rejected") }}

	result, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.DeadLettered != 1 {
		t.Fatalf("dead_lettered = %d, want 1", result.DeadLettered)
	}
	if len(queue.events) != 2 || queue.events[0] != "publish" || queue.events[1] != "ack:msg-7" {
		t.Fatalf("events = %v, want publish before ack", queue.events)
	}
	if !queue.published.DeadLetter || queue.published.Attempt != 3 || queue.published.FailureClass != "processing_rejected" {
		t.Fatalf("published job = %+v", queue.published)
	}
}

type recordingQueue struct {
	messages  []QueueMessage
	events    []string
	published GameJob
}

func (q *recordingQueue) Consume(context.Context, int, int) ([]QueueMessage, error) {
	return q.messages, nil
}

func (q *recordingQueue) Publish(_ context.Context, payload any, _ string) error {
	q.events = append(q.events, "publish")
	q.published = payload.(GameJob)
	return nil
}

func (q *recordingQueue) Ack(_ context.Context, messageID string) error {
	q.events = append(q.events, "ack:"+messageID)
	return nil
}
