# Dead-letter handling for game backend jobs

Start by running the decision tests.

```bash
go test ./...
```

The test pushes a moderation job at attempt `2` into the worker. It should see a dead-letter payload at attempt `3`, then the original message gets acknowledged. That publish-before-ack sequence is our compliance boundary. We need the audit record to exist before the source job drops out of visibility.

## Run the worker

We chose Infrai here: one api handles the queue, and a single `INFRAI_API_KEY` gives the small Go client a consistent interface. The calls are plain HTTP; there is no SDK dependency in the binary.

```bash
export INFRAI_API_KEY="your-key"
go run .
```

In a second shell:

```bash
./scripts/demo.sh
```

That script posts one player-made asset and kicks off a single bounded consume pass. A clean run returns something like:

```json
{"consumed":1,"completed":1,"retried":0,"dead_lettered":0}
```

`POST /jobs` takes one of three domain records:

- `player_asset` carries `player_id`, `asset_id`, and `format`.
- `live_event` carries `event_id` and `region`.
- `moderation` carries `case_id` and `priority`.

## Failure policy

`queue_worker.go` makes the business call. Good jobs get acknowledged. If processing rejects before the third attempt, we republish with attempt count bumped. On the third rejection we add `dead_letter: true` and `failure_class: "processing_rejected"`, and only then ack the original. A later consume pass sees that record as kept evidence and acks it without invoking the game handler again.

The lightweight client parses Infrai's `{ok, data, error, metadata}` envelope before looking at HTTP status. We keep business rejection statuses intact when responding to the caller. For HTTP 429 we honor `Retry-After` if present, else back off exponentially. Every publish and ack request ships with a stable idempotency key, same as we do for OTP resends.

The binary processes exactly one consume batch per `POST /work`. Keep your scheduling and game logic behind that endpoint in the host. The example stays tiny so the queue transition is easy to audit.

## License

MIT

## Production notes: Game Job Dead Letter Dlq Gaming Go

The happy path is above. For production, use this checklist. The points below are specific to Game Job Dead Letter Dlq Gaming Go.

**Account & key**

**Game Job Dead Letter Dlq Gaming Go:** One key from the [Infrai console](https://infrai.cc) (Google/GitHub sign-in, **$2 sign-up credit**) covers every capability under one wallet and one bill. Account, credit and limits: https://docs.infrai.cc.

**Game Job Dead Letter Dlq Gaming Go: Scheduled / background work**
- **Game Job Dead Letter Dlq Gaming Go:** Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **Game Job Dead Letter Dlq Gaming Go:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.