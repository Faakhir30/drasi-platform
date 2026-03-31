# SmartRouter Reaction (Python)

This reaction routes Drasi query output (see `drasi-platform/typespec/query-output/main.tsp`) to Dapr pub/sub topics, with per-subscriber options aligned with [PostDaprPubSub](../dapr/post-pubsub).

## Features

- **Change events**: `Unpacked` (one message per row op, same shape as PostDaprPubSub `DrasiChangeFormatter`) or `Packed` (full `ChangeEvent` JSON per typespec).
- **Control events**: Per route, `skipControlSignals` (default **true** for SmartRouter). When false, publishes unpacked (`op: X`) or packed `ControlEvent` like PostDaprPubSub `ControlSignalHandler`.
- **Per route**: Each `(queryId, topic)` has its own `pubsubName`, `format`, `skipControlSignals`.
- HTTP: `GET /queries`, `GET/POST/DELETE /subscriptions`.

## Defaults

- `format`: **Unpacked**
- `skipControlSignals`: **true** (unlike PostDaprPubSub manifest default `false`, SmartRouter is primarily used for agent data topics)

## Query config (YAML)

```yaml
description: "Human readable query purpose"
defaultFormat: Unpacked          # optional
defaultSkipControlSignals: true  # optional; root key skipControlSignals also accepted
defaultPubsubName: messagepubsub # optional; else reaction property pubsubName
# (query, topic) level overrides can be done from subscription endpoints at time of subscribing.
```

## Subscriptions API

`POST /subscriptions` body:

```json
{
  "queryId": "low-stock-event-query",
  "topic": "my-agent-instance-id", # optional, {{queryId}}-topic is automatically used otherwise.
  "pubsubName": "messagepubsub", # TODO: make this optional by providing a defult pubsub component.
  "format": "Unpacked",
  "skipControlSignals": true
}
```

`GET /subscriptions` returns each subscription as `{ topic, pubsubName, format, skipControlSignals }`.

## Try it out

Deploy SmartRouter, then from `dapr-ext-drasi` use `examples/02_durable_agent_smart_router.py` or call `POST /subscriptions` and subscribe your agent to the same pub/sub topic.
