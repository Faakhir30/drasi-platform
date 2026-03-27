# SmartRouter Reaction (Python)

This reaction routes Drasi query result changes to dynamic Dapr pub/sub topics.

## Why Python for this reaction

This implementation uses the Python SDK because your agent-side stack is already Python-heavy (`dapr-agents`, `dapr.ext.drasi`) and you asked to optimize for Python proficiency. Using Python keeps the reaction and ToolSet evolution in one language and reduces integration overhead.

## Features

- List configured queries and descriptions via `GET /queries`.
- Dynamic query subscriptions via `POST /subscriptions`.
- Dynamic query unsubscriptions via `DELETE /subscriptions`.
- List active subscriptions via `GET /subscriptions`.
- Fan-out query results to all subscribed topics.

## Subscription model

- An agent instance topic can be its `ctx.instance_id`.
- Subscribing means adding that topic to a query in SmartRouter.
- Subsequent query changes are published to the subscribed topic through configured Dapr pub/sub.

## Query config schema

Each query config supports:

```yaml
description: "Human readable query purpose"
initialTopics:
  - optional-topic-name
skipControlSignals: true
```

## Endpoints

- `GET /queries`
- `GET /subscriptions?queryId=<optional>`
- `POST /subscriptions` with `{"queryId":"...", "topic":"..."}`
- `DELETE /subscriptions` with `{"queryId":"...", "topic":"..."}`

## Try it out (instance_id topic routing)

1) Deploy the SmartRouter reaction (provider + reaction manifest) into your cluster.

2) Run the agent-side listener with a chosen instance topic:

```bash
export SMART_ROUTER_URL="http://smart-router.drasi-system.svc.cluster.local"
export INSTANCE_ID="demo-instance-1"
export QUERY_ID="low-stock-event-query"
python /home/f/w/oss/d/python-sdk/ext/dapr-ext-drasi/examples/02_smart_router_instance_topic.py
```

This will:
- call SmartRouter `POST /subscriptions` so the query routes to the `INSTANCE_ID` topic
- start a workflow listener subscribed to `pubsub=messagepubsub` and `topic=$INSTANCE_ID`
