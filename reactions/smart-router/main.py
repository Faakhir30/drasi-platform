import logging
from collections import defaultdict
from typing import Any, Dict, Optional, Set

from dapr.clients import DaprClient
from fastapi import Body
from fastapi import HTTPException

from drasi.reaction.models.ChangeEvent import ChangeEvent
from drasi.reaction.models.ControlEvent import ControlEvent
from drasi.reaction.sdk import DrasiReaction
from drasi.reaction.utils import get_config_value, yaml_query_configs

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("smart_router_reaction")

PUBSUB_NAME = get_config_value("pubsubName", "messagepubsub")

# queryId -> set(topic)
SUBSCRIPTIONS: Dict[str, Set[str]] = defaultdict(set)
# queryId -> description
QUERY_DESCRIPTIONS: Dict[str, str] = {}


def _emit_payload(query_id: str, operation: str, payload: Dict[str, Any], topic: str) -> None:
    envelope = {
        "queryId": query_id,
        "operation": operation,
        "data": payload,
    }
    with DaprClient() as client:
        client.publish_event(
            pubsub_name=PUBSUB_NAME,
            topic_name=topic,
            data=envelope,
            data_content_type="application/json",
        )


async def on_change_event(event: ChangeEvent, query_config: dict[str, Any] | None) -> None:
    query_id = event.queryId
    if query_config:
        QUERY_DESCRIPTIONS[query_id] = query_config.get("description", "") or ""

        for topic in query_config.get("initialTopics", []) or []:
            SUBSCRIPTIONS[query_id].add(topic)

    topics = list(SUBSCRIPTIONS.get(query_id, set()))
    if not topics:
        logger.info("No subscribers for query %s", query_id)
        return

    for added in event.addedResults or []:
        for topic in topics:
            _emit_payload(query_id, "added", added, topic)

    for updated in event.updatedResults or []:
        payload = {"before": updated.before, "after": updated.after}
        for topic in topics:
            _emit_payload(query_id, "updated", payload, topic)

    for deleted in event.deletedResults or []:
        for topic in topics:
            _emit_payload(query_id, "deleted", deleted, topic)


async def on_control_event(event: ControlEvent, query_config: dict[str, Any] | None) -> None:
    if query_config:
        QUERY_DESCRIPTIONS[event.queryId] = query_config.get("description", "") or ""
    logger.info(
        "Received control signal %s for query %s", event.controlSignal, event.queryId
    )


if __name__ == "__main__":
    reaction = DrasiReaction(
        on_change_event=on_change_event,
        on_control_event=on_control_event,
        parse_query_configs=yaml_query_configs,
    )

    app = reaction._app

    @app.get("/queries")
    async def list_queries() -> Dict[str, Any]:
        return {
            "queries": [
                {"queryId": query_id, "description": QUERY_DESCRIPTIONS.get(query_id, "")}
                for query_id in sorted(reaction.query_configs.keys())
            ]
        }

    @app.get("/subscriptions")
    async def list_subscriptions(queryId: Optional[str] = None) -> Dict[str, Any]:
        if queryId:
            return {
                "subscriptions": {
                    queryId: sorted(list(SUBSCRIPTIONS.get(queryId, set())))
                }
            }
        return {
            "subscriptions": {
                query_id: sorted(list(topics))
                for query_id, topics in SUBSCRIPTIONS.items()
            }
        }

    @app.post("/subscriptions")
    async def subscribe(payload: Dict[str, str] = Body(...)) -> Dict[str, Any]:
        query_id = payload.get("queryId")
        topic = payload.get("topic")
        if not query_id or not topic:
            raise HTTPException(status_code=400, detail="queryId and topic are required")
        if query_id not in reaction.query_configs:
            raise HTTPException(status_code=404, detail=f"Unknown queryId: {query_id}")

        SUBSCRIPTIONS[query_id].add(topic)
        return {"queryId": query_id, "topic": topic, "subscribed": True}

    @app.delete("/subscriptions")
    async def unsubscribe(payload: Dict[str, str] = Body(...)) -> Dict[str, Any]:
        query_id = payload.get("queryId")
        topic = payload.get("topic")
        if not query_id or not topic:
            raise HTTPException(status_code=400, detail="queryId and topic are required")
        SUBSCRIPTIONS[query_id].discard(topic)
        return {"queryId": query_id, "topic": topic, "subscribed": False}

    reaction.start()
