from __future__ import annotations

import logging
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional, Tuple, Union

from dapr.clients import DaprClient
from fastapi import Body
from fastapi import HTTPException

from drasi.reaction.models.ChangeEvent import ChangeEvent
from drasi.reaction.models.ControlEvent import ControlEvent
from drasi.reaction.models.ResultEvent import ResultEvent
from drasi.reaction.sdk import DrasiReaction
from drasi.reaction.utils import get_config_value, yaml_query_configs

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("smart_router_reaction")

PUBSUB_NAME = get_config_value("pubsubName", "messagepubsub")

OutputFormat = Literal["Unpacked", "Packed"]


@dataclass
class RouteConfig:
    """Per (queryId, topic) routing options, aligned with PostDaprPubSub QueryConfig."""

    pubsub_name: str
    format: OutputFormat = "Unpacked"
    skip_control_signals: bool = True

# TODO: move SUBSCRIPTIONS to statestore for persistance??

# queryId -> topic -> RouteConfig
SUBSCRIPTIONS: Dict[str, Dict[str, RouteConfig]] = defaultdict(dict)
QUERY_DESCRIPTIONS: Dict[str, str] = {}


def _now_ms() -> int:
    return int(time.time() * 1000)


def _normalize_format(value: Any) -> OutputFormat:
    if value is None:
        return "Unpacked"
    s = str(value).strip().lower()
    if s == "packed":
        return "Packed"
    return "Unpacked"


def _record_to_dict(row: Any) -> Optional[Dict[str, Any]]:
    if row is None:
        return None
    if isinstance(row, dict):
        return row
    root = getattr(row, "root", None)
    if root is not None:
        return root if isinstance(root, dict) else None
    if hasattr(row, "model_dump"):
        dumped = row.model_dump(mode="json")
        if isinstance(dumped, dict) and "root" in dumped:
            return dumped.get("root")
        return dumped if isinstance(dumped, dict) else None
    return None


def _query_route_defaults(query_config: Optional[dict[str, Any]]) -> RouteConfig:
    """Defaults for new routes: Unpacked, skipControlSignals True (SmartRouter default)."""
    if not query_config:
        return RouteConfig(pubsub_name=PUBSUB_NAME, format="Unpacked", skip_control_signals=True)
    skip = query_config.get("defaultSkipControlSignals")
    if skip is None:
        skip = query_config.get("skipControlSignals", True)
    fmt = query_config.get("defaultFormat") or query_config.get("format")
    pub = query_config.get("defaultPubsubName") or PUBSUB_NAME
    return RouteConfig(
        pubsub_name=str(pub),
        format=_normalize_format(fmt),
        skip_control_signals=bool(skip),
    )

def bootstrap_subscriptions_from_config(reaction: DrasiReaction) -> None:
    """Apply description from mounted query YAML once at startup."""
    for query_id, cfg in reaction.query_configs.items():
        if not cfg:
            continue
        QUERY_DESCRIPTIONS[query_id] = str(cfg.get("description", "") or "")

def _source_block(query_id: str, source_time_ms: int) -> Dict[str, Any]:
    return {"queryId": query_id, "ts_ms": source_time_ms}


def _metadata_from_result_event(event: ResultEvent) -> Optional[Dict[str, Any]]:
    meta = event.metadata
    if meta is None:
        return None
    d = _record_to_dict(meta)
    return d if d else None


def _iter_unpacked_change_messages(event: ChangeEvent) -> List[Dict[str, Any]]:
    """PostDaprPubSub DrasiChangeFormatter-compatible unpacked notifications."""
    out: List[Dict[str, Any]] = []
    qid = event.queryId
    stm = int(event.sourceTimeMs)
    seq = int(event.sequence)
    meta = _metadata_from_result_event(event)

    for added in event.addedResults or []:
        after = _record_to_dict(added)
        msg: Dict[str, Any] = {
            "op": "I",
            "seq": seq,
            "ts_ms": _now_ms(),
            "payload": {"source": _source_block(qid, stm), "after": after},
        }
        if meta:
            msg["metadata"] = meta
        out.append(msg)

    for updated in event.updatedResults or []:
        before = _record_to_dict(getattr(updated, "before", None))
        after = _record_to_dict(getattr(updated, "after", None))
        msg = {
            "op": "U",
            "seq": seq,
            "ts_ms": _now_ms(),
            "payload": {
                "source": _source_block(qid, stm),
                "before": before,
                "after": after,
            },
        }
        if meta:
            msg["metadata"] = meta
        out.append(msg)

    for deleted in event.deletedResults or []:
        before = _record_to_dict(deleted)
        msg = {
            "op": "D",
            "seq": seq,
            "ts_ms": _now_ms(),
            "payload": {"source": _source_block(qid, stm), "before": before},
        }
        if meta:
            msg["metadata"] = meta
        out.append(msg)

    return out


def _change_event_to_packed_dict(event: ChangeEvent) -> Dict[str, Any]:
    """Full ChangeEvent as one message (typespec ResultEvent / ChangeEvent shape)."""
    return event.model_dump(mode="json", by_alias=True)


def _control_kind_string(control_event: ControlEvent) -> str:
    sig = control_event.controlSignal
    if hasattr(sig, "model_dump"):
        d = sig.model_dump(mode="json")
        return str(d.get("kind", ""))
    return str(getattr(sig, "kind", ""))


def _iter_unpacked_control_messages(event: ControlEvent) -> List[Dict[str, Any]]:
    """PostDaprPubSub ControlSignalHandler unpacked shape (op X)."""
    qid = event.queryId
    stm = int(event.sourceTimeMs)
    seq = int(event.sequence)
    kind = _control_kind_string(event)
    meta = _metadata_from_result_event(event)
    msg: Dict[str, Any] = {
        "op": "X",
        "seq": seq,
        "ts_ms": _now_ms(),
        "payload": {
            "kind": kind,
            "source": _source_block(qid, stm),
        },
    }
    if meta:
        msg["metadata"] = meta
    return [msg]


def _control_event_to_packed_dict(event: ControlEvent) -> Dict[str, Any]:
    return event.model_dump(mode="json", by_alias=True)


def _publish(pubsub_name: str, topic: str, data: Dict[str, Any]) -> None:
    with DaprClient() as client:
        client.publish_event(
            pubsub_name=pubsub_name,
            topic_name=topic,
            data=data,
            data_content_type="application/json",
        )


def _emit_change_for_route(
    event: ChangeEvent, query_id: str, topic: str, route: RouteConfig
) -> None:
    if route.format == "Packed":
        _publish(route.pubsub_name, topic, _change_event_to_packed_dict(event))
        return
    for msg in _iter_unpacked_change_messages(event):
        _publish(route.pubsub_name, topic, msg)


def _emit_control_for_route(
    event: ControlEvent, topic: str, route: RouteConfig
) -> None:
    if route.skip_control_signals:
        return
    if route.format == "Packed":
        _publish(route.pubsub_name, topic, _control_event_to_packed_dict(event))
        return
    for msg in _iter_unpacked_control_messages(event):
        _publish(route.pubsub_name, topic, msg)


async def on_change_event(event: ChangeEvent, query_config: dict[str, Any] | None) -> None:
    query_id = event.queryId
    if query_config and isinstance(query_config, dict):
        QUERY_DESCRIPTIONS[query_id] = str(query_config.get("description", "") or "")

    routes = SUBSCRIPTIONS.get(query_id) or {}
    if not routes:
        logger.info("No subscribers for query %s", query_id)
        return

    for topic, route in list(routes.items()):
        try:
            _emit_change_for_route(event, query_id, topic, route)
        except Exception:
            logger.exception(
                "Failed to publish change for query=%s topic=%s pubsub=%s",
                query_id,
                topic,
                route.pubsub_name,
            )


async def on_control_event(event: ControlEvent, query_config: dict[str, Any] | None) -> None:
    query_id = event.queryId
    if query_config and isinstance(query_config, dict):
        QUERY_DESCRIPTIONS[query_id] = str(query_config.get("description", "") or "")

    logger.info(
        "Received control signal %s for query %s",
        _control_kind_string(event),
        query_id,
    )

    routes = SUBSCRIPTIONS.get(query_id) or {}
    for topic, route in list(routes.items()):
        if route.skip_control_signals:
            continue
        try:
            _emit_control_for_route(event, topic, route)
        except Exception:
            logger.exception(
                "Failed to publish control for query=%s topic=%s pubsub=%s",
                query_id,
                topic,
                route.pubsub_name,
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
        def route_to_dict(topic: str, r: RouteConfig) -> Dict[str, Any]:
            return {
                "topic": topic,
                "pubsubName": r.pubsub_name,
                "format": r.format,
                "skipControlSignals": r.skip_control_signals,
            }

        if queryId:
            routes = SUBSCRIPTIONS.get(queryId) or {}
            return {
                "subscriptions": {
                    queryId: [route_to_dict(t, r) for t, r in sorted(routes.items())]
                }
            }
        return {
            "subscriptions": {
                qid: [route_to_dict(t, r) for t, r in sorted(routes.items())]
                for qid, routes in sorted(SUBSCRIPTIONS.items())
            }
        }

    @app.post("/subscriptions")
    async def subscribe(payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
        query_id = payload.get("queryId")
        topic = payload.get("topic")
        if not query_id or not topic:
            raise HTTPException(status_code=400, detail="queryId and topic are required")
        if query_id not in reaction.query_configs:
            raise HTTPException(status_code=404, detail=f"Unknown queryId: {query_id}")

        qc = reaction.query_configs.get(query_id)
        defaults = _query_route_defaults(qc if isinstance(qc, dict) else None)

        pub = payload.get("pubsubName") or defaults.pubsub_name
        fmt = _normalize_format(payload.get("format") if payload.get("format") is not None else defaults.format)
        if "skipControlSignals" in payload:
            skip = bool(payload["skipControlSignals"])
        else:
            skip = defaults.skip_control_signals

        route = RouteConfig(pubsub_name=str(pub), format=fmt, skip_control_signals=skip)
        SUBSCRIPTIONS[query_id][str(topic)] = route
        return {
            "queryId": query_id,
            "topic": str(topic),
            "pubsubName": route.pubsub_name,
            "format": route.format,
            "skipControlSignals": route.skip_control_signals,
            "subscribed": True,
        }

    @app.delete("/subscriptions")
    async def unsubscribe(payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
        query_id = payload.get("queryId")
        topic = payload.get("topic")
        if not query_id or not topic:
            raise HTTPException(status_code=400, detail="queryId and topic are required")
        routes = SUBSCRIPTIONS.get(query_id)
        if not routes:
            return {
                "queryId": query_id,
                "topic": str(topic),
                "subscribed": False,
                "removed": False,
            }
        removed = routes.pop(str(topic), None)
        if not routes:
            del SUBSCRIPTIONS[query_id]
        return {
            "queryId": query_id,
            "topic": str(topic),
            "subscribed": False,
            "removed": removed is not None,
        }

    try:
        bootstrap_subscriptions_from_config(reaction)
        logger.info("starting python smart-router reaction app")        
        reaction.start()

    except Exception as err:
        logger.error(err)
        import sys

        with open("/dev/termination-log", "w") as f:
            f.write(str(err))
        sys.exit(1)
