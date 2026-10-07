"""HTTP 接入层：将进程内 Gateway 暴露为 REST API。"""
from __future__ import annotations

import time
from typing import Any

import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

from gateway.models import RejectCategory, TelemetryMessage
from gateway.service import Gateway

app = FastAPI(title="IoT Device Twin Gateway")
gateway = Gateway()


def _now() -> float:
    return time.time()


class RegisterRequest(BaseModel):
    device_id: str
    secret: str
    ttl_seconds: float = 3600.0


class RotateRequest(BaseModel):
    new_secret: str
    ttl_seconds: float = 3600.0


class TelemetryRequest(BaseModel):
    device_id: str
    secret: str
    message_id: str
    seq: int
    timestamp: float
    protocol_version: int = 1
    payload: Any = None


class DesiredRequest(BaseModel):
    desired: dict[str, Any]
    expected_version: int


def _item_json(v) -> dict[str, Any]:
    return {
        "index": v.index,
        "message_id": v.message_id,
        "accepted": v.accepted,
        "flags": [f.value for f in v.flags],
        "reject_reason": v.reject_reason.value if v.reject_reason else None,
        "payload_issue": v.payload_issue.value if v.payload_issue else None,
        "issue_detail": v.issue_detail,
        "applied_seq": v.applied_seq,
    }


def _record_json(rec) -> dict[str, Any]:
    return {
        "seq_no": rec.seq_no,
        "received_at": rec.received_at,
        "device_id": rec.device_id,
        "category": rec.category.value,
        "reason": rec.reason,
        "detail": rec.detail,
        "message_id": rec.message_id,
        "msg_seq": rec.msg_seq,
        "raw": rec.raw,
    }


@app.post("/devices")
def register(req: RegisterRequest):
    device = gateway.registry.register(req.device_id, req.secret, _now(),
                                       req.ttl_seconds)
    return {"device_id": device.device_id, "status": device.status.value,
            "registered_at": device.registered_at}


@app.post("/devices/{device_id}/rotate")
def rotate(device_id: str, req: RotateRequest):
    try:
        cred = gateway.registry.rotate_credential(device_id, req.new_secret,
                                                  _now(), req.ttl_seconds)
    except KeyError:
        return {"ok": False, "error": "unknown_device"}
    return {"ok": True, "not_before": cred.not_before, "not_after": cred.not_after}


@app.post("/devices/{device_id}/revoke")
def revoke(device_id: str):
    return {"ok": gateway.registry.revoke(device_id)}


@app.post("/telemetry")
def ingest(req: TelemetryRequest):
    msg = TelemetryMessage(
        device_id=req.device_id, secret=req.secret, message_id=req.message_id,
        seq=req.seq, timestamp=req.timestamp,
        protocol_version=req.protocol_version, payload=req.payload)
    result = gateway.ingest(msg, _now())
    return {
        "accepted": result.accepted,
        "flags": [f.value for f in result.flags],
        "auth_failure": result.auth_failure.value if result.auth_failure else None,
        "reject_reason": result.reject_reason.value if result.reject_reason else None,
        "payload_issue": result.payload_issue.value if result.payload_issue else None,
        "issue_detail": result.issue_detail,
        "applied_seq": result.applied_seq,
    }


@app.post("/telemetry/batch")
def ingest_batch(body: dict[str, Any]):
    """批量补传。入参不做严格 schema 绑定：任何离谱输入都返回结构化判定。"""
    batch_id = body.get("batch_id")
    device_id = body.get("device_id")
    secret = body.get("secret")
    items = body.get("items")
    if not isinstance(device_id, str) or not device_id.strip():
        return {"batch_id": batch_id if isinstance(batch_id, str) else None,
                "auth_ok": False, "error": "malformed_batch_request",
                "detail": "device_id: expected non-empty string", "items": []}
    if not isinstance(secret, str) or not secret:
        return {"batch_id": batch_id if isinstance(batch_id, str) else None,
                "auth_ok": False, "error": "malformed_batch_request",
                "detail": "secret: expected non-empty string", "items": []}
    if batch_id is not None and not isinstance(batch_id, str):
        return {"batch_id": None, "auth_ok": False,
                "error": "malformed_batch_request",
                "detail": "batch_id: expected string or null", "items": []}
    if not isinstance(items, list):
        return {"batch_id": batch_id, "auth_ok": False,
                "error": "malformed_batch_request",
                "detail": "items: expected a JSON array", "items": []}
    result = gateway.ingest_batch(batch_id, device_id, secret, items, _now())
    return {
        "batch_id": result.batch_id,
        "auth_ok": result.auth_ok,
        "auth_failure": result.auth_failure.value if result.auth_failure else None,
        "accepted_count": result.accepted_count,
        "items": [_item_json(v) for v in result.items],
    }


@app.get("/rejections")
def query_rejections(device_id: str | None = None,
                     category: str | None = None,
                     limit: int | None = None):
    cat = None
    if category is not None:
        try:
            cat = RejectCategory(category)
        except ValueError:
            return {"error": "unknown_category",
                    "supported": [c.value for c in RejectCategory]}
    if limit is not None and limit < 0:
        limit = None
    recs = gateway.rejections.query(device_id=device_id, category=cat,
                                    limit=limit)
    return {"capacity": gateway.rejections.capacity, "count": len(recs),
            "records": [_record_json(r) for r in recs]}


@app.get("/rejections/counts")
def rejection_counts(device_id: str | None = None):
    return {"device_id": device_id,
            "counts": gateway.rejections.counts(device_id)}


@app.get("/twin/{device_id}")
def get_twin(device_id: str):
    twin = gateway.get_twin(device_id)
    return {
        "desired": twin.desired, "desired_version": twin.desired_version,
        "reported": twin.reported, "reported_version": twin.reported_version,
    }


@app.post("/twin/{device_id}/desired")
def set_desired(device_id: str, req: DesiredRequest):
    result = gateway.set_desired(device_id, req.desired, req.expected_version)
    return {"ok": result.ok, "version": result.version,
            "conflict": result.conflict, "detail": result.detail}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
