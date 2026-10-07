"""HTTP 接入层：将进程内 Gateway 暴露为 REST API。"""
from __future__ import annotations

import time
from typing import Any

import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel, Field

from gateway.models import BatchResult, IngestResult, TelemetryMessage
from gateway.service import Gateway

app = FastAPI(title="IoT Device Twin Gateway")
gateway = Gateway()


def _now() -> float:
    return time.time()


def _ingest_result_dict(r: IngestResult) -> dict[str, Any]:
    return {
        "accepted": r.accepted,
        "flags": [f.value for f in r.flags],
        "auth_failure": r.auth_failure.value if r.auth_failure else None,
        "reject_reason": r.reject_reason.value if r.reject_reason else None,
        "applied_seq": r.applied_seq,
        "detail": r.detail,
    }


def _batch_result_dict(b: BatchResult) -> dict[str, Any]:
    return {
        "batch_id": b.batch_id,
        "duplicate_batch": b.duplicate_batch,
        "items": [{"index": it.index, **_ingest_result_dict(it.result)}
                  for it in b.items],
    }


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


class BatchRequest(BaseModel):
    batch_id: str = Field(min_length=1)
    messages: list[Any]


class DesiredRequest(BaseModel):
    desired: dict[str, Any]
    expected_version: int


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
    return {"ok": True, "revoked": gateway.registry.revoke(device_id)}


@app.post("/telemetry")
def ingest(req: TelemetryRequest):
    msg = TelemetryMessage(
        device_id=req.device_id, secret=req.secret, message_id=req.message_id,
        seq=req.seq, timestamp=req.timestamp,
        protocol_version=req.protocol_version, payload=req.payload)
    return _ingest_result_dict(gateway.ingest(msg, _now()))


@app.post("/telemetry/raw")
def ingest_raw(raw: Any):
    """单条原始上报：任何输入都返回结构化判定，绝不因负载异常中断。"""
    return _ingest_result_dict(gateway.ingest_raw(raw, _now()))


@app.post("/telemetry/batch")
def ingest_batch(req: BatchRequest):
    """批量补传：逐条下结论；同一 batch_id 重试只生效一次。"""
    return _batch_result_dict(gateway.ingest_batch(req.batch_id, req.messages,
                                                   _now()))


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


@app.get("/rejections")
def rejections(device_id: str | None = None, category: str | None = None,
               limit: int = 100):
    """排查视图：按设备/分类查最近未接受的上报（最新在前）。"""
    from gateway.models import RejectionCategory
    cat = None
    if category is not None:
        try:
            cat = RejectionCategory(category)
        except ValueError:
            return {"ok": False, "error": f"unknown_category:{category}",
                    "valid": [c.value for c in RejectionCategory]}
    records = gateway.rejections.query(device_id=device_id, category=cat,
                                       limit=limit)
    return {
        "records": [
            {"device_id": r.device_id, "category": r.category.value,
             "detail": r.detail, "message_id": r.message_id, "seq": r.seq,
             "raw": r.raw, "recorded_at": r.recorded_at}
            for r in records
        ],
        "stats": gateway.rejections.stats(),
    }


@app.get("/rejections/stats")
def rejection_stats():
    return {"stats": gateway.rejections.stats(),
            "category_totals": {c.value: n for c, n in
                                gateway.rejections.category_totals().items()}}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
