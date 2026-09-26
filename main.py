"""HTTP 接入层：将进程内 Gateway 暴露为 REST API。"""
from __future__ import annotations

import time
from typing import Any

import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

from gateway.models import TelemetryMessage
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
        "applied_seq": result.applied_seq,
    }


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
