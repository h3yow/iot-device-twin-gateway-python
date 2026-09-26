"""HTTP 接入层：将 GatewayService 暴露为 REST API（演示/联调用）。"""
from __future__ import annotations

import time
from typing import Any

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from gateway.models import TelemetryMessage
from gateway.service import GatewayService
from gateway.twin import VersionConflict

app = FastAPI(title="IoT Device Twin Gateway")
service = GatewayService()


def _now() -> float:
    return time.time()


class RegisterIn(BaseModel):
    device_id: str
    secret: str


class RotateIn(BaseModel):
    new_secret: str


class TelemetryIn(BaseModel):
    device_id: str
    secret: str
    seq: int
    timestamp: float
    payload: dict[str, Any]
    version: int = 1


class DesiredIn(BaseModel):
    value: dict[str, Any]
    base_version: int
    writer_id: str = Field(default="api")


@app.get("/")
def read_root() -> dict[str, str]:
    return {"Hello": "World"}


@app.post("/devices", status_code=201)
def register(body: RegisterIn) -> dict[str, Any]:
    device = service.register(body.device_id, body.secret, _now())
    return {"device_id": device.device_id, "status": device.status.value}


@app.post("/devices/{device_id}/rotate")
def rotate(device_id: str, body: RotateIn) -> dict[str, str]:
    try:
        service.rotate_credential(device_id, body.new_secret, _now())
    except KeyError:
        raise HTTPException(404, "unknown device")
    return {"device_id": device_id, "rotated": "true"}


@app.post("/devices/{device_id}/revoke")
def revoke(device_id: str) -> dict[str, str]:
    try:
        service.revoke(device_id)
    except KeyError:
        raise HTTPException(404, "unknown device")
    return {"device_id": device_id, "revoked": "true"}


@app.post("/telemetry")
def ingest(body: TelemetryIn) -> dict[str, Any]:
    msg = TelemetryMessage(
        device_id=body.device_id,
        secret=body.secret,
        seq=body.seq,
        timestamp=body.timestamp,
        payload=body.payload,
        version=body.version,
    )
    result = service.ingest(msg, _now())
    return {
        "verdict": result.verdict.value,
        "auth_failure": result.auth_failure.value if result.auth_failure else None,
        "detail": result.detail,
    }


@app.get("/twins/{device_id}")
def get_twin(device_id: str) -> dict[str, Any]:
    twin = service.get_twin(device_id)
    return {
        "device_id": twin.device_id,
        "desired": twin.desired.__dict__,
        "reported": twin.reported.__dict__,
        "converged": twin.converged,
    }


@app.put("/twins/{device_id}/desired")
def update_desired(device_id: str, body: DesiredIn) -> dict[str, Any]:
    try:
        result = service.update_desired(
            device_id, body.value, body.base_version, body.writer_id, _now()
        )
    except VersionConflict as e:
        raise HTTPException(409, str(e))
    return {"applied": result.applied, "reason": result.reason,
            "version": result.twin.desired.version}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
