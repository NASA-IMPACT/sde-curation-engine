"""Thin async wrapper over boto3 S3 (blocking calls run in a thread)."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any


class S3:
    def __init__(self, bucket: str, *, region: str = "us-east-1", client=None):
        if not bucket:
            raise ValueError("S3 bucket name is empty")
        import boto3

        self.bucket = bucket
        self.client = client or boto3.client("s3", region_name=region)

    async def upload_file(self, path: Path, key: str, content_type: str = "application/octet-stream") -> None:
        await asyncio.to_thread(
            self.client.upload_file, str(path), self.bucket, key, ExtraArgs={"ContentType": content_type}
        )

    async def put_json(self, key: str, payload: dict[str, Any]) -> None:
        await asyncio.to_thread(
            self.client.put_object, Bucket=self.bucket, Key=key,
            Body=json.dumps(payload, indent=2, default=str).encode("utf-8"), ContentType="application/json",
        )

    async def get_json(self, key: str) -> dict[str, Any] | None:
        try:
            obj = await asyncio.to_thread(self.client.get_object, Bucket=self.bucket, Key=key)
        except self.client.exceptions.NoSuchKey:
            return None
        except self.client.exceptions.ClientError as e:  # pragma: no cover - other 4xx
            if e.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
                return None
            raise
        return json.loads(obj["Body"].read())

    async def exists(self, key: str) -> bool:
        try:
            await asyncio.to_thread(self.client.head_object, Bucket=self.bucket, Key=key)
            return True
        except self.client.exceptions.ClientError:
            return False

    async def delete(self, *keys: str) -> None:
        for k in keys:
            await asyncio.to_thread(self.client.delete_object, Bucket=self.bucket, Key=k)

    def url(self, key: str) -> str:
        return f"s3://{self.bucket}/{key}"
