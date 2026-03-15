
from __future__ import annotations

import shutil
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Union


class StorageClient(ABC):

    @abstractmethod
    def put_object(self, key: str, data: Union[bytes, str]) -> None: ...

    @abstractmethod
    def get_object(self, key: str) -> bytes: ...

    @abstractmethod
    def list_objects(self, prefix: str = "") -> list[str]: ...

    @abstractmethod
    def delete_object(self, key: str) -> None: ...

    @abstractmethod
    def exists(self, key: str) -> bool: ...

    @abstractmethod
    def key_to_path(self, key: str) -> str: ...


class LocalS3Client(StorageClient):

    def __init__(self, root: Union[str, Path]) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _resolve(self, key: str) -> Path:
        resolved = (self.root / key).resolve()
        if not str(resolved).startswith(str(self.root.resolve())):
            raise ValueError(f"Invalid key (path traversal): {key}")
        return resolved

    def put_object(self, key: str, data: Union[bytes, str]) -> None:
        path = self._resolve(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        mode = "wb" if isinstance(data, bytes) else "w"
        with open(path, mode) as f:
            f.write(data)

    def get_object(self, key: str) -> bytes:
        path = self._resolve(key)
        with open(path, "rb") as f:
            return f.read()

    def list_objects(self, prefix: str = "") -> list[str]:
        base = self._resolve(prefix) if prefix else self.root
        if not base.exists():
            return []
        results: list[str] = []
        for p in sorted(base.rglob("*")):
            if p.is_file():
                results.append(str(p.relative_to(self.root)))
        return results

    def delete_object(self, key: str) -> None:
        path = self._resolve(key)
        if path.is_file():
            path.unlink()
        elif path.is_dir():
            shutil.rmtree(path)

    def exists(self, key: str) -> bool:
        return self._resolve(key).exists()

    def key_to_path(self, key: str) -> str:
        return str(self._resolve(key))

    def __repr__(self) -> str:
        return f"LocalS3Client(root={self.root!r})"


class MinioS3Client(StorageClient):

    def __init__(
        self,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        bucket: str,
        secure: bool = False,
    ) -> None:
        import boto3
        scheme = "https" if secure else "http"
        self._bucket = bucket
        self._s3 = boto3.client(
            "s3",
            endpoint_url=f"{scheme}://{endpoint_url}",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )
        try:
            self._s3.head_bucket(Bucket=bucket)
        except Exception:
            self._s3.create_bucket(Bucket=bucket)

    def put_object(self, key: str, data: Union[bytes, str]) -> None:
        body = data.encode() if isinstance(data, str) else data
        self._s3.put_object(Bucket=self._bucket, Key=key, Body=body)

    def get_object(self, key: str) -> bytes:
        resp = self._s3.get_object(Bucket=self._bucket, Key=key)
        return resp["Body"].read()

    def list_objects(self, prefix: str = "") -> list[str]:
        keys: list[str] = []
        paginator = self._s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
        return keys

    def delete_object(self, key: str) -> None:
        self._s3.delete_object(Bucket=self._bucket, Key=key)

    def exists(self, key: str) -> bool:
        try:
            self._s3.head_object(Bucket=self._bucket, Key=key)
            return True
        except Exception:
            return False

    def key_to_path(self, key: str) -> str:
        return f"s3://{self._bucket}/{key}"

    def __repr__(self) -> str:
        return f"MinioS3Client(bucket={self._bucket!r})"


def get_storage_client() -> StorageClient:
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
    from config.settings import (
        LOCAL_DATA_LAKE_ROOT,
        MINIO_ACCESS_KEY,
        MINIO_BUCKET,
        MINIO_ENDPOINT,
        MINIO_SECRET_KEY,
        MINIO_SECURE,
        STORAGE_BACKEND,
    )

    if STORAGE_BACKEND == "minio":
        return MinioS3Client(
            endpoint_url=MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            bucket=MINIO_BUCKET,
            secure=MINIO_SECURE,
        )

    return LocalS3Client(root=LOCAL_DATA_LAKE_ROOT)
