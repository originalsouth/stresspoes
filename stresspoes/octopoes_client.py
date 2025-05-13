import datetime
import sys
from typing import Any

import httpx
from pydantic import JsonValue


class OctopoesClient:
    def __init__(self, base_url: str, organisation: str, timeout: int | None = None):
        self.url = base_url
        self.org = organisation
        self.timeout = timeout
        self._root = httpx.Client(
            base_url=f"{self.url}",
            headers={"Accept": "application/json"},
            timeout=self.timeout,
        )
        self._client = httpx.Client(
            base_url=f"{self.url}/{self.org}",
            headers={"Accept": "application/json"},
            timeout=self.timeout,
        )

    def _organisation(self, org: str):
        self.org = org
        self._root = httpx.Client(
            base_url=f"{self.url}",
            headers={"Accept": "application/json"},
            timeout=self.timeout,
        )
        self._client = httpx.Client(
            base_url=f"{self.url}/{self.org}",
            headers={"Accept": "application/json"},
            timeout=self.timeout,
        )

    def _gettime(self, time: datetime.datetime | None) -> str:
        if time is None:
            time = datetime.datetime.now(tz=datetime.timezone.utc)
        return time.isoformat()

    def roothealth(self) -> JsonValue:
        res = self._root.get("/health")

        return res.json()

    def health(self) -> JsonValue:
        res = self._client.get("/health")

        return res.json()

    def objects(
        self,
        scan_level: list[int] = [0, 1, 2, 3, 4],
        scan_profile_type: list[str] = ["inherited", "declared", "empty"],
        offset: int = 0,
        limit: int = sys.maxsize,
        search_string: str | None = None,
        order_by: str = "object_type",
        asc_desc: str = "asc",
        types: list[str] = ["OOI"],
        valid_time: datetime.datetime | None = None,
    ) -> JsonValue:
        params = {
            "scan_level": scan_level,
            "scan_profile_type": scan_profile_type,
            "offset": offset,
            "limit": limit,
            "search_string": search_string,
            "order_by": order_by,
            "asc_desc": asc_desc,
            "types": types,
            "valid_time": self._gettime(valid_time),
        }
        params = {k: v for k, v in params.items() if v is not None}

        res = self._client.get("/objects", params=params)

        return res.json()

    def query(
        self,
        path: str,
        offset: int = 0,
        limit: int = sys.maxsize,
        valid_time: datetime.datetime | None = None,
    ) -> JsonValue:
        params = {
            "path": path,
            "offset": offset,
            "limit": limit,
            "valid_time": self._gettime(valid_time),
        }

        res = self._client.get("/query", params=params)

        return res.json()

    def query_many(
        self,
        path: str,
        sources: list[str],
        valid_time: datetime.datetime = datetime.datetime.now(tz=datetime.timezone.utc),
    ) -> JsonValue:
        params = {
            "path": path,
            "sources": sources,
            "valid_time": self._gettime(valid_time),
        }

        res = self._client.get("/query_many", params=params)

        return res.json()

    def load_bulk(
        self,
        references: list[str],
        valid_time: datetime.datetime | None = None,
    ) -> JsonValue:
        params = {
            "valid_time": self._gettime(valid_time),
        }

        res = self._client.post("/objects/load_bulk", params=params, json=references)

        return res.json()

    def object(
        self,
        reference: str,
        valid_time: datetime.datetime | None = None,
    ) -> JsonValue:
        params = {
            "reference": reference,
            "valid_time": self._gettime(valid_time),
        }

        res = self._client.get("/object", params=params)

        return res.json()

    def object_history(
        self,
        reference: str,
        sort_order: str = "asc",
        with_docs: bool = False,
        has_doc: bool | None = None,
        offset: int = 0,
        limit: int | None = None,
        indices: list[int] | None = None,
    ) -> JsonValue:
        params = {
            "reference": reference,
            "sort_order": sort_order,
            "with_docs": with_docs,
            "has_doc": has_doc,
            "offset": offset,
            "limit": limit,
            "indices": indices,
        }

        res = self._client.get("/object-history", params=params)

        return res.json()

    def random(
        self,
        key: str,
        amount: int = 1,
        scan_level: list[int] = [0, 1, 2, 3, 4],
        valid_time: datetime.datetime | None = None,
    ) -> JsonValue:
        params = {
            "key": key,
            "amount": amount,
            "scan_level": scan_level,
            "valid_time": self._gettime(valid_time),
        }

        res = self._client.get("/objects/random", params=params)

        return res.json()

    def delete(
        self, reference: str, valid_time: datetime.datetime | None = None
    ) -> JsonValue:
        params = {
            "reference": reference,
            "valid_time": self._gettime(valid_time),
        }

        res = self._client.delete("/", params=params)

        return res.json()

    def delete_origin(
        self, origin_id: str, valid_time: datetime.datetime | None = None
    ) -> JsonValue:
        params = {
            "origin_id": origin_id,
            "valid_time": self._gettime(valid_time),
        }

        res = self._client.delete("/origins", params=params)

        return res.json()

    def delete_many(
        self, references: list[str], valid_time: datetime.datetime | None = None
    ) -> JsonValue:
        params = {
            "valid_time": self._gettime(valid_time),
        }

        res = self._client.post("/objects/delete_many", params=params, json=references)

        return res.json()

    def tree(
        self,
        types: list[str],
        reference: str,
        amount: int = 1,
        valid_time: datetime.datetime | None = None,
    ) -> JsonValue:
        params = {
            "types": types,
            "reference": reference,
            "amount": amount,
            "valid_time": self._gettime(valid_time),
        }

        res = self._client.get("/tree", params=params)

        return res.json()

    def origins(
        self,
        offset: int = 0,
        limit: int = sys.maxsize,
        source: str | None = None,
        result: str | None = None,
        method: str | list[str] | None = None,
        task_id: str | None = None,
        origin_type: str | None = None,
        valid_time: datetime.datetime | None = None,
    ) -> JsonValue:
        params = {
            "offset": offset,
            "limit": limit,
            "source": source,
            "result": result,
            "method": method,
            "task_id": task_id,
            "origin_type": origin_type,
            "valid_time": self._gettime(valid_time),
        }
        params = {k: v for k, v in params.items() if v is not None}

        res = self._client.get("/origins", params=params)

        return res.json()

    def origin_parameters(
        self, origin_id: str, valid_time: datetime.datetime | None = None
    ) -> JsonValue:
        params = {
            "origin_id": origin_id,
            "valid_time": self._gettime(valid_time),
        }

        res = self._client.get("/origin_parameters", params=params)

        return res.json()

    def save_observation(
        self, origin: dict[str, Any], valid_time: datetime.datetime | None = None
    ) -> JsonValue:
        origin["valid_time"] = self._gettime(valid_time)

        res = self._client.post("/observations", json=origin)

        return res.json()

    def save_declaration(
        self, origin: dict[str, Any], valid_time: datetime.datetime | None = None
    ) -> JsonValue:
        origin["valid_time"] = self._gettime(valid_time)

        res = self._client.post("/declarations", json=origin)

        return res.json()

    def save_many_declarations(
        self, origins: list[dict[str, Any]], valid_time: datetime.datetime | None = None
    ) -> JsonValue:
        for origin in origins:
            origin["valid_time"] = self._gettime(valid_time)

        res = self._client.post("/declarations/save_many", json=origins)

        return res.json()

    def save_affirmations(
        self, origin: dict[str, Any], valid_time: datetime.datetime | None = None
    ) -> JsonValue:
        origin["valid_time"] = self._gettime(valid_time)

        res = self._client.post("/affirmations", json=origin)

        return res.json()

    def findings(
        self,
        exclude_muted: bool = True,
        only_muted: bool = False,
        offset: int = 0,
        limit: int = sys.maxsize,
        severities: list[str] = [
            "unknown",
            "recommendation",
            "high",
            "critical",
            "pending",
            "low",
            "medium",
        ],
        search_string: str | None = None,
        order_by: str = "score",
        asc_desc: str = "asc",
        valid_time: datetime.datetime | None = None,
    ) -> JsonValue:
        params = {
            "exclude_muted": exclude_muted,
            "only_muted": only_muted,
            "offset": offset,
            "limit": limit,
            "severities": severities,
            "search_string": search_string,
            "order_by": order_by,
            "asc_desc": asc_desc,
            "valid_time": self._gettime(valid_time),
        }
        params = {k: v for k, v in params.items() if v is not None}

        res = self._client.get("/findings", params=params)

        return res.json()

    def findings_count_by_severity(
        self, valid_time: datetime.datetime | None = None
    ) -> JsonValue:
        params = {"valid_time": self._gettime(valid_time)}

        res = self._client.get("/findings/count_by_severity", params=params)

        return res.json()

    def node_create(self, organisation: str) -> JsonValue:
        res = self._root.post(f"{organisation}/node")

        return res.json()

    def node_delete(self, organisation: str) -> JsonValue:
        res = self._root.delete(f"{organisation}/node")

        return res.json()

    def bits_recalculate(self) -> JsonValue:
        res = self._client.post("bits/recalculate")

        return res.json()

    def scan_profile(
        self, scan_profile_type: str | None, valid_time: datetime.datetime | None = None
    ) -> JsonValue:
        params = {
            "scan_profile_type": scan_profile_type,
            "valid_time": self._gettime(valid_time),
        }
        params = {k: v for k, v in params.items() if v is not None}

        res = self._client.get("/scan_profiles", params=params)

        return res.json()

    def save_scan_profile(
        self, scan_profile: dict[str, Any], valid_time: datetime.datetime | None = None
    ) -> JsonValue:
        params = {"valid_time": self._gettime(valid_time)}

        res = self._client.put("/scan_profiles", params=params, json=scan_profile)

        return res.json()

    def save_many_scan_profile(
        self,
        scan_profile: list[dict[str, Any]],
        valid_time: datetime.datetime | None = None,
    ) -> JsonValue:
        params = {"valid_time": self._gettime(valid_time)}

        res = self._client.post(
            "/scan_profiles/save_many", params=params, json=scan_profile
        )

        return res.json()

    def scan_profiles_recalculate(
        self, valid_time: datetime.datetime | None = None
    ) -> JsonValue:
        params = {"valid_time": self._gettime(valid_time)}

        res = self._client.get("/scan_profiles/recalculate", params=params)

        return res.json()

    def scan_profiles_inheritance(
        self, reference: str, valid_time: datetime.datetime | None = None
    ) -> JsonValue:
        params = {
            "reference": reference,
            "valid_time": self._gettime(valid_time),
        }

        res = self._client.get("/scan_profiles/recalculate", params=params)

        return res.json()
