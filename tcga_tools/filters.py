from __future__ import annotations
from typing import Any, Dict, Iterable, List


class Filters:
    """Helper to build GDC filter JSON objects.


    Examples
    --------
    >>> f = Filters.AND(
    ... Filters.EQ("cases.project.project_id", "TCGA-LUSC"),
    ... Filters.IN("data_format", ["SVS"]) )
    >>> f
    {'op': 'and', 'content': [{'op': '=', 'content': {'field': 'cases.project.project_id', 'value': ['TCGA-LUSC']}}, {'op': 'in', 'content': {'field': 'data_format', 'value': ['SVS']}}]}
    """


    @staticmethod
    def _wrap_value(v: Any) -> List[Any]:
        return v if isinstance(v, list) else [v]


    @staticmethod
    def EQ(field: str, value: Any) -> Dict[str, Any]:
        return {"op": "=", "content": {"field": field, "value": Filters._wrap_value(value)}}


    @staticmethod
    def IN(field: str, values: Iterable[Any]) -> Dict[str, Any]:
        return {"op": "in", "content": {"field": field, "value": list(values)}}


    @staticmethod
    def AND(*parts: Dict[str, Any]) -> Dict[str, Any]:
        return {"op": "and", "content": list(parts)}


    @staticmethod
    def OR(*parts: Dict[str, Any]) -> Dict[str, Any]:
        return {"op": "or", "content": list(parts)}