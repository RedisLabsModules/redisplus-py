from .client import Client
from .result import Result
from .document import Result
from .field import NumericField, TextField, GeoField, TagField
from .indexDefinition import IndexDefinition, IndexType
from .query import Query, NumericFilter, GeoFilter, SortbyField
from .aggregation import AggregateRequest, AggregateResult
from .auto_complete import AutoCompleter, Suggestion

__all__ = [
    "Client",
    "Result",
    "NumericField",
    "TextField",
    "GeoField",
    "TagField"
]
