import typing as t
from datetime import datetime
from google.cloud import bigquery
import pandas as pd
from sqlmesh import ExecutionContext, model

@model(
    "test.shakespeare",
    columns={
        "title": "string",
        "unique_words": "int",
    },
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:

    




