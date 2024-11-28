import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model

from lib.utils import update_metadata


@model(
    "sha.metadata",
    depends_on=["sha.provedores", "sha.producao_ambulatorial", "sha.producao_hospitalar", "sha.procedimentos"],
    columns={
        "id": "TEXT",
        "created_at": "TIMESTAMP",
        "metadata": "TEXT"}
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
):
    
    try:
        return update_metadata(context, execution_time, "br_ms.sha")
    except Exception as e:
        print(f"Error: {str(e)}")
        raise
        