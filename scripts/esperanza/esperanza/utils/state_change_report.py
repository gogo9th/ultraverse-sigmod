import json

from typing import TypedDict


class StateChangeReport(TypedDict):
    intermediateDBName: str

    replaceQuery: str
    replayGidCount: int
    totalCount: int

    sqlLoadTime: float
    executionTime: float
    threadNum: int


def read_state_change_report(path: str) -> StateChangeReport:
    with open(path, "r") as f:
        return json.load(f)