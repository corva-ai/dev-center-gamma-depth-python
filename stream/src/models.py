from __future__ import annotations

import copy
from typing import List, Optional, Set

import pydantic
from corva import StreamTimeEvent, StreamTimeRecord


class WitsRecordMetadata(pydantic.BaseModel):
    drillstring_id: Optional[str] = pydantic.Field(None, alias="drillstring")


class WitsRecordData(pydantic.BaseModel):
    bit_depth: float
    gamma_ray: float


class WitsRecord(StreamTimeRecord):
    data: WitsRecordData
    metadata: WitsRecordMetadata


class GammaDepthEvent(StreamTimeEvent):
    records: pydantic.conlist(WitsRecord, min_items=1)

    @staticmethod
    def filter_records(event: GammaDepthEvent) -> List[WitsRecord]:
        """filters records with no drillstring_id"""

        new_records = [
            copy.deepcopy(record)
            for record in event.records
            if record.metadata.drillstring_id
        ]

        return new_records

    @property
    def drillstring_ids(self) -> Set[str]:
        """returns unique drillstring ids"""

        ids = set(
            record.metadata.drillstring_id
            for record in self.records
            if record.metadata and record.metadata.drillstring_id
        )

        return ids


class DrillstringDataComponent(pydantic.BaseModel):
    family: str
    gamma_sensor_to_bit_distance: Optional[float]
    has_gamma_sensor: Optional[bool] = False

    @property
    def is_mwd_with_gamma_sensor(self):
        return (
            self.family == 'mwd'
            and self.has_gamma_sensor
            and self.gamma_sensor_to_bit_distance is not None
        )


class DrillstringData(pydantic.BaseModel):
    components: List[DrillstringDataComponent]


class Drillstring(pydantic.BaseModel):
    """Needed subset of drillstring response fields"""

    id: str = pydantic.Field(..., alias="_id")
    data: DrillstringData

    @property
    def mwd_with_gamma_sensor(self) -> Optional[DrillstringDataComponent]:
        """returns MWD component with a gamma sensor"""

        for component in self.data.components:
            if component.is_mwd_with_gamma_sensor:
                return component

        return None


class ActualGammaDepthData(pydantic.BaseModel):
    bit_depth: float
    gamma_depth: float
    gamma_ray: float


class ActualGammaDepth(pydantic.BaseModel):
    asset_id: int
    collection: str
    company_id: int
    data: ActualGammaDepthData
    provider: str
    timestamp: int
    version: int
