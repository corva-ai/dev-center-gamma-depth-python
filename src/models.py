from __future__ import annotations

import copy
from typing import List, Optional, Set

import pydantic
from corva import StreamEvent
from corva.models.stream import Record, RecordData


class GammaDepthRecordMetadata(pydantic.BaseModel):
    drillstring_id: Optional[str] = pydantic.Field(None, alias='drillstring')


class GammaDepthRecordData(RecordData):
    bit_depth: float
    gamma_ray: int


class GammaDepthRecord(Record):
    data: GammaDepthRecordData
    metadata: GammaDepthRecordMetadata


class GammaDepthEvent(StreamEvent):
    records: List[GammaDepthRecord]

    @staticmethod
    def filter_records_with_no_drillstring_id(
        event: GammaDepthEvent,
    ) -> GammaDepthEvent:
        new_records = [
            record
            for record in event.records
            if record.metadata.drillstring_id is not None
        ]
        return event.copy(update={'records': new_records}, deep=True)

    @property
    def drillstring_ids(self) -> Set[str]:
        """returns unique drillstring ids"""

        ids = set(
            record.metadata.drillstring_id
            for record in self.records
            if record.metadata.drillstring_id
        )

        return ids


class DrillstringDataComponent(pydantic.BaseModel):
    family: str
    gamma_sensor_to_bit_distance: Optional[float]
    has_gamma_sensor: Optional[bool] = False


class DrillstringData(pydantic.BaseModel):
    components: List[DrillstringDataComponent]


class Drillstring(pydantic.BaseModel):
    """Needed subset of drillstring response fields"""

    id: str = pydantic.Field(..., alias='_id')
    data: DrillstringData

    @classmethod
    def filter(cls, drillstring: Drillstring) -> Drillstring:
        new_components = [
            component
            for component in copy.deepcopy(drillstring.data.components)
            if component.gamma_sensor_to_bit_distance is not None
            and component.has_gamma_sensor is not None
        ]

        result = drillstring.copy(
            update={'data': DrillstringData(components=new_components)}, deep=True
        )

        return result

    @property
    def mwd_with_gamma_sensor(self) -> Optional[DrillstringDataComponent]:
        """returns MWD component with a gamma sensor"""

        for component in self.data.components:
            if component.family == 'mwd' and component.has_gamma_sensor:
                return component

        return None


class ActualGammaDepthData(pydantic.BaseModel):
    bit_depth: float
    gamma_depth: float
    gamma_ray: int


class ActualGammaDepth(pydantic.BaseModel):
    asset_id: int
    collection: str
    company_id: int
    data: ActualGammaDepthData
    provider: str
    timestamp: int
    version: int
