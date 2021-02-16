from __future__ import annotations

import copy
from typing import List, Optional, Set

import pydantic


class WitsRecordMetadata(pydantic.BaseModel):
    drillstring_id: Optional[str] = pydantic.Field(None, alias="drillstring")


class WitsRecordData(pydantic.BaseModel):
    bit_depth: float
    gamma_ray: float


class WitsRecord(pydantic.BaseModel):
    asset_id: int
    company_id: int
    timestamp: int
    data: WitsRecordData
    metadata: Optional[WitsRecordMetadata] = None


class GammaDepthEvent(pydantic.BaseModel):
    records: pydantic.conlist(WitsRecord, min_items=1)

    @property
    def asset_id(self) -> int:
        return self.records[0].asset_id

    @staticmethod
    def filter_records(event: GammaDepthEvent) -> List[WitsRecord]:
        """filters records with no drillstring_id"""

        new_records = [
            copy.deepcopy(record)
            for record in event.records
            if record.metadata and record.metadata.drillstring_id
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


class DrillstringData(pydantic.BaseModel):
    components: List[DrillstringDataComponent]


class Drillstring(pydantic.BaseModel):
    """Needed subset of drillstring response fields"""

    id: str = pydantic.Field(..., alias="_id")
    data: DrillstringData

    @classmethod
    def filter(cls, drillstring: Drillstring) -> Drillstring:
        new_components = [
            copy.deepcopy(component)
            for component in drillstring.data.components
            if component.gamma_sensor_to_bit_distance is not None
            and component.has_gamma_sensor is not None
            and component.family == "mwd"
        ]

        result = drillstring.copy(
            update={"data": DrillstringData(components=new_components)}, deep=True
        )

        return result

    @property
    def mwd_with_gamma_sensor(self) -> Optional[DrillstringDataComponent]:
        """returns MWD component with a gamma sensor"""

        for component in self.data.components:
            if component.family == "mwd" and component.has_gamma_sensor:
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
