from typing import List, Optional, Set

import pydantic


class WitsRecordMetadata(pydantic.BaseModel):
    drillstring_id: str = pydantic.Field(..., alias="drillstring")


class WitsRecordData(pydantic.BaseModel):
    bit_depth: float
    gamma_ray: float


class WitsRecord(pydantic.BaseModel):
    asset_id: int
    company_id: int
    timestamp: int
    data: WitsRecordData
    metadata: WitsRecordMetadata


class GammaDepthEvent(pydantic.BaseModel):
    records: pydantic.conlist(WitsRecord, min_items=1)

    @property
    def asset_id(self) -> int:
        # asset id is the same among all records, that's why we fetch from the first one
        return self.records[0].asset_id

    @property
    def drillstring_ids(self) -> Set[str]:
        """Returns unique drillstring ids."""

        ids = set(record.metadata.drillstring_id for record in self.records)

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
        """Returns MWD component with a gamma sensor."""

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
