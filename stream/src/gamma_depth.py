from typing import Dict, List, Optional

import pydantic
from corva import Api, StreamTimeEvent

from src.configuration import SETTINGS
from src.models import (
    ActualGammaDepth,
    ActualGammaDepthData,
    Drillstring,
    GammaDepthEvent,
)


def parse_event(event: StreamTimeEvent) -> Optional[GammaDepthEvent]:
    event = GammaDepthEvent.parse_obj(event)

    new_records = GammaDepthEvent.filter_records(event=event)

    # return early if there are no records left after filtering
    if not new_records:
        return None

    new_event = event.copy(update={'records': new_records}, deep=True)

    return new_event


def gamma_depth(event: StreamTimeEvent, api: Api) -> None:
    event = parse_event(event=event)

    if not event:
        return

    # no exception handling. if request fails, lambda will be reinvoked.
    raw_drillstrings = api.get_dataset(
        provider='corva',
        dataset=SETTINGS.drillstring_collection,
        query={"asset_id": event.asset_id, "_id": {"$in": list(event.drillstring_ids)}},
        sort={"timestamp": 1},
        limit=100,
        fields="_id,data",
    )
    drillstrings = pydantic.parse_obj_as(List[Drillstring], raw_drillstrings)

    id_to_drillstring = {
        drillstring.id: drillstring for drillstring in drillstrings
    }  # type: Dict[str, Drillstring]

    actual_gamma_depths = []
    for record in event.records:  # build actual gamma depth for each record
        gamma_depth_val = record.data.bit_depth

        # The record may be tagged with a drillstring,
        # that gets deleted before the Lambda run.
        # Data about this drillstring won't be received from the api,
        # thus missing from the dict.
        drillstring = id_to_drillstring.get(record.metadata.drillstring_id)

        if drillstring and (mwd_with_gamma_sensor := drillstring.mwd_with_gamma_sensor):
            gamma_depth_val = (
                record.data.bit_depth
                - mwd_with_gamma_sensor.gamma_sensor_to_bit_distance
            )

        actual_gamma_depths.append(
            ActualGammaDepth(
                asset_id=event.asset_id,
                collection=SETTINGS.actual_gamma_depth_collection,
                company_id=event.company_id,
                data=ActualGammaDepthData(
                    gamma_depth=gamma_depth_val,
                    bit_depth=record.data.bit_depth,
                    gamma_ray=record.data.gamma_ray,
                ),
                provider=SETTINGS.provider,
                timestamp=record.timestamp,
                version=SETTINGS.version,
            )
        )

    # if request fails, lambda will be reinvoked. so no exception handling
    api.post(
        f"api/v1/data/{SETTINGS.provider}/{SETTINGS.actual_gamma_depth_collection}/",
        data=[entry.dict() for entry in actual_gamma_depths],
    ).raise_for_status()
