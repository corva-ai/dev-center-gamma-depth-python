from typing import Dict, List

import pydantic
from corva import Api, Cache, ScheduledEvent

from src.configuration import SETTINGS
from src.models import (
    ActualGammaDepth,
    ActualGammaDepthData,
    Drillstring,
    GammaDepthEvent,
    WitsRecord,
)


def gamma_depth(event: ScheduledEvent, api: Api, cache: Cache) -> None:
    # no exception handling. if request fails, lambda will be reinvoked.
    records = pydantic.parse_obj_as(
        List[WitsRecord],
        api.get_dataset(
            provider='corva',
            dataset=SETTINGS.wits_collection,
            query={
                'asset_id': event.asset_id,
                'timestamp': {
                    '$gt': event.schedule_start - event.interval,
                    '$lte': event.schedule_start,
                },
                'metadata.drillstring': {'$exist': True, '$ne': None},
            },
            sort={'timestamp': 1},
            limit=1000,
        ),
    )

    if not records:
        # return early if no records received
        return

    event = GammaDepthEvent(records=records)

    # no exception handling. if request fails, lambda will be reinvoked.
    drillstrings = pydantic.parse_obj_as(
        List[Drillstring],
        api.get_dataset(
            provider='corva',
            dataset=SETTINGS.drillstring_collection,
            query={
                'asset_id': event.asset_id,
                '_id': {'$in': list(event.drillstring_ids)},
            },
            sort={'timestamp': 1},
            limit=100,
        ),
    )

    id_to_drillstring = {
        drillstring.id: drillstring for drillstring in drillstrings
    }  # type: Dict[str, Drillstring]

    actual_gamma_depths = []
    for record in event.records:  # build actual gamma depth for each record
        gamma_depth_val = record.data.bit_depth

        # the record may be tagged with a drillstring, that gets deleted before the Lambda run.
        # data about this drillstring won't be received from the api, thus missing from the dict
        if (
            drillstring := id_to_drillstring.get(record.metadata.drillstring_id)
        ) and drillstring.mwd_with_gamma_sensor:
            gamma_depth_val = (
                record.data.bit_depth
                - drillstring.mwd_with_gamma_sensor.gamma_sensor_to_bit_distance
            )

        actual_gamma_depths.append(
            ActualGammaDepth(
                asset_id=record.asset_id,
                collection=SETTINGS.actual_gamma_depth_collection,
                company_id=record.company_id,
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

    # no exception handling. if request fails, lambda will be reinvoked.
    api.post(
        f"api/v1/data/{SETTINGS.provider}/{SETTINGS.actual_gamma_depth_collection}/",
        data=[entry.dict() for entry in actual_gamma_depths],
    ).raise_for_status()