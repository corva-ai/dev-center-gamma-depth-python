import itertools
import json
from typing import Dict, List, Optional

import pydantic
from corva import Api, Cache, StreamEvent

from src.configuration import SETTINGS
from src.models import (
    ActualGammaDepth,
    ActualGammaDepthData,
    Drillstring,
    GammaDepthEvent,
)


def get_drillstrings(
    asset_id: int, ids: List[str], api: Api, limit: int
) -> List[Drillstring]:
    """gathers all drillstring data from the api"""

    all_drillstrings = []

    for skip in itertools.count(0, limit):
        response = api.get(
            f"api/v1/data/corva/{SETTINGS.drillstring_collection}/",
            params={
                "query": json.dumps({"asset_id": asset_id, "_id": {"$in": ids}}),
                "sort": '{"timestamp": 1}',
                "limit": limit,
                "skip": skip,
                "fields": "_id,data",
            },
        )
        response.raise_for_status()

        drillstrings = pydantic.parse_obj_as(List[Drillstring], response.json())

        all_drillstrings.extend(drillstrings)

        if len(drillstrings) != limit:
            break

    return all_drillstrings


def parse_event(event: StreamEvent) -> Optional[GammaDepthEvent]:
    event = GammaDepthEvent.parse_obj(event)

    new_records = GammaDepthEvent.filter_records(event=event)

    # return early if there are no records left after filtering
    if not new_records:
        return None

    event.records = new_records

    return event


def gamma_depth(event: StreamEvent, api: Api, cache: Cache) -> None:
    event = parse_event(event=event)

    if not event:
        return

    # if request fails, lambda will be reinvoked. so no exception handling
    drillstrings = get_drillstrings(
        asset_id=event.asset_id, ids=list(event.drillstring_ids), api=api, limit=100
    )

    # filter drillstrings
    drillstrings = [Drillstring.filter(drillstring) for drillstring in drillstrings]

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

    # if request fails, lambda will be reinvoked. so no exception handling
    api.post(
        f"api/v1/data/{SETTINGS.provider}/{SETTINGS.actual_gamma_depth_collection}/",
        data=[entry.dict() for entry in actual_gamma_depths],
    ).raise_for_status()
