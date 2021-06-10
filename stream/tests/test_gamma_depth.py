import contextlib
from typing import List

import pytest
import requests_mock as requests_mock_lib
from corva import Api, StreamTimeEvent, StreamTimeRecord
from pytest_mock import MockerFixture
from requests import HTTPError

from lambda_function import lambda_handler
from src.configuration import SETTINGS
from src.models import (
    ActualGammaDepth,
    ActualGammaDepthData,
    Drillstring,
    DrillstringData,
    DrillstringDataComponent,
    WitsRecordData,
    WitsRecordMetadata,
)


@pytest.mark.parametrize(
    "metadata,exc_ctx",
    [
        (
            {},
            contextlib.nullcontext(),
        ),
        (
            {"drillstring": "5"},
            pytest.raises(
                Exception, match=r'^test_return_early_if_no_records_after_filtering$'
            ),
        ),
    ],
    ids=["returns early", "doesnt return early"],
)
def test_return_early_if_no_records_after_filtering(
    metadata,
    exc_ctx,
    mocker: MockerFixture,
    app_runner,
):
    """Records with no drillstring get deleted and app returns early."""
    event = StreamTimeEvent(
        asset_id=0,
        company_id=1,
        records=[
            StreamTimeRecord(
                timestamp=2,
                data=WitsRecordData(bit_depth=3, gamma_ray=4).dict(),
                metadata=metadata,
            )
        ],
    )

    mocker.patch.object(
        Api,
        'get_dataset',
        side_effect=Exception('test_return_early_if_no_records_after_filtering'),
    )

    with exc_ctx:
        app_runner(lambda_handler, event)


@pytest.mark.parametrize('family', ('random', 'mwd'))
@pytest.mark.parametrize('has_gamma_sensor', (None, True, False))
@pytest.mark.parametrize('gamma_sensor_to_bit_distance', (None, 1.0))
def test_gamma_depth_1(
    family,
    has_gamma_sensor,
    gamma_sensor_to_bit_distance,
    mocker: MockerFixture,
    app_runner,
):
    event = StreamTimeEvent(
        asset_id=0,
        company_id=1,
        records=[
            StreamTimeRecord(
                timestamp=2,
                data=WitsRecordData(bit_depth=3, gamma_ray=4).dict(),
                metadata=WitsRecordMetadata(drillstring='5').dict(by_alias=True),
            )
        ],
    )

    drillstring = Drillstring(
        _id='5',
        data=DrillstringData(
            components=[
                DrillstringDataComponent(
                    family=family,
                    has_gamma_sensor=has_gamma_sensor,
                    gamma_sensor_to_bit_distance=gamma_sensor_to_bit_distance,
                )
            ]
        ),
    )

    expected_gamma_depth = 2.0 if drillstring.mwd_with_gamma_sensor else 3.0

    _test_gamma_depth(
        event=event,
        drillstrings=[drillstring.dict(by_alias=True)],
        expected_gamma_depth=expected_gamma_depth,
        mocker=mocker,
        app_runner=app_runner,
    )


@pytest.mark.parametrize(
    'drillstrings,mwd_with_gamma_sensor',
    (
        ([], False),
        ([{"_id": '5', "data": {"components": []}}], False),
        (
            [
                {
                    "_id": '5',
                    "data": {
                        "components": [
                            {
                                "family": "mwd",
                                "has_gamma_sensor": True,
                                "gamma_sensor_to_bit_distance": 1.0,
                            }
                        ]
                    },
                }
            ],
            True,
        ),
    ),
    ids=(
        "drillstring data was not received from api",
        "drillstring has empty components",
        "drillstring has mwd with gamma sensor",
    ),
)
def test_gamma_depth_2(
    drillstrings, mwd_with_gamma_sensor, mocker: MockerFixture, app_runner
):
    event = StreamTimeEvent(
        asset_id=0,
        company_id=1,
        records=[
            StreamTimeRecord(
                timestamp=2,
                data=WitsRecordData(bit_depth=3, gamma_ray=4).dict(),
                metadata=WitsRecordMetadata(drillstring='5').dict(by_alias=True),
            )
        ],
    )

    expected_gamma_depth = 2.0 if mwd_with_gamma_sensor else 3.0

    _test_gamma_depth(
        event=event,
        drillstrings=drillstrings,
        expected_gamma_depth=expected_gamma_depth,
        mocker=mocker,
        app_runner=app_runner,
    )


def _test_gamma_depth(
    event: StreamTimeEvent,
    drillstrings: List[dict],
    expected_gamma_depth: float,
    mocker: MockerFixture,
    app_runner,
):
    mocker.patch.object(
        Api,
        'get_dataset',
        return_value=drillstrings,
    )
    post_mock = mocker.patch.object(Api, 'post')

    app_runner(lambda_handler, event)

    assert post_mock.call_args.kwargs['data'] == [
        ActualGammaDepth(
            asset_id=event.asset_id,
            collection=SETTINGS.actual_gamma_depth_collection,
            company_id=event.company_id,
            data=ActualGammaDepthData(
                gamma_depth=expected_gamma_depth,
                bit_depth=event.records[0].data['bit_depth'],
                gamma_ray=event.records[0].data['gamma_ray'],
            ),
            provider=SETTINGS.provider,
            timestamp=event.records[0].timestamp,
            version=SETTINGS.version,
        ).dict()
    ]


@pytest.mark.parametrize(
    "exc_ctx,status_code",
    [(pytest.raises(HTTPError), 404), (contextlib.nullcontext(), 200)],
    ids=["must raise", "must pass"],
)
def test_fail_if_couldnt_post(
    exc_ctx,
    status_code,
    mocker: MockerFixture,
    requests_mock: requests_mock_lib.Mocker,
    app_runner,
):
    event = StreamTimeEvent(
        asset_id=0,
        company_id=1,
        records=[
            StreamTimeRecord(
                timestamp=2,
                data=WitsRecordData(bit_depth=3, gamma_ray=4).dict(),
                metadata=WitsRecordMetadata(drillstring='5').dict(by_alias=True),
            )
        ],
    )

    mocker.patch.object(
        Api,
        'get_dataset',
        return_value=[],
    )
    post_mock = requests_mock.post(requests_mock_lib.ANY, status_code=status_code)

    with exc_ctx:
        app_runner(lambda_handler, event)

    assert post_mock.called_once
