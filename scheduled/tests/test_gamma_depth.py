import contextlib

import pytest
from corva import Api, ScheduledEvent
from pytest_mock import MockerFixture
from requests_mock import Mocker as RequestsMocker

from src.configuration import SETTINGS
from src.gamma_depth import gamma_depth
from src.models import (
    ActualGammaDepth,
    ActualGammaDepthData,
    Drillstring,
    DrillstringData,
    DrillstringDataComponent,
    GammaDepthEvent,
    WitsRecord,
    WitsRecordData,
    WitsRecordMetadata,
)


@pytest.mark.parametrize(
    'records,exc_ctx',
    [
        ([], contextlib.nullcontext()),
        (
            [
                WitsRecord(
                    asset_id=0,
                    company_id=1,
                    timestamp=2,
                    data=WitsRecordData(bit_depth=3.0, gamma_ray=4.0),
                    metadata=WitsRecordMetadata(drillstring=''),
                ).dict(by_alias=True)
            ],
            pytest.raises(
                Exception, match=r'^test_early_return_if_no_records_fetched$'
            ),
        ),
    ],
)
def test_early_return_if_no_records_fetched(
    records, exc_ctx, mocker: MockerFixture, app_runner
):
    event = ScheduledEvent(asset_id=0, company_id=1, start_time=2, end_time=3)

    mocker.patch.object(
        GammaDepthEvent,
        '__init__',
        side_effect=Exception('test_early_return_if_no_records_fetched'),
    )  # raise to return early
    mocker.patch.object(Api, 'get_dataset', return_value=records)

    with exc_ctx:
        app_runner(gamma_depth, event)


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
    wits_record = WitsRecord(
        asset_id=0,
        company_id=1,
        timestamp=2,
        data=WitsRecordData(bit_depth=3.0, gamma_ray=4.0),
        metadata=WitsRecordMetadata(drillstring=''),
    )

    drillstring = Drillstring(
        _id='',
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
        drillstrings=[drillstring.dict(by_alias=True)],
        expected_gamma_depth=expected_gamma_depth,
        mocker=mocker,
        app_runner=app_runner,
        wits_record=wits_record.dict(by_alias=True),
    )


@pytest.mark.parametrize(
    'drillstrings,mwd_with_gamma_sensor',
    (
        ([], False),
        ([{"_id": str(), "data": {"components": []}}], False),
        (
            [
                {
                    "_id": str(),
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
    wits_record = WitsRecord(
        asset_id=0,
        company_id=1,
        timestamp=2,
        data=WitsRecordData(bit_depth=3.0, gamma_ray=4.0),
        metadata=WitsRecordMetadata(drillstring=''),
    )

    expected_gamma_depth = 2.0 if mwd_with_gamma_sensor else 3.0

    _test_gamma_depth(
        drillstrings=drillstrings,
        expected_gamma_depth=expected_gamma_depth,
        mocker=mocker,
        app_runner=app_runner,
        wits_record=wits_record.dict(by_alias=True),
    )


def _test_gamma_depth(
    drillstrings,
    expected_gamma_depth,
    mocker: MockerFixture,
    app_runner,
    wits_record,
):
    event = ScheduledEvent(asset_id=0, company_id=1, start_time=2, end_time=3)

    mocker.patch.object(
        Api,
        'get_dataset',
        side_effect=[[wits_record], drillstrings],
    )
    post_mock = mocker.patch.object(Api, 'post')

    app_runner(gamma_depth, event)

    assert post_mock.call_args.kwargs['data'] == [
        ActualGammaDepth(
            asset_id=wits_record['asset_id'],
            collection=SETTINGS.actual_gamma_depth_collection,
            company_id=wits_record['company_id'],
            data=ActualGammaDepthData(
                gamma_depth=expected_gamma_depth,
                bit_depth=wits_record['data']['bit_depth'],
                gamma_ray=wits_record['data']['gamma_ray'],
            ),
            provider=SETTINGS.provider,
            timestamp=wits_record['timestamp'],
            version=SETTINGS.version,
        ).dict()
    ]


@pytest.mark.parametrize(
    'exc_ctx,status_code',
    ([pytest.raises(Exception), 400], (contextlib.nullcontext(), 200)),
)
def test_fail_if_post_unsuccessful(
    exc_ctx,
    status_code,
    mocker: MockerFixture,
    requests_mock: RequestsMocker,
    app_runner,
):
    event = ScheduledEvent(asset_id=0, company_id=1, start_time=2, end_time=3)
    wits_record = WitsRecord(
        asset_id=0,
        company_id=1,
        timestamp=2,
        data=WitsRecordData(bit_depth=3.0, gamma_ray=4.0),
        metadata=WitsRecordMetadata(drillstring=''),
    ).dict(by_alias=True)

    mocker.patch.object(
        Api,
        'get_dataset',
        side_effect=[[wits_record], []],
    )
    post_mock = requests_mock.post(
        f"/api/v1/data/{SETTINGS.provider}/{SETTINGS.actual_gamma_depth_collection}/",
        status_code=status_code,
    )

    with exc_ctx:
        app_runner(gamma_depth, event)

    assert post_mock.called_once
