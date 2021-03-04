from datetime import datetime, timezone

import pytest
import requests
from corva import Api
from pytest_mock import MockerFixture

from lambda_function import lambda_handler
from src.configuration import SETTINGS
from src.models import ActualGammaDepth, ActualGammaDepthData, GammaDepthEvent


@pytest.fixture
def event():
    return {
        "schedule": 0,
        "interval": 1,
        "schedule_start": datetime(
            year=2021, month=1, day=1, second=1, tzinfo=timezone.utc
        ).timestamp()
        * 1000,
        "asset_id": 0,
    }


@pytest.fixture
def wits_record():
    return {
        'asset_id': int(),
        'company_id': int(),
        'timestamp': int(),
        'data': {'bit_depth': float(), 'gamma_ray': float()},
        'metadata': {'drillstring': str()},
    }


@pytest.mark.parametrize(
    'raises',
    (True, False),
)
def test_fail_if_records_fetch_raised(
    raises, mocker: MockerFixture, corva_context, event, wits_record
):
    mock_event = mocker.patch.object(
        GammaDepthEvent, '__init__', side_effect=Exception
    )  # raise to return early

    if raises:
        mock_api = mocker.patch.object(
            Api, 'get_dataset', side_effect=requests.HTTPError
        )

        pytest.raises(requests.HTTPError, lambda_handler, event, corva_context)

        mock_api.assert_called_once()
        mock_event.assert_not_called()

        return

    mock_api = mocker.patch.object(
        Api,
        'get_dataset',
        return_value=[wits_record],
    )

    pytest.raises(Exception, lambda_handler, event, corva_context)

    mock_api.assert_called_once()
    mock_event.assert_called_once()


@pytest.mark.parametrize('early', (True, False))
def test_early_return_if_no_records_fetched(
    early, mocker: MockerFixture, corva_context, event, wits_record
):
    mock_event = mocker.patch.object(
        GammaDepthEvent, '__init__', side_effect=Exception
    )  # raise to return early

    if early:
        mocker.patch.object(Api, 'get_dataset', return_value=[])
        lambda_handler(event, corva_context)
        mock_event.assert_not_called()
        return

    mocker.patch.object(Api, 'get_dataset', return_value=[wits_record])
    pytest.raises(Exception, lambda_handler, event, corva_context)
    mock_event.assert_called_once()


@pytest.mark.parametrize(
    'raises,side_effect',
    ([True, Exception], [False, [{'_id': '', 'data': {'components': []}}]]),
)
def test_fail_if_drillstrings_fetch_raised(
    raises, side_effect, mocker: MockerFixture, corva_context, event, wits_record
):
    mock_api = mocker.patch.object(
        Api, 'get_dataset', side_effect=[[wits_record], side_effect]
    )
    mock_post = mocker.patch.object(Api, 'post')

    if raises:
        pytest.raises(side_effect, lambda_handler, event, corva_context)
        assert mock_api.call_count == 2
        mock_post.assert_not_called()
        return

    lambda_handler(event, corva_context)
    assert mock_api.call_count == 2
    mock_post.assert_called_once()


@pytest.mark.parametrize('family', ('random', 'mwd'))
@pytest.mark.parametrize('has_gamma_sensor', (None, True, False))
@pytest.mark.parametrize('gamma_sensor_to_bit_distance', (None, 1.0))
def test_gamma_depth_1(
    family,
    has_gamma_sensor,
    gamma_sensor_to_bit_distance,
    mocker: MockerFixture,
    corva_context,
    event,
    wits_record,
):
    drillstrings = [
        {
            '_id': str(),
            'data': {
                'components': [
                    {
                        'family': family,
                        'has_gamma_sensor': has_gamma_sensor,
                        'gamma_sensor_to_bit_distance': gamma_sensor_to_bit_distance,
                    }
                ]
            },
        }
    ]
    expected_gamma_depth = (
        -1.0
        if family == 'mwd'
        and has_gamma_sensor
        and gamma_sensor_to_bit_distance is not None
        else 0.0
    )

    _test_gamma_depth(
        drillstrings=drillstrings,
        expected_gamma_depth=expected_gamma_depth,
        mocker=mocker,
        corva_context=corva_context,
        event=event,
        wits_record=wits_record,
    )


@pytest.mark.parametrize(
    'drillstrings,expected_gamma_depth',
    (
        ([], 0.0),
        ([{"_id": str(), "data": {"components": []}}], 0.0),
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
            -1.0,
        ),
    ),
    ids=(
        "drillstring data was not received from api",
        "drillstring has empty components",
        "drillstring has mwd with gamma sensor",
    ),
)
def test_gamma_depth_2(
    drillstrings,
    expected_gamma_depth,
    mocker: MockerFixture,
    corva_context,
    event,
    wits_record,
):
    _test_gamma_depth(
        drillstrings=drillstrings,
        expected_gamma_depth=expected_gamma_depth,
        mocker=mocker,
        corva_context=corva_context,
        event=event,
        wits_record=wits_record,
    )


def _test_gamma_depth(
    drillstrings,
    expected_gamma_depth,
    mocker: MockerFixture,
    corva_context,
    event,
    wits_record,
):
    mocker.patch.object(
        Api,
        'get_dataset',
        side_effect=[[wits_record], drillstrings],
    )
    post_mock = mocker.patch.object(Api, 'post')

    lambda_handler(event, corva_context)

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


@pytest.mark.parametrize('raises,status_code', ([True, 400], (False, 200)))
def test_fail_if_post_unsuccessful(
    raises,
    status_code,
    mocker: MockerFixture,
    requests_mock,
    corva_context,
    event,
    wits_record,
):

    mocker.patch.object(
        Api,
        'get_dataset',
        side_effect=[[wits_record], []],
    )
    post_mock = requests_mock.post(
        f"/api/v1/data/{SETTINGS.provider}/{SETTINGS.actual_gamma_depth_collection}/",
        status_code=status_code,
    )

    (
        pytest.raises(Exception, lambda_handler, event, corva_context)
        if raises
        else lambda_handler(event, corva_context)
    )

    assert post_mock.called_once
