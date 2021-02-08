import json
from types import SimpleNamespace
from typing import List
from urllib.parse import urlencode, urljoin

import pydantic
import pytest
import requests_mock as requests_mock_lib
from corva.configuration import SETTINGS as CORVA_SETTINGS
from pytest_mock import MockerFixture

from lambda_function import lambda_handler
from src.configuration import SETTINGS
from src.gamma_depth import get_drillstrings
from src.models import ActualGammaDepth, ActualGammaDepthData, GammaDepthEvent


@pytest.fixture
def context():
    return SimpleNamespace(client_context=SimpleNamespace(env={'API_KEY': '123'}))


def test_records_with_no_drillstring_are_filtered(mocker: MockerFixture, context):
    """tests, that record with no drillstring is deleted and func returns early, as no records left."""

    event = (
        '[{"records": [{"timestamp": 0, "asset_id": 0, "company_id": 0, "version": 0, "collection": "", '
        '"data": {"bit_depth": 0.0, "gamma_ray": 0}, "metadata": {}}], "metadata": '
        '{"app_stream_id": 0, "apps": {"%s": {"app_connection_id": 0, "app_version": 0}}}, '
        '"asset_id": 0}]'
    ) % CORVA_SETTINGS.APP_KEY

    spy = mocker.spy(GammaDepthEvent, 'filter_records_with_no_drillstring_id')

    lambda_handler(event, context)

    assert len(spy.spy_return.records) == 0


@pytest.mark.parametrize(
    'event,text,expected',
    [
        (
            '[{"records": [{"timestamp": 0, "asset_id": 0, "company_id": 0, "version": 0, "collection": "", '
            '"data": {"bit_depth": 1.0, "gamma_ray": 2}, "metadata": {"drillstring": "0"}}], '
            '"metadata": {"app_stream_id": 0, "apps": {"%s": {"app_connection_id": 0, "app_version": 0}}}, '
            '"asset_id": 0}]' % CORVA_SETTINGS.APP_KEY,
            '[{"_id": "0", "data": {"components": []}}]',
            ActualGammaDepth(
                asset_id=0,
                collection=SETTINGS.actual_gamma_depth_collection,
                company_id=0,
                data=ActualGammaDepthData(gamma_depth=1.0, bit_depth=1.0, gamma_ray=2),
                provider=SETTINGS.provider,
                timestamp=0,
                version=SETTINGS.version,
            ),
        ),
        (
            '[{"records": [{"timestamp": 0, "asset_id": 0, "company_id": 0, "version": 0, "collection": "", '
            '"data": {"bit_depth": 1.0, "gamma_ray": 2}, "metadata": {"drillstring": "0"}}], "metadata": '
            '{"app_stream_id": 0, "apps": {"%s": {"app_connection_id": 0, "app_version": 0}}},'
            ' "asset_id": 0}]' % CORVA_SETTINGS.APP_KEY,
            '[{"_id": "0", "data": {"components": [{"family": "mwd", "has_gamma_sensor": true, '
            '"gamma_sensor_to_bit_distance": 1.0}]}}]',
            ActualGammaDepth(
                asset_id=0,
                collection=SETTINGS.actual_gamma_depth_collection,
                company_id=0,
                data=ActualGammaDepthData(gamma_depth=0.0, bit_depth=1.0, gamma_ray=2),
                provider=SETTINGS.provider,
                timestamp=0,
                version=SETTINGS.version,
            ),
        ),
        (
            '[{"records": [{"timestamp": 0, "asset_id": 0, "company_id": 0, "version": 0, "collection": "", '
            '"data": {"bit_depth": 1.0, "gamma_ray": 2}, "metadata": {"drillstring": "0"}}], '
            '"metadata": {"app_stream_id": 0, "apps": {"%s": {"app_connection_id": 0, "app_version": 0}}}, '
            '"asset_id": 0}]' % CORVA_SETTINGS.APP_KEY,
            '[]',
            ActualGammaDepth(
                asset_id=0,
                collection=SETTINGS.actual_gamma_depth_collection,
                company_id=0,
                data=ActualGammaDepthData(gamma_depth=1.0, bit_depth=1.0, gamma_ray=2),
                provider=SETTINGS.provider,
                timestamp=0,
                version=SETTINGS.version,
            ),
        ),
    ],
    ids=[
        'no_mwd_with_gamma_sensor',
        'mwd_with_gamma_sensor',
        'missing_drillstring_data_from_api',
    ],
)
def test_gamma_depth(
    event, text, expected, requests_mock: requests_mock_lib.Mocker, context
):
    get_mock = requests_mock.get(
        urljoin(
            CORVA_SETTINGS.DATA_API_ROOT_URL,
            'api/v1/data/corva/%s?%s'
            % (
                SETTINGS.drillstring_collection,
                urlencode(
                    {
                        'query': '{"asset_id": 0, "_id": {"$in": ["0"]}}',
                        'sort': '{"timestamp": 1}',
                        'limit': 100,
                        'skip': 0,
                        'fields': '_id,data',
                    }
                ),
            ),
        ),
        text=text,
    )

    post_mock = requests_mock.post(
        urljoin(
            CORVA_SETTINGS.DATA_API_ROOT_URL,
            f'api/v1/data/{SETTINGS.provider}/{SETTINGS.actual_gamma_depth_collection}',
        )
    )

    lambda_handler(event, context)

    assert get_mock.called
    assert post_mock.called_once

    actual_gamma_depths = pydantic.parse_obj_as(
        List[ActualGammaDepth], post_mock.last_request.json()
    )  # type: List[ActualGammaDepth]

    assert actual_gamma_depths[0] == expected


def test_get_drillstrings_gathers_all_data(
    mocker: MockerFixture, requests_mock: requests_mock_lib.Mocker, context
):
    """tests, that all drillstrings received from the api, in case there is more drillstrings, than api limit"""

    event = (
        '[{"records": [{"timestamp": 0, "asset_id": 0, "company_id": 0, "version": 0, "collection": "", '
        '"data": {"bit_depth": 0.0, "gamma_ray": 0}, "metadata": {"drillstring": 0}}, {"timestamp": 0, '
        '"asset_id": 0, "company_id": 0, "version": 0, "collection": "", "data": {"bit_depth": 0.0, '
        '"gamma_ray": 0}, "metadata": {"drillstring": 1}}], "metadata": '
        '{"app_stream_id": 0, "apps": {"%s": {"app_connection_id": 0, "app_version": 0}}}, '
        '"asset_id": 0}]'
    ) % CORVA_SETTINGS.APP_KEY

    mocker.patch(
        'src.gamma_depth.get_drillstrings',
        lambda *args, **kwargs: get_drillstrings(
            *args, **{**kwargs, 'limit': 1}
        ),  # override limit
    )

    for skip, text in [
        (0, '[{"_id": "0", "data": {"components": []}}]'),
        (1, '[{"_id": "1", "data": {"components": []}}]'),
        (2, '[]'),
    ]:
        for ids in [["0", "1"], ["1", "0"]]:  # ids can be in any order
            requests_mock.get(
                urljoin(
                    CORVA_SETTINGS.DATA_API_ROOT_URL,
                    'api/v1/data/corva/%s?%s'
                    % (
                        SETTINGS.drillstring_collection,
                        urlencode(
                            {
                                'query': '{"asset_id": 0, "_id": {"$in": %s}}'
                                % json.dumps(ids),
                                'sort': '{"timestamp": 1}',
                                'limit': 1,
                                'skip': skip,
                                'fields': '_id,data',
                            }
                        ),
                    ),
                ),
                text=text,
            )

    post_mock = requests_mock.post(requests_mock_lib.ANY)

    lambda_handler(event, context)

    assert post_mock.called_once
    assert len(post_mock.last_request.json()) == 2
