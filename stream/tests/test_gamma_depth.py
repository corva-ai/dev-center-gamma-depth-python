import json
from typing import List
from urllib.parse import urlencode, urljoin

import pydantic
import pytest
import requests_mock as requests_mock_lib
from corva.configuration import SETTINGS as CORVA_SETTINGS
from pytest_mock import MockerFixture
from requests import HTTPError

from lambda_function import lambda_handler
from src.configuration import SETTINGS
from src.gamma_depth import get_drillstrings
from src.models import ActualGammaDepth, ActualGammaDepthData, GammaDepthEvent


@pytest.mark.parametrize(
    "metadata,is_early",
    [(None, True), ({}, True), ({"drillstring": "0"}, False)],
    ids=["returns early", "returns early", "doesnt return early"],
)
def test_return_early_if_no_records_after_filtering(
    metadata,
    is_early,
    mocker: MockerFixture,
    requests_mock: requests_mock_lib.Mocker,
    corva_context,
):
    """tests, that records with no drillstring get deleted and app returns early, as no records left."""

    event = [
        {
            "records": [
                {
                    "timestamp": 0,
                    "asset_id": 0,
                    "company_id": 0,
                    "data": {"bit_depth": 0.0, "gamma_ray": 0},
                    "metadata": metadata,
                }
            ],
            "metadata": {
                "app_stream_id": 0,
                "apps": {CORVA_SETTINGS.APP_KEY: {"app_connection_id": 0}},
            },
        }
    ]

    spy_filter_records = mocker.spy(GammaDepthEvent, "filter_records")
    patch_get_drillstrings = mocker.patch(
        "src.gamma_depth.get_drillstrings", return_value=[]
    )
    patch_api_post = requests_mock.post(requests_mock_lib.ANY)

    lambda_handler(event, corva_context)

    if is_early:
        assert len(spy_filter_records.spy_return) == 0
        patch_get_drillstrings.assert_not_called()
        return

    assert patch_api_post.called_once


@pytest.mark.parametrize(
    "raises,status_code", [(True, 404), (False, 200)], ids=["must raise", "must pass"]
)
def test_fail_if_couldnt_receive_drillstrings(
    raises,
    status_code,
    requests_mock: requests_mock_lib.Mocker,
    corva_context,
):
    event = [
        {
            "records": [
                {
                    "timestamp": 0,
                    "asset_id": 0,
                    "company_id": 0,
                    "data": {"bit_depth": 0.0, "gamma_ray": 0},
                    "metadata": {"drillstring": "0"},
                }
            ],
            "metadata": {
                "app_stream_id": 0,
                "apps": {CORVA_SETTINGS.APP_KEY: {"app_connection_id": 0}},
            },
        }
    ]

    requests_mock.get(requests_mock_lib.ANY, status_code=status_code, text="[]")
    patch_api_post = requests_mock.post(requests_mock_lib.ANY)

    if raises:
        pytest.raises(HTTPError, lambda_handler, event, corva_context)
        return

    lambda_handler(event, corva_context)

    assert patch_api_post.called_once


def test_get_drillstrings_gathers_all_data(
    mocker: MockerFixture, requests_mock: requests_mock_lib.Mocker, corva_context
):
    """tests, that all drillstrings received from the api, in case there is more drillstrings, than api limit"""

    event = [
        {
            "records": [
                {
                    "timestamp": 0,
                    "asset_id": 0,
                    "company_id": 0,
                    "data": {"bit_depth": 0.0, "gamma_ray": 0},
                    "metadata": {"drillstring": 0},
                },
                {
                    "timestamp": 0,
                    "asset_id": 0,
                    "company_id": 0,
                    "data": {"bit_depth": 0.0, "gamma_ray": 0},
                    "metadata": {"drillstring": 1},
                },
            ],
            "metadata": {
                "app_stream_id": 0,
                "apps": {CORVA_SETTINGS.APP_KEY: {"app_connection_id": 0}},
            },
        }
    ]

    mocker.patch(
        "src.gamma_depth.get_drillstrings",
        lambda *args, **kwargs: get_drillstrings(
            *args, **dict(kwargs, limit=1)
        ),  # override limit
    )

    get_mock = requests_mock.get(
        requests_mock_lib.ANY,
        [
            {"text": '[{"_id": "0", "data": {"components": []}}]'},
            {"text": '[{"_id": "1", "data": {"components": []}}]'},
            {"text": "[]"},
        ],
    )

    post_mock = requests_mock.post(requests_mock_lib.ANY)

    lambda_handler(event, corva_context)

    assert get_mock.call_count == 3
    for skip, req in enumerate(get_mock.request_history):
        assert req.qs["skip"] == [f"{skip}"]
    assert post_mock.called_once
    assert len(post_mock.last_request.json()) == 2


@pytest.mark.parametrize(
    "text,expected_gamma_depth",
    [
        (
            json.dumps([{"_id": "0", "data": {"components": []}}]),
            1.0,
        ),
        (
            json.dumps(
                [
                    {
                        "_id": "0",
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
                ]
            ),
            0.0,
        ),
        (
            json.dumps([]),
            1.0,
        ),
        (
            json.dumps(
                [
                    {
                        "_id": "0",
                        "data": {
                            "components": [
                                {
                                    "family": "random",
                                    "has_gamma_sensor": True,
                                    "gamma_sensor_to_bit_distance": 1.0,
                                }
                            ]
                        },
                    }
                ]
            ),
            1.0,
        ),
    ],
    ids=[
        "no_mwd_with_gamma_sensor",
        "mwd_with_gamma_sensor",
        "missing_drillstring_data_from_api",
        "family not mwd",
    ],
)
def test_gamma_depth(
    text, expected_gamma_depth, requests_mock: requests_mock_lib.Mocker, corva_context
):
    event = [
        {
            "records": [
                {
                    "timestamp": 0,
                    "asset_id": 0,
                    "company_id": 0,
                    "data": {"bit_depth": 1.0, "gamma_ray": 2},
                    "metadata": {"drillstring": "0"},
                }
            ],
            "metadata": {
                "app_stream_id": 0,
                "apps": {
                    CORVA_SETTINGS.APP_KEY: {"app_connection_id": 0, "app_version": 0}
                },
            },
        }
    ]
    expected = ActualGammaDepth(
        asset_id=0,
        collection=SETTINGS.actual_gamma_depth_collection,
        company_id=0,
        data=ActualGammaDepthData(
            gamma_depth=expected_gamma_depth, bit_depth=1.0, gamma_ray=2
        ),
        provider=SETTINGS.provider,
        timestamp=0,
        version=SETTINGS.version,
    )

    get_mock = requests_mock.get(
        urljoin(
            CORVA_SETTINGS.DATA_API_ROOT_URL,
            "api/v1/data/corva/%s/?%s"
            % (
                SETTINGS.drillstring_collection,
                urlencode(
                    {
                        "query": '{"asset_id": 0, "_id": {"$in": ["0"]}}',
                        "sort": '{"timestamp": 1}',
                        "limit": 100,
                        "skip": 0,
                        "fields": "_id,data",
                    }
                ),
            ),
        ),
        text=text,
    )

    post_mock = requests_mock.post(
        urljoin(
            CORVA_SETTINGS.DATA_API_ROOT_URL,
            f"api/v1/data/{SETTINGS.provider}/{SETTINGS.actual_gamma_depth_collection}/",
        )
    )

    lambda_handler(event, corva_context)

    assert get_mock.called
    assert post_mock.called_once

    actual_gamma_depths = pydantic.parse_obj_as(
        List[ActualGammaDepth], post_mock.last_request.json()
    )  # type: List[ActualGammaDepth]

    assert actual_gamma_depths[0] == expected


@pytest.mark.parametrize(
    "components,expected_gamma_depth",
    [
        (
            [
                {
                    "family": "random",
                    "has_gamma_sensor": None,
                    "gamma_sensor_to_bit_distance": None,
                }
            ],
            1.0,
        ),
        (
            [
                {
                    "family": "random",
                    "has_gamma_sensor": None,
                    "gamma_sensor_to_bit_distance": 1.0,
                }
            ],
            1.0,
        ),
        (
            [
                {
                    "family": "random",
                    "has_gamma_sensor": True,
                    "gamma_sensor_to_bit_distance": None,
                }
            ],
            1.0,
        ),
        (
            [
                {
                    "family": "random",
                    "has_gamma_sensor": True,
                    "gamma_sensor_to_bit_distance": 1.0,
                }
            ],
            1.0,
        ),
        (
            [
                {
                    "family": "mwd",
                    "has_gamma_sensor": None,
                    "gamma_sensor_to_bit_distance": None,
                }
            ],
            1.0,
        ),
        (
            [
                {
                    "family": "mwd",
                    "has_gamma_sensor": None,
                    "gamma_sensor_to_bit_distance": 1.0,
                }
            ],
            1.0,
        ),
        (
            [
                {
                    "family": "mwd",
                    "has_gamma_sensor": True,
                    "gamma_sensor_to_bit_distance": None,
                }
            ],
            1.0,
        ),
        (
            [
                {
                    "family": "mwd",
                    "has_gamma_sensor": True,
                    "gamma_sensor_to_bit_distance": 1.0,
                }
            ],
            0.0,
        ),
        (
            [
                {
                    "family": "mwd",
                    "has_gamma_sensor": True,
                    "gamma_sensor_to_bit_distance": None,
                },
                {
                    "family": "mwd",
                    "has_gamma_sensor": True,
                    "gamma_sensor_to_bit_distance": 1.0,
                },
            ],
            0.0,
        ),
    ],
    ids=[
        "wrong component",
        "wrong component",
        "wrong component",
        "wrong component",
        "wrong component",
        "wrong component",
        "wrong component",
        "correct component",
        "one wrong, one correct component",
    ],
)
def test_drillstrings_are_filtered(
    components,
    expected_gamma_depth,
    requests_mock: requests_mock_lib.Mocker,
    corva_context,
):
    event = [
        {
            "records": [
                {
                    "timestamp": 0,
                    "asset_id": 0,
                    "company_id": 0,
                    "data": {"bit_depth": 1.0, "gamma_ray": 2},
                    "metadata": {"drillstring": "0"},
                }
            ],
            "metadata": {
                "app_stream_id": 0,
                "apps": {
                    CORVA_SETTINGS.APP_KEY: {
                        "app_connection_id": 0,
                    }
                },
            },
        }
    ]
    text = json.dumps([{"_id": "0", "data": {"components": components}}])
    expected = ActualGammaDepth(
        asset_id=0,
        collection=SETTINGS.actual_gamma_depth_collection,
        company_id=0,
        data=ActualGammaDepthData(
            gamma_depth=expected_gamma_depth, bit_depth=1.0, gamma_ray=2
        ),
        provider=SETTINGS.provider,
        timestamp=0,
        version=SETTINGS.version,
    )

    get_mock = requests_mock.get(requests_mock_lib.ANY, text=text)

    post_mock = requests_mock.post(requests_mock_lib.ANY)

    lambda_handler(event, corva_context)

    assert get_mock.called_once
    assert post_mock.called_once

    actual_gamma_depths = pydantic.parse_obj_as(
        List[ActualGammaDepth], post_mock.last_request.json()
    )  # type: List[ActualGammaDepth]

    assert len(actual_gamma_depths) == 1
    assert actual_gamma_depths[0] == expected


@pytest.mark.parametrize(
    "raises,status_code", [(True, 404), (False, 200)], ids=["must raise", "must pass"]
)
def test_fail_if_couldnt_post(
    raises, status_code, requests_mock: requests_mock_lib.Mocker, corva_context
):
    event = [
        {
            "records": [
                {
                    "timestamp": 0,
                    "asset_id": 0,
                    "company_id": 0,
                    "data": {"bit_depth": 0.0, "gamma_ray": 0},
                    "metadata": {"drillstring": "0"},
                }
            ],
            "metadata": {
                "app_stream_id": 0,
                "apps": {CORVA_SETTINGS.APP_KEY: {"app_connection_id": 0}},
            },
        }
    ]

    requests_mock.get(requests_mock_lib.ANY, text="[]")
    patch_api_post = requests_mock.post(requests_mock_lib.ANY, status_code=status_code)

    if raises:
        pytest.raises(HTTPError, lambda_handler, event, corva_context)
        return

    lambda_handler(event, corva_context)

    assert patch_api_post.called_once
