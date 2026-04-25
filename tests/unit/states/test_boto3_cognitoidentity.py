"""
Unit tests for the ``boto3_cognitoidentity`` state module.
"""

from unittest.mock import patch

import pytest

from saltext.boto3.states import boto3_cognitoidentity as cognitoidentity_state

try:
    import botocore  # pylint: disable=unused-import

    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

pytestmark = [
    pytest.mark.skip_on_fips_enabled_platform,
    pytest.mark.skipif(HAS_BOTO3 is False, reason="botocore is required for these tests."),
]


@pytest.fixture
def configure_loader_modules():
    return {
        cognitoidentity_state: {
            "__opts__": {"test": False},
            "__salt__": {},
            "__pillar__": {},
        }
    }


def test_virtual(mock_salt):
    with mock_salt(
        cognitoidentity_state,
        {"boto3_cognitoidentity.describe_identity_pools": {"identity_pools": []}},
    ):
        assert cognitoidentity_state.__virtual__() == "boto3_cognitoidentity"


def test_virtual_no_exec_module(mock_salt):
    with mock_salt(cognitoidentity_state, {}):
        result = cognitoidentity_state.__virtual__()
    assert result[0] is False
    assert "boto3_cognitoidentity" in result[1]


def test_get_object_none():
    assert cognitoidentity_state._get_object(None, list) is None


def test_get_object_passthrough_matching_type():
    payload = ["a", "b"]
    assert cognitoidentity_state._get_object(payload, list) == payload


def test_get_object_passthrough_wrong_type_returns_none():
    assert cognitoidentity_state._get_object({"x": 1}, list) is None


def test_get_object_str_not_found():
    with (
        patch.dict(cognitoidentity_state.__opts__, {}, clear=True),
        patch.dict(cognitoidentity_state.__pillar__, {}, clear=True),
    ):
        assert cognitoidentity_state._get_object("missing", list) is None


def test_get_object_str_in_opts():
    with (
        patch.dict(cognitoidentity_state.__opts__, {"providers": ["a"]}),
        patch.dict(cognitoidentity_state.__pillar__, {}, clear=True),
    ):
        assert cognitoidentity_state._get_object("providers", list) == ["a"]


def test_get_object_master_overrides_opts():
    with (
        patch.dict(cognitoidentity_state.__opts__, {"providers": ["a"]}),
        patch.dict(
            cognitoidentity_state.__pillar__,
            {"master": {"providers": ["b"]}},
            clear=True,
        ),
    ):
        assert cognitoidentity_state._get_object("providers", list) == ["b"]


def test_get_object_pillar_overrides_master():
    with (
        patch.dict(cognitoidentity_state.__opts__, {"providers": ["a"]}),
        patch.dict(
            cognitoidentity_state.__pillar__,
            {"master": {"providers": ["b"]}, "providers": ["c"]},
            clear=True,
        ),
    ):
        assert cognitoidentity_state._get_object("providers", list) == ["c"]


def test_get_object_wrong_type_value_returns_none():
    with patch.dict(cognitoidentity_state.__pillar__, {"providers": "not-a-list"}, clear=True):
        assert cognitoidentity_state._get_object("providers", list) is None
