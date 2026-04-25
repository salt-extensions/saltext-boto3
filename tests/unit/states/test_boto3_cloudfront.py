"""
Unit tests for the ``boto3_cloudfront`` state module.
"""

import pytest

from saltext.boto3.states import boto3_cloudfront as cloudfront_state

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
        cloudfront_state: {
            "__opts__": {"test": False},
            "__salt__": {},
        }
    }


def _defaults(**overrides):
    base = {
        "boto3_cloudfront.get_distribution": {"result": None},
        "boto3_cloudfront.create_distribution": {"result": True},
        "boto3_cloudfront.update_distribution": {"result": True},
    }
    base.update(overrides)
    return base


def test_virtual(mock_salt):
    with mock_salt(cloudfront_state, _defaults()):
        assert cloudfront_state.__virtual__() == "boto3_cloudfront"


def test_virtual_no_exec_module(mock_salt):
    with mock_salt(cloudfront_state, {}):
        result = cloudfront_state.__virtual__()
    assert result[0] is False
    assert "boto3_cloudfront" in result[1]


def test_present_get_error(mock_salt):
    salt_map = _defaults(**{"boto3_cloudfront.get_distribution": {"error": {"message": "nope"}}})
    with mock_salt(cloudfront_state, salt_map):
        ret = cloudfront_state.present("mydist", {"c": 1}, {"x": "y"})
    assert ret["result"] is False
    assert "Error checking distribution" in ret["comment"]


def test_present_creates_when_missing(mock_salt):
    with mock_salt(cloudfront_state, _defaults()) as salt:
        ret = cloudfront_state.present("mydist", {"c": 1}, {"x": "y"})
    assert ret["result"] is True
    assert ret["changes"] == {"old": None, "new": "mydist"}
    salt["boto3_cloudfront.create_distribution"].assert_called_once()


def test_present_create_test_mode(mock_salt):
    with mock_salt(cloudfront_state, _defaults(), test=True) as salt:
        ret = cloudfront_state.present("mydist", {"c": 1}, {"x": "y"})
    assert ret["result"] is None
    assert "set for creation" in ret["comment"]
    salt["boto3_cloudfront.create_distribution"].assert_not_called()


def test_present_create_failure(mock_salt):
    salt_map = _defaults(**{"boto3_cloudfront.create_distribution": {"error": {"message": "boom"}}})
    with mock_salt(cloudfront_state, salt_map):
        ret = cloudfront_state.present("mydist", {"c": 1}, {"x": "y"})
    assert ret["result"] is False
    assert "Error creating distribution" in ret["comment"]


def test_present_existing_no_changes(mock_salt):
    existing = {
        "distribution": {"DistributionConfig": {"Comment": "c"}},
        "tags": {"x": "y"},
        "etag": "e",
    }
    salt_map = _defaults(**{"boto3_cloudfront.get_distribution": {"result": existing}})
    with mock_salt(cloudfront_state, salt_map) as salt:
        ret = cloudfront_state.present("mydist", {"Comment": "c"}, {"x": "y"})
    assert ret["result"] is True
    assert "correct config" in ret["comment"]
    salt["boto3_cloudfront.update_distribution"].assert_not_called()


def test_present_existing_update(mock_salt):
    existing = {
        "distribution": {"DistributionConfig": {"Comment": "old"}},
        "tags": {"x": "y"},
        "etag": "e",
    }
    salt_map = _defaults(**{"boto3_cloudfront.get_distribution": {"result": existing}})
    with mock_salt(cloudfront_state, salt_map) as salt:
        ret = cloudfront_state.present("mydist", {"Comment": "new"}, {"x": "y"})
    assert ret["result"] is True
    assert "Updated distribution" in ret["comment"]
    assert "diff" in ret["changes"]
    salt["boto3_cloudfront.update_distribution"].assert_called_once()


def test_present_existing_update_test_mode(mock_salt):
    existing = {
        "distribution": {"DistributionConfig": {"Comment": "old"}},
        "tags": {"x": "y"},
        "etag": "e",
    }
    salt_map = _defaults(**{"boto3_cloudfront.get_distribution": {"result": existing}})
    with mock_salt(cloudfront_state, salt_map, test=True) as salt:
        ret = cloudfront_state.present("mydist", {"Comment": "new"}, {"x": "y"})
    assert ret["result"] is None
    assert "set for new config" in ret["comment"]
    salt["boto3_cloudfront.update_distribution"].assert_not_called()


def test_present_update_failure(mock_salt):
    existing = {
        "distribution": {"DistributionConfig": {"Comment": "old"}},
        "tags": {"x": "y"},
        "etag": "e",
    }
    salt_map = _defaults(
        **{
            "boto3_cloudfront.get_distribution": {"result": existing},
            "boto3_cloudfront.update_distribution": {"error": {"message": "boom"}},
        }
    )
    with mock_salt(cloudfront_state, salt_map):
        ret = cloudfront_state.present("mydist", {"Comment": "new"}, {"x": "y"})
    assert ret["result"] is False
    assert "Error updating distribution" in ret["comment"]
