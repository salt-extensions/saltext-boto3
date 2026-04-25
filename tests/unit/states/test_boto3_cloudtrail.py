"""
Unit tests for the ``boto3_cloudtrail`` state module.

This file is the canonical template for state-module unit tests in this
extension. New ``tests/unit/states/test_boto3_*.py`` files should follow the
patterns demonstrated here:

- A module-level ``pytestmark`` skips on FIPS and when ``botocore`` is missing.
- ``configure_loader_modules`` initialises ``__opts__`` and ``__salt__`` so the
  loader dunders exist before each test.
- The shared ``mock_salt`` fixture (see ``tests/unit/conftest.py``) is used as a
  context manager to swap ``__salt__`` and toggle ``__opts__["test"]``. Pass a
  mapping of dotted exec-module keys to plain return values, or to ``MagicMock``
  instances (when a ``side_effect`` is needed).
- Every test asserts the explicit ``result``/``comment``/``changes`` contract
  required by Salt state modules, and exercises both normal and test mode where
  applicable.
- Happy-path tests are named for what they do (``test_present_creates_trail``);
  branch tests carry descriptive suffixes (``test_present_test_mode_create``,
  ``test_present_create_failure`` ...).
"""

from unittest.mock import MagicMock

import pytest

from saltext.boto3.states import boto3_cloudtrail as cloudtrail_state

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
        cloudtrail_state: {
            "__opts__": {"test": False},
            "__salt__": {},
        }
    }


@pytest.fixture
def trail_describe():
    return {
        "trail": {
            "Name": "mytrail",
            "S3BucketName": "mybucket",
            "S3KeyPrefix": None,
            "SnsTopicName": None,
            "IncludeGlobalServiceEvents": True,
            "IsMultiRegionTrail": None,
            "LogFileValidationEnabled": False,
            "CloudWatchLogsLogGroupArn": None,
            "CloudWatchLogsRoleArn": None,
            "KmsKeyId": None,
            "HomeRegion": "us-east-1",
            "TrailARN": "arn:aws:cloudtrail:us-east-1:111111111111:trail/mytrail",
        }
    }


@pytest.fixture
def create_mocks(trail_describe):
    """Return a mapping suitable for the create-on-missing branch."""
    return {
        "boto3_cloudtrail.exists": {"exists": False},
        "boto3_cloudtrail.create": {"created": True, "name": "mytrail"},
        "boto3_cloudtrail.describe": trail_describe,
        "boto3_cloudtrail.start_logging": {"started": True},
        "boto3_cloudtrail.add_tags": {"tagged": True},
    }


@pytest.fixture
def already_present_mocks(trail_describe):
    """Return a mapping suitable for the no-change branch."""
    return {
        "boto3_cloudtrail.exists": {"exists": True},
        "boto3_cloudtrail.describe": trail_describe,
        "boto3_cloudtrail.status": {"trail": {"IsLogging": True}},
        "boto3_cloudtrail.list_tags": {"tags": {}},
    }


def test_virtual(mock_salt):
    with mock_salt(cloudtrail_state, {"boto3_cloudtrail.exists": {"exists": False}}):
        assert cloudtrail_state.__virtual__() == "boto3_cloudtrail"


def test_virtual_no_exec_module(mock_salt):
    with mock_salt(cloudtrail_state, {}):
        result = cloudtrail_state.__virtual__()
    assert result[0] is False
    assert "boto3_cloudtrail" in result[1]


def test_present_creates_trail(mock_salt, create_mocks):
    with mock_salt(cloudtrail_state, create_mocks) as salt:
        ret = cloudtrail_state.present("mytrail", "mytrail", "mybucket")
    assert ret["result"] is True
    assert ret["name"] == "mytrail"
    assert ret["changes"]["old"] == {"trail": None}
    assert ret["changes"]["new"]["trail"]["Name"] == "mytrail"
    assert ret["changes"]["new"]["trail"]["LoggingEnabled"] is True
    assert "created" in ret["comment"]
    salt["boto3_cloudtrail.create"].assert_called_once()
    salt["boto3_cloudtrail.start_logging"].assert_called_once()


def test_present_creates_trail_with_logging_disabled(mock_salt, create_mocks):
    with mock_salt(cloudtrail_state, create_mocks) as salt:
        ret = cloudtrail_state.present("mytrail", "mytrail", "mybucket", LoggingEnabled=False)
    assert ret["result"] is True
    assert ret["changes"]["new"]["trail"]["LoggingEnabled"] is False
    salt["boto3_cloudtrail.start_logging"].assert_not_called()


def test_present_creates_trail_with_tags(mock_salt, create_mocks):
    tags = {"env": "prod", "team": "infra"}
    with mock_salt(cloudtrail_state, create_mocks) as salt:
        ret = cloudtrail_state.present("mytrail", "mytrail", "mybucket", Tags=tags)
    assert ret["result"] is True
    assert ret["changes"]["new"]["trail"]["Tags"] == tags
    salt["boto3_cloudtrail.add_tags"].assert_called_once()
    _, kwargs = salt["boto3_cloudtrail.add_tags"].call_args
    assert kwargs["env"] == "prod"
    assert kwargs["team"] == "infra"


def test_present_test_mode_create(mock_salt):
    mocks = {"boto3_cloudtrail.exists": {"exists": False}}
    with mock_salt(cloudtrail_state, mocks, test=True) as salt:
        ret = cloudtrail_state.present("mytrail", "mytrail", "mybucket")
    assert ret["result"] is None
    assert not ret["changes"]
    assert "set to be created" in ret["comment"]
    assert "boto3_cloudtrail.create" not in salt or salt["boto3_cloudtrail.exists"].called


def test_present_exists_check_error(mock_salt):
    mocks = {"boto3_cloudtrail.exists": {"error": {"message": "denied"}}}
    with mock_salt(cloudtrail_state, mocks):
        ret = cloudtrail_state.present("mytrail", "mytrail", "mybucket")
    assert ret["result"] is False
    assert not ret["changes"]
    assert "denied" in ret["comment"]


def test_present_create_failure(mock_salt):
    mocks = {
        "boto3_cloudtrail.exists": {"exists": False},
        "boto3_cloudtrail.create": {"created": False, "error": {"message": "quota exceeded"}},
    }
    with mock_salt(cloudtrail_state, mocks):
        ret = cloudtrail_state.present("mytrail", "mytrail", "mybucket")
    assert ret["result"] is False
    assert not ret["changes"]
    assert "quota exceeded" in ret["comment"]


def test_present_start_logging_failure(mock_salt, create_mocks):
    create_mocks["boto3_cloudtrail.start_logging"] = MagicMock(
        return_value={"started": False, "error": {"message": "no permission"}}
    )
    with mock_salt(cloudtrail_state, create_mocks):
        ret = cloudtrail_state.present("mytrail", "mytrail", "mybucket")
    assert ret["result"] is False
    assert not ret["changes"]
    assert "no permission" in ret["comment"]


def test_present_add_tags_failure(mock_salt, create_mocks):
    create_mocks["boto3_cloudtrail.add_tags"] = MagicMock(
        return_value={"tagged": False, "error": {"message": "tag denied"}}
    )
    with mock_salt(cloudtrail_state, create_mocks):
        ret = cloudtrail_state.present("mytrail", "mytrail", "mybucket", Tags={"a": "1"})
    assert ret["result"] is False
    assert not ret["changes"]
    assert "tag denied" in ret["comment"]


def test_present_already_present_no_changes(mock_salt, already_present_mocks):
    with mock_salt(cloudtrail_state, already_present_mocks):
        ret = cloudtrail_state.present("mytrail", "mytrail", "mybucket")
    assert ret["result"] is True
    assert not ret["changes"]
    assert "is present" in ret["comment"]


def test_present_describe_error_during_update(mock_salt, already_present_mocks):
    already_present_mocks["boto3_cloudtrail.describe"] = MagicMock(
        return_value={"error": {"message": "describe denied"}}
    )
    with mock_salt(cloudtrail_state, already_present_mocks):
        ret = cloudtrail_state.present("mytrail", "mytrail", "mybucket")
    assert ret["result"] is False
    assert not ret["changes"]
    assert "describe denied" in ret["comment"]


def test_absent_when_missing(mock_salt):
    mocks = {"boto3_cloudtrail.exists": {"exists": False}}
    with mock_salt(cloudtrail_state, mocks):
        ret = cloudtrail_state.absent("mytrail", "mytrail")
    assert ret["result"] is True
    assert not ret["changes"]
    assert "does not exist" in ret["comment"]


def test_absent_deletes_existing(mock_salt):
    mocks = {
        "boto3_cloudtrail.exists": {"exists": True},
        "boto3_cloudtrail.delete": {"deleted": True},
    }
    with mock_salt(cloudtrail_state, mocks) as salt:
        ret = cloudtrail_state.absent("mytrail", "mytrail")
    assert ret["result"] is True
    assert ret["changes"] == {"old": {"trail": "mytrail"}, "new": {"trail": None}}
    assert "deleted" in ret["comment"]
    salt["boto3_cloudtrail.delete"].assert_called_once()


def test_absent_test_mode(mock_salt):
    mocks = {"boto3_cloudtrail.exists": {"exists": True}}
    with mock_salt(cloudtrail_state, mocks, test=True):
        ret = cloudtrail_state.absent("mytrail", "mytrail")
    assert ret["result"] is None
    assert not ret["changes"]
    assert "set to be removed" in ret["comment"]


def test_absent_exists_check_error(mock_salt):
    mocks = {"boto3_cloudtrail.exists": {"error": {"message": "denied"}}}
    with mock_salt(cloudtrail_state, mocks):
        ret = cloudtrail_state.absent("mytrail", "mytrail")
    assert ret["result"] is False
    assert not ret["changes"]
    assert "denied" in ret["comment"]


def test_absent_delete_failure(mock_salt):
    mocks = {
        "boto3_cloudtrail.exists": {"exists": True},
        "boto3_cloudtrail.delete": {"deleted": False, "error": {"message": "in use"}},
    }
    with mock_salt(cloudtrail_state, mocks):
        ret = cloudtrail_state.absent("mytrail", "mytrail")
    assert ret["result"] is False
    assert not ret["changes"]
    assert "in use" in ret["comment"]
