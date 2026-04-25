"""
Unit tests for the ``boto3_cloudwatch_alarm`` state module.
"""

from unittest.mock import MagicMock

import pytest

from saltext.boto3.states import boto3_cloudwatch_alarm as alarm_state

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
        alarm_state: {
            "__opts__": {"test": False},
            "__context__": {},
            "__salt__": {},
        }
    }


@pytest.fixture
def passthrough_arn():
    return MagicMock(side_effect=lambda v, **kw: v)


def test_virtual(mock_salt):
    with mock_salt(alarm_state, {"boto3_cloudwatch.get_alarm": None}):
        assert alarm_state.__virtual__() == "boto3_cloudwatch_alarm"


def test_virtual_no_exec_module(mock_salt):
    with mock_salt(alarm_state, {}):
        result = alarm_state.__virtual__()
    assert result[0] is False


def test_present_create_success(mock_salt, passthrough_arn):
    salt_map = {
        "boto3_cloudwatch.get_alarm": None,
        "boto3_cloudwatch.convert_to_arn": passthrough_arn,
        "boto3_cloudwatch.create_or_update_alarm": True,
    }
    attrs = {"MetricName": "m", "AlarmActions": ["arn:x"]}
    with mock_salt(alarm_state, salt_map):
        result = alarm_state.present("a", attrs)
    assert result["result"] is True
    assert result["changes"]["new"] == attrs


def test_present_create_test_mode(mock_salt, passthrough_arn):
    salt_map = {
        "boto3_cloudwatch.get_alarm": None,
        "boto3_cloudwatch.convert_to_arn": passthrough_arn,
    }
    with mock_salt(alarm_state, salt_map, test=True):
        result = alarm_state.present("a", {})
    assert result["result"] is None


def test_present_create_fail(mock_salt, passthrough_arn):
    salt_map = {
        "boto3_cloudwatch.get_alarm": None,
        "boto3_cloudwatch.convert_to_arn": passthrough_arn,
        "boto3_cloudwatch.create_or_update_alarm": False,
    }
    with mock_salt(alarm_state, salt_map):
        result = alarm_state.present("a", {})
    assert result["result"] is False
    assert "Failed" in result["comment"]


def test_present_existing_no_changes(mock_salt, passthrough_arn):
    existing = {"MetricName": "m", "Threshold": 1.0}
    salt_map = {
        "boto3_cloudwatch.get_alarm": existing,
        "boto3_cloudwatch.convert_to_arn": passthrough_arn,
    }
    with mock_salt(alarm_state, salt_map):
        result = alarm_state.present("a", {"MetricName": "m", "Threshold": 1.0})
    assert result["result"] is True
    assert not result["changes"]
    assert "matching" in result["comment"]


def test_present_existing_update(mock_salt, passthrough_arn):
    existing = {"MetricName": "m", "Threshold": 1.0}
    salt_map = {
        "boto3_cloudwatch.get_alarm": existing,
        "boto3_cloudwatch.convert_to_arn": passthrough_arn,
        "boto3_cloudwatch.create_or_update_alarm": True,
    }
    with mock_salt(alarm_state, salt_map):
        result = alarm_state.present("a", {"MetricName": "m", "Threshold": 2.0})
    assert result["result"] is True
    assert "diff" in result["changes"]


def test_present_existing_update_test_mode(mock_salt, passthrough_arn):
    existing = {"MetricName": "m", "Threshold": 1.0}
    salt_map = {
        "boto3_cloudwatch.get_alarm": existing,
        "boto3_cloudwatch.convert_to_arn": passthrough_arn,
    }
    with mock_salt(alarm_state, salt_map, test=True):
        result = alarm_state.present("a", {"Threshold": 2.0})
    assert result["result"] is None


def test_present_existing_update_fail(mock_salt, passthrough_arn):
    existing = {"MetricName": "m"}
    salt_map = {
        "boto3_cloudwatch.get_alarm": existing,
        "boto3_cloudwatch.convert_to_arn": passthrough_arn,
        "boto3_cloudwatch.create_or_update_alarm": False,
    }
    with mock_salt(alarm_state, salt_map):
        result = alarm_state.present("a", {"MetricName": "new"})
    assert result["result"] is False


def test_absent_missing(mock_salt):
    with mock_salt(alarm_state, {"boto3_cloudwatch.get_alarm": None}):
        result = alarm_state.absent("a")
    assert result["result"] is True
    assert "does not exist" in result["comment"]


def test_absent_test_mode(mock_salt):
    with mock_salt(alarm_state, {"boto3_cloudwatch.get_alarm": {"AlarmName": "a"}}, test=True):
        result = alarm_state.absent("a")
    assert result["result"] is None


def test_absent_success(mock_salt):
    salt_map = {
        "boto3_cloudwatch.get_alarm": {"AlarmName": "a"},
        "boto3_cloudwatch.delete_alarm": True,
    }
    with mock_salt(alarm_state, salt_map):
        result = alarm_state.absent("a")
    assert result["result"] is True
    assert result["changes"]["old"] == "a"


def test_absent_failure(mock_salt):
    salt_map = {
        "boto3_cloudwatch.get_alarm": {"AlarmName": "a"},
        "boto3_cloudwatch.delete_alarm": False,
    }
    with mock_salt(alarm_state, salt_map):
        result = alarm_state.absent("a")
    assert result["result"] is False
