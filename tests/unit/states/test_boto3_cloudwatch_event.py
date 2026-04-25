"""
Unit tests for the ``boto3_cloudwatch_event`` state module.
"""

import pytest

from saltext.boto3.states import boto3_cloudwatch_event as event_state

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
        event_state: {
            "__opts__": {"test": False},
            "__context__": {},
            "__salt__": {},
        }
    }


def _describe(**overrides):
    rule = {
        "ScheduleExpression": None,
        "EventPattern": None,
        "Description": None,
        "RoleArn": None,
        "State": None,
    }
    rule.update(overrides)
    return {"rule": rule}


def test_virtual(mock_salt):
    with mock_salt(event_state, {"boto3_cloudwatch_event.exists": True}):
        assert event_state.__virtual__() == "boto3_cloudwatch_event"


def test_virtual_no_exec_module(mock_salt):
    with mock_salt(event_state, {}):
        result = event_state.__virtual__()
    assert result[0] is False


def test_present_create_success(mock_salt):
    salt_map = {
        "boto3_cloudwatch_event.exists": {"exists": False},
        "boto3_cloudwatch_event.create_or_update": {"created": True},
        "boto3_cloudwatch_event.describe": _describe(ScheduleExpression="rate(1 minute)"),
        "boto3_cloudwatch_event.put_targets": {"failures": None},
    }
    targets = [{"Id": "t1", "Arn": "arn:x"}]
    with mock_salt(event_state, salt_map):
        result = event_state.present(
            "x", Name="r", ScheduleExpression="rate(1 minute)", Targets=targets
        )
    assert result["result"] is True
    assert result["changes"]["new"]["rule"]["Targets"] == targets


def test_present_test_mode_create(mock_salt):
    salt_map = {"boto3_cloudwatch_event.exists": {"exists": False}}
    with mock_salt(event_state, salt_map, test=True):
        result = event_state.present("x", Name="r")
    assert result["result"] is None


def test_present_exists_error(mock_salt):
    salt_map = {"boto3_cloudwatch_event.exists": {"error": {"message": "boom"}}}
    with mock_salt(event_state, salt_map):
        result = event_state.present("x", Name="r")
    assert result["result"] is False


def test_present_create_fail(mock_salt):
    salt_map = {
        "boto3_cloudwatch_event.exists": {"exists": False},
        "boto3_cloudwatch_event.create_or_update": {
            "created": False,
            "error": {"message": "fail"},
        },
    }
    with mock_salt(event_state, salt_map):
        result = event_state.present("x", Name="r")
    assert result["result"] is False


def test_present_existing_no_changes(mock_salt):
    salt_map = {
        "boto3_cloudwatch_event.exists": {"exists": True},
        "boto3_cloudwatch_event.describe": _describe(ScheduleExpression="rate(1 minute)"),
        "boto3_cloudwatch_event.list_targets": {"targets": []},
    }
    with mock_salt(event_state, salt_map):
        result = event_state.present("x", Name="r", ScheduleExpression="rate(1 minute)", Targets=[])
    assert result["result"] is True
    assert not result["changes"]


def test_present_existing_update_schedule_and_targets(mock_salt):
    salt_map = {
        "boto3_cloudwatch_event.exists": {"exists": True},
        "boto3_cloudwatch_event.describe": _describe(ScheduleExpression="rate(1 minute)"),
        "boto3_cloudwatch_event.list_targets": {"targets": [{"Id": "old"}]},
        "boto3_cloudwatch_event.create_or_update": {"created": True},
        "boto3_cloudwatch_event.put_targets": {"failures": None},
        "boto3_cloudwatch_event.remove_targets": {"failures": None},
    }
    new_targets = [{"Id": "new", "Arn": "arn:x"}]
    with mock_salt(event_state, salt_map) as salt_mocks:
        result = event_state.present(
            "x",
            Name="r",
            ScheduleExpression="rate(5 minutes)",
            Targets=new_targets,
        )
    assert result["result"] is True
    salt_mocks["boto3_cloudwatch_event.put_targets"].assert_called_once()
    removes = salt_mocks["boto3_cloudwatch_event.remove_targets"].call_args.kwargs
    assert removes["Ids"] == ["old"]


def test_present_existing_update_test_mode(mock_salt):
    salt_map = {
        "boto3_cloudwatch_event.exists": {"exists": True},
        "boto3_cloudwatch_event.describe": _describe(),
        "boto3_cloudwatch_event.list_targets": {"targets": []},
    }
    with mock_salt(event_state, salt_map, test=True):
        result = event_state.present("x", Name="r", ScheduleExpression="rate(5 minutes)")
    assert result["result"] is None


def test_present_existing_update_fail(mock_salt):
    salt_map = {
        "boto3_cloudwatch_event.exists": {"exists": True},
        "boto3_cloudwatch_event.describe": _describe(),
        "boto3_cloudwatch_event.list_targets": {"targets": []},
        "boto3_cloudwatch_event.create_or_update": {
            "created": False,
            "error": {"message": "fail"},
        },
    }
    with mock_salt(event_state, salt_map):
        result = event_state.present("x", Name="r", ScheduleExpression="rate(5 minutes)")
    assert result["result"] is False


def test_absent_missing(mock_salt):
    salt_map = {"boto3_cloudwatch_event.exists": {"exists": False}}
    with mock_salt(event_state, salt_map):
        result = event_state.absent("x", Name="r")
    assert result["result"] is True
    assert "does not exist" in result["comment"]


def test_absent_test_mode(mock_salt):
    salt_map = {"boto3_cloudwatch_event.exists": {"exists": True}}
    with mock_salt(event_state, salt_map, test=True):
        result = event_state.absent("x", Name="r")
    assert result["result"] is None


def test_absent_success_with_targets(mock_salt):
    salt_map = {
        "boto3_cloudwatch_event.exists": {"exists": True},
        "boto3_cloudwatch_event.list_targets": {"targets": [{"Id": "t1"}]},
        "boto3_cloudwatch_event.remove_targets": {"failures": None},
        "boto3_cloudwatch_event.delete": {"deleted": True},
    }
    with mock_salt(event_state, salt_map):
        result = event_state.absent("x", Name="r")
    assert result["result"] is True
    assert result["changes"]["old"] == {"rule": "r"}


def test_absent_success_no_targets(mock_salt):
    salt_map = {
        "boto3_cloudwatch_event.exists": {"exists": True},
        "boto3_cloudwatch_event.list_targets": {"targets": []},
        "boto3_cloudwatch_event.delete": {"deleted": True},
    }
    with mock_salt(event_state, salt_map):
        result = event_state.absent("x", Name="r")
    assert result["result"] is True


def test_absent_delete_failure(mock_salt):
    salt_map = {
        "boto3_cloudwatch_event.exists": {"exists": True},
        "boto3_cloudwatch_event.list_targets": {"targets": []},
        "boto3_cloudwatch_event.delete": {"deleted": False, "error": {"message": "fail"}},
    }
    with mock_salt(event_state, salt_map):
        result = event_state.absent("x", Name="r")
    assert result["result"] is False


def test_absent_remove_targets_failures(mock_salt):
    salt_map = {
        "boto3_cloudwatch_event.exists": {"exists": True},
        "boto3_cloudwatch_event.list_targets": {"targets": [{"Id": "t1"}]},
        "boto3_cloudwatch_event.remove_targets": {"failures": [{"Id": "t1"}]},
    }
    with mock_salt(event_state, salt_map):
        result = event_state.absent("x", Name="r")
    assert result["result"] is False
