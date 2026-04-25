"""
Unit tests for the boto3_cloudwatch_event execution module.
"""

from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from saltext.boto3.modules import boto3_cloudwatch_event as module

try:
    from botocore.exceptions import ClientError

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
        module: {
            "__opts__": {},
            "__context__": {},
            "__salt__": {},
        }
    }


@pytest.fixture
def conn():
    client = MagicMock()
    with patch.object(module.boto3mod, "get_connection", return_value=client):
        yield client


def _client_error(code, op):
    return ClientError({"Error": {"Code": code, "Message": "boom"}}, op)


def test_virtual_true():
    with patch.object(module, "HAS_BOTO3", True):
        assert module.__virtual__() == "boto3_cloudwatch_event"


def test_virtual_false():
    with patch.object(module, "HAS_BOTO3", False):
        assert module.__virtual__()[0] is False


def test_exists_true(conn):
    conn.list_rules.return_value = {"Rules": [{"Name": "a"}, {"Name": "b"}]}
    assert module.exists("b") == {"exists": True}
    conn.list_rules.assert_called_once_with(NamePrefix="b")


def test_exists_false(conn):
    conn.list_rules.return_value = {"Rules": [{"Name": "other"}]}
    assert module.exists("missing") == {"exists": False}


def test_exists_empty(conn):
    conn.list_rules.return_value = {}
    assert module.exists("x") == {"exists": False}


def test_exists_error(conn):
    conn.list_rules.side_effect = _client_error("Denied", "ListRules")
    assert "error" in module.exists("x")


def test_create_or_update_success(conn):
    conn.put_rule.return_value = {"RuleArn": "arn:x"}
    result = module.create_or_update("r", ScheduleExpression="rate(1 minute)", State="ENABLED")
    assert result == {"created": True, "arn": "arn:x"}
    kwargs = conn.put_rule.call_args.kwargs
    assert kwargs["Name"] == "r"
    assert kwargs["ScheduleExpression"] == "rate(1 minute)"
    assert kwargs["State"] == "ENABLED"
    assert "EventPattern" not in kwargs


def test_create_or_update_empty(conn):
    conn.put_rule.return_value = None
    result = module.create_or_update("r")
    assert result == {"created": False}


def test_create_or_update_error(conn):
    conn.put_rule.side_effect = _client_error("Denied", "PutRule")
    result = module.create_or_update("r")
    assert result["created"] is False
    assert "error" in result


def test_delete_success(conn):
    assert module.delete("r") == {"deleted": True}
    conn.delete_rule.assert_called_once_with(Name="r")


def test_delete_error(conn):
    conn.delete_rule.side_effect = _client_error("Denied", "DeleteRule")
    assert module.delete("r")["deleted"] is False


def test_describe_success(conn):
    conn.describe_rule.return_value = {
        "Name": "r",
        "Arn": "arn:x",
        "ScheduleExpression": "rate(1 minute)",
        "State": "ENABLED",
    }
    result = module.describe("r")
    assert result["rule"]["Name"] == "r"
    assert result["rule"]["ScheduleExpression"] == "rate(1 minute)"


def test_describe_not_found(conn):
    conn.describe_rule.side_effect = _client_error("ResourceNotFoundException", "DescribeRule")
    result = module.describe("r")
    assert result["error"] == "Rule r not found"


def test_describe_other_error(conn):
    conn.describe_rule.side_effect = _client_error("Denied", "DescribeRule")
    assert "error" in module.describe("r")


def test_list_rules(conn):
    conn.list_rules.side_effect = [
        {"Rules": [{"Name": "a"}], "NextToken": "t"},
        {"Rules": [{"Name": "b"}]},
    ]
    result = module.list_rules()
    assert [r["Name"] for r in result] == ["a", "b"]


def test_list_rules_error(conn):
    conn.list_rules.side_effect = _client_error("Denied", "ListRules")
    assert "error" in module.list_rules()


def test_list_targets_success(conn):
    conn.list_targets_by_rule.return_value = {
        "Targets": [{"Id": "t1", "Arn": "arn:x", "Input": "{}"}]
    }
    result = module.list_targets("r")
    assert result == {"targets": [{"Id": "t1", "Arn": "arn:x", "Input": "{}"}]}


def test_list_targets_empty(conn):
    conn.list_targets_by_rule.return_value = {}
    assert module.list_targets("r") == {"targets": None}


def test_list_targets_not_found(conn):
    conn.list_targets_by_rule.side_effect = _client_error(
        "ResourceNotFoundException", "ListTargets"
    )
    assert module.list_targets("r") == {"error": "Rule r not found"}


def test_put_targets_success(conn):
    conn.put_targets.return_value = {"FailedEntryCount": 0}
    result = module.put_targets("r", [{"Id": "t1", "Arn": "arn:x"}])
    assert result == {"failures": None}


def test_put_targets_string_input(conn):
    conn.put_targets.return_value = {"FailedEntryCount": 0}
    module.put_targets("r", '[{"Id":"t1","Arn":"arn:x"}]')
    conn.put_targets.assert_called_once_with(Rule="r", Targets=[{"Id": "t1", "Arn": "arn:x"}])


def test_put_targets_failures(conn):
    conn.put_targets.return_value = {
        "FailedEntryCount": 1,
        "FailedEntries": [{"Id": "t1"}],
    }
    assert module.put_targets("r", [])["failures"] == [{"Id": "t1"}]


def test_put_targets_not_found(conn):
    conn.put_targets.side_effect = _client_error("ResourceNotFoundException", "PutTargets")
    assert module.put_targets("r", []) == {"error": "Rule r not found"}


def test_remove_targets_success(conn):
    conn.remove_targets.return_value = {"FailedEntryCount": 0}
    assert module.remove_targets("r", ["t1"]) == {"failures": None}


def test_remove_targets_string_ids(conn):
    conn.remove_targets.return_value = {"FailedEntryCount": 0}
    module.remove_targets("r", '["t1","t2"]')
    conn.remove_targets.assert_called_once_with(Rule="r", Ids=["t1", "t2"])


def test_remove_targets_failures(conn):
    conn.remove_targets.return_value = {
        "FailedEntryCount": 1,
        "FailedEntries": [{"Id": "t1"}],
    }
    assert module.remove_targets("r", ["t1"])["failures"] == [{"Id": "t1"}]
