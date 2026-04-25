"""
Unit tests for the ``boto3_lambda`` state module.
"""

from unittest.mock import patch

import pytest

from saltext.boto3.states import boto3_lambda

pytestmark = [
    pytest.mark.skip_on_fips_enabled_platform,
]


@pytest.fixture
def configure_loader_modules():
    return {
        boto3_lambda: {
            "__opts__": {"test": False},
            "__salt__": {},
        }
    }


def test_virtual(mock_salt):
    with mock_salt(boto3_lambda, {"boto3_lambda.function_exists": True}):
        assert boto3_lambda.__virtual__() == "boto3_lambda"


def test_virtual_no_exec_module(mock_salt):
    with mock_salt(boto3_lambda, {}):
        result = boto3_lambda.__virtual__()
    assert result[0] is False


def test_function_present_exists_error(mock_salt):
    salt_map = {"boto3_lambda.function_exists": {"error": {"message": "boom"}}}
    with mock_salt(boto3_lambda, salt_map):
        ret = boto3_lambda.function_present("n", "F", "py", "r", "h")
    assert ret["result"] is False
    assert "boom" in ret["comment"]


def test_function_present_create_test_mode(mock_salt):
    salt_map = {"boto3_lambda.function_exists": {"exists": False}}
    with mock_salt(boto3_lambda, salt_map, test=True):
        ret = boto3_lambda.function_present("n", "F", "py", "r", "h")
    assert ret["result"] is None
    assert "set to be created" in ret["comment"]


def test_function_present_create_fail(mock_salt):
    salt_map = {
        "boto3_lambda.function_exists": {"exists": False},
        "boto3_lambda.create_function": {"created": False, "error": {"message": "nope"}},
    }
    with mock_salt(boto3_lambda, salt_map):
        ret = boto3_lambda.function_present("n", "F", "py", "r", "h")
    assert ret["result"] is False


def test_function_present_create_success(mock_salt):
    salt_map = {
        "boto3_lambda.function_exists": {"exists": False},
        "boto3_lambda.create_function": {"created": True},
        "boto3_lambda.describe_function": {"function": {"FunctionName": "F"}},
        "boto3_lambda.get_permissions": {"permissions": {}},
    }
    with mock_salt(boto3_lambda, salt_map):
        ret = boto3_lambda.function_present("n", "F", "py", "r", "h")
    assert ret["result"] is True
    assert "created" in ret["comment"]


def test_function_present_create_with_permissions(mock_salt):
    salt_map = {
        "boto3_lambda.function_exists": {"exists": False},
        "boto3_lambda.create_function": {"created": True},
        "boto3_lambda.add_permission": {"updated": True},
        "boto3_lambda.describe_function": {"function": {"FunctionName": "F"}},
        "boto3_lambda.get_permissions": {"permissions": {}},
    }
    perms = {"sid": {"Action": "lambda:*", "Principal": "p"}}
    with mock_salt(boto3_lambda, salt_map):
        ret = boto3_lambda.function_present("n", "F", "py", "r", "h", Permissions=perms)
    assert ret["result"] is True


def test_function_present_update_all_noop(mock_salt):
    func = {
        "FunctionName": "F",
        "Role": "arn:aws:iam::1:role/r",
        "Handler": "h",
        "Description": "",
        "Timeout": 3,
        "MemorySize": 128,
        "CodeSha256": "x",
        "CodeSize": 10,
        "VpcConfig": None,
        "Environment": None,
    }
    salt_map = {
        "boto3_lambda.function_exists": {"exists": True},
        "boto3_lambda.describe_function": {"function": func},
        "boto3_lambda.update_function_config": {"updated": True},
        "boto3_lambda.update_function_code": {
            "updated": True,
            "function": {"CodeSha256": "x", "CodeSize": 10},
        },
        "boto3_lambda.get_permissions": {"permissions": {}},
    }
    with (
        patch.object(boto3_lambda, "_get_role_arn", return_value="arn:aws:iam::1:role/r"),
        mock_salt(boto3_lambda, salt_map),
    ):
        ret = boto3_lambda.function_present("n", "F", "py", "r", "h", S3Bucket="b", S3Key="k")
    assert ret["result"] is True


def test_function_absent_missing(mock_salt):
    salt_map = {"boto3_lambda.function_exists": {"exists": False}}
    with mock_salt(boto3_lambda, salt_map):
        ret = boto3_lambda.function_absent("n", "F")
    assert ret["result"] is True
    assert "does not exist" in ret["comment"]


def test_function_absent_error(mock_salt):
    salt_map = {"boto3_lambda.function_exists": {"error": {"message": "bad"}}}
    with mock_salt(boto3_lambda, salt_map):
        ret = boto3_lambda.function_absent("n", "F")
    assert ret["result"] is False


def test_function_absent_test_mode(mock_salt):
    salt_map = {"boto3_lambda.function_exists": {"exists": True}}
    with mock_salt(boto3_lambda, salt_map, test=True):
        ret = boto3_lambda.function_absent("n", "F")
    assert ret["result"] is None


def test_function_absent_delete(mock_salt):
    salt_map = {
        "boto3_lambda.function_exists": {"exists": True},
        "boto3_lambda.delete_function": {"deleted": True},
    }
    with mock_salt(boto3_lambda, salt_map):
        ret = boto3_lambda.function_absent("n", "F")
    assert ret["result"] is True
    assert ret["changes"]["new"] == {"function": None}


def test_function_absent_delete_fail(mock_salt):
    salt_map = {
        "boto3_lambda.function_exists": {"exists": True},
        "boto3_lambda.delete_function": {"deleted": False, "error": {"message": "bad"}},
    }
    with mock_salt(boto3_lambda, salt_map):
        ret = boto3_lambda.function_absent("n", "F")
    assert ret["result"] is False


def test_alias_present_exists_error(mock_salt):
    salt_map = {"boto3_lambda.alias_exists": {"error": {"message": "x"}}}
    with mock_salt(boto3_lambda, salt_map):
        ret = boto3_lambda.alias_present("n", "F", "a", "1")
    assert ret["result"] is False


def test_alias_present_create_test_mode(mock_salt):
    salt_map = {"boto3_lambda.alias_exists": {"exists": False}}
    with mock_salt(boto3_lambda, salt_map, test=True):
        ret = boto3_lambda.alias_present("n", "F", "a", "1")
    assert ret["result"] is None


def test_alias_present_create_success(mock_salt):
    salt_map = {
        "boto3_lambda.alias_exists": {"exists": False},
        "boto3_lambda.create_alias": {"created": True},
        "boto3_lambda.describe_alias": {
            "alias": {"Name": "a", "FunctionVersion": "1", "Description": ""}
        },
    }
    with mock_salt(boto3_lambda, salt_map):
        ret = boto3_lambda.alias_present("n", "F", "a", "1")
    assert ret["result"] is True
    assert "created" in ret["comment"]


def test_alias_present_create_fail(mock_salt):
    salt_map = {
        "boto3_lambda.alias_exists": {"exists": False},
        "boto3_lambda.create_alias": {"created": False, "error": {"message": "bad"}},
    }
    with mock_salt(boto3_lambda, salt_map):
        ret = boto3_lambda.alias_present("n", "F", "a", "1")
    assert ret["result"] is False


def test_alias_present_no_update_needed(mock_salt):
    salt_map = {
        "boto3_lambda.alias_exists": {"exists": True},
        "boto3_lambda.describe_alias": {"alias": {"FunctionVersion": "1", "Description": "d"}},
    }
    with mock_salt(boto3_lambda, salt_map):
        ret = boto3_lambda.alias_present("n", "F", "a", "1", Description="d")
    assert ret["result"] is True


def test_alias_present_update(mock_salt):
    salt_map = {
        "boto3_lambda.alias_exists": {"exists": True},
        "boto3_lambda.describe_alias": {"alias": {"FunctionVersion": "1", "Description": "old"}},
        "boto3_lambda.update_alias": {"updated": True},
    }
    with mock_salt(boto3_lambda, salt_map):
        ret = boto3_lambda.alias_present("n", "F", "a", "2", Description="new")
    assert ret["result"] is True
    assert "old" in ret["changes"]


def test_alias_present_update_test_mode(mock_salt):
    salt_map = {
        "boto3_lambda.alias_exists": {"exists": True},
        "boto3_lambda.describe_alias": {"alias": {"FunctionVersion": "1", "Description": "old"}},
    }
    with mock_salt(boto3_lambda, salt_map, test=True):
        ret = boto3_lambda.alias_present("n", "F", "a", "2")
    assert ret["result"] is None


def test_alias_present_update_fail(mock_salt):
    salt_map = {
        "boto3_lambda.alias_exists": {"exists": True},
        "boto3_lambda.describe_alias": {"alias": {"FunctionVersion": "1", "Description": "old"}},
        "boto3_lambda.update_alias": {"updated": False, "error": {"message": "x"}},
    }
    with mock_salt(boto3_lambda, salt_map):
        ret = boto3_lambda.alias_present("n", "F", "a", "2")
    assert ret["result"] is False


def test_alias_absent_missing(mock_salt):
    salt_map = {"boto3_lambda.alias_exists": {"exists": False}}
    with mock_salt(boto3_lambda, salt_map):
        ret = boto3_lambda.alias_absent("n", "F", "a")
    assert ret["result"] is True


def test_alias_absent_error(mock_salt):
    salt_map = {"boto3_lambda.alias_exists": {"error": {"message": "x"}}}
    with mock_salt(boto3_lambda, salt_map):
        ret = boto3_lambda.alias_absent("n", "F", "a")
    assert ret["result"] is False


def test_alias_absent_test_mode(mock_salt):
    salt_map = {"boto3_lambda.alias_exists": {"exists": True}}
    with mock_salt(boto3_lambda, salt_map, test=True):
        ret = boto3_lambda.alias_absent("n", "F", "a")
    assert ret["result"] is None


def test_alias_absent_delete(mock_salt):
    salt_map = {
        "boto3_lambda.alias_exists": {"exists": True},
        "boto3_lambda.delete_alias": {"deleted": True},
    }
    with mock_salt(boto3_lambda, salt_map):
        ret = boto3_lambda.alias_absent("n", "F", "a")
    assert ret["result"] is True


def test_alias_absent_delete_fail(mock_salt):
    salt_map = {
        "boto3_lambda.alias_exists": {"exists": True},
        "boto3_lambda.delete_alias": {"deleted": False, "error": {"message": "x"}},
    }
    with mock_salt(boto3_lambda, salt_map):
        ret = boto3_lambda.alias_absent("n", "F", "a")
    assert ret["result"] is False


def test_esm_present_exists_error(mock_salt):
    salt_map = {"boto3_lambda.event_source_mapping_exists": {"error": {"message": "x"}}}
    with mock_salt(boto3_lambda, salt_map):
        ret = boto3_lambda.event_source_mapping_present("n", "arn", "F", "LATEST")
    assert ret["result"] is False


def test_esm_present_create_test_mode(mock_salt):
    salt_map = {"boto3_lambda.event_source_mapping_exists": {"exists": False}}
    with mock_salt(boto3_lambda, salt_map, test=True):
        ret = boto3_lambda.event_source_mapping_present("n", "arn", "F", "LATEST")
    assert ret["result"] is None


def test_esm_present_create_success(mock_salt):
    salt_map = {
        "boto3_lambda.event_source_mapping_exists": {"exists": False},
        "boto3_lambda.create_event_source_mapping": {"created": True},
        "boto3_lambda.describe_event_source_mapping": {"event_source_mapping": {"UUID": "u"}},
    }
    with mock_salt(boto3_lambda, salt_map):
        ret = boto3_lambda.event_source_mapping_present("n", "arn", "F", "LATEST")
    assert ret["result"] is True
    assert ret["name"] == "u"


def test_esm_present_create_fail(mock_salt):
    salt_map = {
        "boto3_lambda.event_source_mapping_exists": {"exists": False},
        "boto3_lambda.create_event_source_mapping": {
            "created": False,
            "error": {"message": "bad"},
        },
    }
    with mock_salt(boto3_lambda, salt_map):
        ret = boto3_lambda.event_source_mapping_present("n", "arn", "F", "LATEST")
    assert ret["result"] is False


def test_esm_present_update_needed(mock_salt):
    salt_map = {
        "boto3_lambda.event_source_mapping_exists": {"exists": True},
        "boto3_lambda.describe_event_source_mapping": {
            "event_source_mapping": {
                "UUID": "u",
                "BatchSize": 50,
                "FunctionArn": "arn:aws:lambda:us-east-1:1:function:old",
            }
        },
        "boto3_lambda.update_event_source_mapping": {"updated": True},
    }
    with (
        patch.object(
            boto3_lambda,
            "_get_function_arn",
            return_value="arn:aws:lambda:us-east-1:1:function:F",
        ),
        mock_salt(boto3_lambda, salt_map),
    ):
        ret = boto3_lambda.event_source_mapping_present("n", "arn", "F", "LATEST", BatchSize=100)
    assert ret["result"] is True


def test_esm_present_update_fail(mock_salt):
    salt_map = {
        "boto3_lambda.event_source_mapping_exists": {"exists": True},
        "boto3_lambda.describe_event_source_mapping": {
            "event_source_mapping": {
                "UUID": "u",
                "BatchSize": 50,
                "FunctionArn": "arn:aws:lambda:us-east-1:1:function:F",
            }
        },
        "boto3_lambda.update_event_source_mapping": {
            "updated": False,
            "error": {"message": "bad"},
        },
    }
    with (
        patch.object(
            boto3_lambda,
            "_get_function_arn",
            return_value="arn:aws:lambda:us-east-1:1:function:F",
        ),
        mock_salt(boto3_lambda, salt_map),
    ):
        ret = boto3_lambda.event_source_mapping_present("n", "arn", "F", "LATEST", BatchSize=100)
    assert ret["result"] is False


def test_esm_absent_missing(mock_salt):
    salt_map = {"boto3_lambda.describe_event_source_mapping": {"event_source_mapping": None}}
    with mock_salt(boto3_lambda, salt_map):
        ret = boto3_lambda.event_source_mapping_absent("n", "arn", "F")
    assert ret["result"] is True


def test_esm_absent_error(mock_salt):
    salt_map = {"boto3_lambda.describe_event_source_mapping": {"error": {"message": "x"}}}
    with mock_salt(boto3_lambda, salt_map):
        ret = boto3_lambda.event_source_mapping_absent("n", "arn", "F")
    assert ret["result"] is False


def test_esm_absent_test_mode(mock_salt):
    salt_map = {
        "boto3_lambda.describe_event_source_mapping": {"event_source_mapping": {"UUID": "u"}}
    }
    with mock_salt(boto3_lambda, salt_map, test=True):
        ret = boto3_lambda.event_source_mapping_absent("n", "arn", "F")
    assert ret["result"] is None


def test_esm_absent_delete(mock_salt):
    salt_map = {
        "boto3_lambda.describe_event_source_mapping": {"event_source_mapping": {"UUID": "u"}},
        "boto3_lambda.delete_event_source_mapping": {"deleted": True},
    }
    with mock_salt(boto3_lambda, salt_map):
        ret = boto3_lambda.event_source_mapping_absent("n", "arn", "F")
    assert ret["result"] is True
    assert ret["changes"]["new"] == {"event_source_mapping": None}


def test_esm_absent_delete_fail(mock_salt):
    salt_map = {
        "boto3_lambda.describe_event_source_mapping": {"event_source_mapping": {"UUID": "u"}},
        "boto3_lambda.delete_event_source_mapping": {
            "deleted": False,
            "error": {"message": "x"},
        },
    }
    with mock_salt(boto3_lambda, salt_map):
        ret = boto3_lambda.event_source_mapping_absent("n", "arn", "F")
    assert ret["result"] is False
