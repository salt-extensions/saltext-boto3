"""
Unit tests for the ``boto3_lambda`` execution module.
"""

import json as _json
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from salt.exceptions import SaltInvocationError

from saltext.boto3.modules import boto3_lambda

try:
    import botocore  # pylint: disable=unused-import

    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

pytestmark = [
    pytest.mark.skip_on_fips_enabled_platform,
    pytest.mark.skipif(HAS_BOTO3 is False, reason="The boto3 module must be installed."),
]


@pytest.fixture
def configure_loader_modules():
    return {
        boto3_lambda: {
            "__opts__": {"test": False},
            "__context__": {},
            "__salt__": {
                "boto3_iam.get_account_id": MagicMock(return_value="123456789012"),
                "boto3_vpc.get_resource_id": MagicMock(return_value={"id": "subnet-1"}),
                "boto3_secgroup.get_group_id": MagicMock(return_value="sg-1"),
                "cp.cache_file": MagicMock(return_value="/tmp/ok.zip"),
            },
        }
    }


@pytest.fixture
def conn(make_conn):
    with make_conn(boto3_lambda) as client:
        yield client


def test_get_role_arn_full_arn():
    arn = "arn:aws:iam::111:role/foo"
    assert boto3_lambda._get_role_arn(arn) == arn


def test_get_role_arn_from_name():
    assert boto3_lambda._get_role_arn("myrole") == "arn:aws:iam::123456789012:role/myrole"


def test_resolve_vpcconfig_none():
    assert boto3_lambda._resolve_vpcconfig(None) is None


def test_resolve_vpcconfig_json_string():
    out = boto3_lambda._resolve_vpcconfig('{"SubnetIds": ["s-1"], "SecurityGroupIds": ["sg-0"]}')
    assert out["SubnetIds"] == ["s-1"]
    assert out["SecurityGroupIds"] == ["sg-0"]


def test_resolve_vpcconfig_resolves_names():
    out = boto3_lambda._resolve_vpcconfig({"SubnetNames": ["a"], "SecurityGroupNames": ["b"]})
    assert out["SubnetIds"] == ["subnet-1"]
    assert out["SecurityGroupIds"] == ["sg-1"]


def test_resolve_vpcconfig_invalid_type():
    with pytest.raises(SaltInvocationError):
        boto3_lambda._resolve_vpcconfig(123)


def test_find_function_found(conn):
    conn.list_functions.side_effect = [
        {"Functions": [{"FunctionName": "a"}], "NextMarker": "x"},
        {"Functions": [{"FunctionName": "b"}]},
    ]
    assert boto3_lambda._find_function("b")["FunctionName"] == "b"


def test_find_function_missing(conn):
    conn.list_functions.return_value = {"Functions": []}
    assert boto3_lambda._find_function("zz") is None


def test_function_exists_true(conn):
    conn.list_functions.return_value = {"Functions": [{"FunctionName": "f"}]}
    assert boto3_lambda.function_exists("f") == {"exists": True}


def test_function_exists_false(conn):
    conn.list_functions.return_value = {"Functions": []}
    assert boto3_lambda.function_exists("f") == {"exists": False}


def test_function_exists_error(conn, client_error):
    conn.list_functions.side_effect = client_error("Oops", "ListFunctions")
    ret = boto3_lambda.function_exists("f")
    assert "error" in ret


def test_create_function_zipfile(conn, tmp_path):
    zf = tmp_path / "z.zip"
    zf.write_bytes(b"payload")
    conn.create_function.return_value = {"FunctionName": "f"}
    ret = boto3_lambda.create_function("f", "python3.9", "myrole", "h.handler", ZipFile=str(zf))
    assert ret == {"created": True, "name": "f"}
    args = conn.create_function.call_args.kwargs
    assert args["Code"] == {"ZipFile": b"payload"}
    assert args["Role"] == "arn:aws:iam::123456789012:role/myrole"


def test_create_function_s3(conn):
    conn.create_function.return_value = {"FunctionName": "f"}
    ret = boto3_lambda.create_function(
        "f",
        "python3.9",
        "arn:aws:iam::1:role/r",
        "h.h",
        S3Bucket="b",
        S3Key="k",
        S3ObjectVersion="v",
    )
    assert ret["created"] is True
    assert conn.create_function.call_args.kwargs["Code"] == {
        "S3Bucket": "b",
        "S3Key": "k",
        "S3ObjectVersion": "v",
    }


def test_create_function_invalid_combo(conn, tmp_path):
    zf = tmp_path / "z.zip"
    zf.write_bytes(b"p")
    with pytest.raises(SaltInvocationError):
        boto3_lambda.create_function("f", "python3.9", "r", "h", ZipFile=str(zf), S3Bucket="b")


def test_create_function_missing_code(conn):
    with pytest.raises(SaltInvocationError):
        boto3_lambda.create_function("f", "python3.9", "r", "h")


def test_create_function_remote_zipfile(conn, tmp_path):
    real = tmp_path / "cached.zip"
    real.write_bytes(b"p")
    conn.create_function.return_value = {"FunctionName": "f"}
    with patch.dict(boto3_lambda.__salt__, {"cp.cache_file": MagicMock(return_value=str(real))}):
        ret = boto3_lambda.create_function("f", "python3.9", "r", "h", ZipFile="salt://z.zip")
    assert ret["created"] is True


def test_create_function_cache_fail(conn):
    with patch.dict(boto3_lambda.__salt__, {"cp.cache_file": MagicMock(return_value=False)}):
        ret = boto3_lambda.create_function("f", "python3.9", "r", "h", ZipFile="salt://z.zip")
    assert ret["created"] is False
    assert "Failed to cache" in ret["error"]["message"]


def test_create_function_client_error(conn, client_error, tmp_path):
    zf = tmp_path / "z.zip"
    zf.write_bytes(b"p")
    conn.create_function.side_effect = client_error("AccessDenied", "CreateFunction")
    ret = boto3_lambda.create_function("f", "python3.9", "r", "h", ZipFile=str(zf))
    assert ret["created"] is False
    assert "error" in ret


def test_create_function_wait_for_role_retries(conn, client_error, tmp_path):
    zf = tmp_path / "z.zip"
    zf.write_bytes(b"p")
    conn.create_function.side_effect = [
        client_error("InvalidParameterValueException", "CreateFunction"),
        {"FunctionName": "f"},
    ]
    with (
        patch("saltext.boto3.modules.boto3_lambda.time.sleep"),
        patch("saltext.boto3.modules.boto3_lambda.random.randint", return_value=0),
    ):
        ret = boto3_lambda.create_function(
            "f", "python3.9", "r", "h", ZipFile=str(zf), WaitForRole=True, RoleRetries=2
        )
    assert ret == {"created": True, "name": "f"}


def test_create_function_none(conn, tmp_path):
    zf = tmp_path / "z.zip"
    zf.write_bytes(b"p")
    conn.create_function.return_value = None
    ret = boto3_lambda.create_function("f", "python3.9", "r", "h", ZipFile=str(zf))
    assert ret == {"created": False}


def test_delete_function(conn):
    assert boto3_lambda.delete_function("f") == {"deleted": True}
    conn.delete_function.assert_called_once_with(FunctionName="f")


def test_delete_function_with_qualifier(conn):
    boto3_lambda.delete_function("f", Qualifier="1")
    conn.delete_function.assert_called_once_with(FunctionName="f", Qualifier="1")


def test_delete_function_error(conn, client_error):
    conn.delete_function.side_effect = client_error("X", "DeleteFunction")
    ret = boto3_lambda.delete_function("f")
    assert ret["deleted"] is False


def test_describe_function_found(conn):
    conn.list_functions.return_value = {
        "Functions": [{"FunctionName": "f", "Runtime": "py", "Timeout": 3, "MemorySize": 128}]
    }
    ret = boto3_lambda.describe_function("f")
    assert ret["function"]["FunctionName"] == "f"
    assert ret["function"]["Runtime"] == "py"


def test_describe_function_missing(conn):
    conn.list_functions.return_value = {"Functions": []}
    assert boto3_lambda.describe_function("f") == {"function": None}


def test_describe_function_error(conn, client_error):
    conn.list_functions.side_effect = client_error("X", "ListFunctions")
    assert "error" in boto3_lambda.describe_function("f")


def test_update_function_config(conn):
    conn.update_function_configuration.return_value = {
        "FunctionName": "f",
        "Timeout": 10,
    }
    ret = boto3_lambda.update_function_config("f", Timeout=10, Description="d")
    assert ret["updated"] is True
    kw = conn.update_function_configuration.call_args.kwargs
    assert kw["Timeout"] == 10
    assert kw["Description"] == "d"


def test_update_function_config_with_role_and_vpc(conn):
    conn.update_function_configuration.return_value = {"FunctionName": "f"}
    ret = boto3_lambda.update_function_config(
        "f", Role="myrole", VpcConfig={"SubnetIds": ["s-1"], "SecurityGroupIds": ["sg-0"]}
    )
    assert ret["updated"] is True
    kw = conn.update_function_configuration.call_args.kwargs
    assert kw["Role"].endswith(":role/myrole")
    assert kw["VpcConfig"] == {"SubnetIds": ["s-1"], "SecurityGroupIds": ["sg-0"]}


def test_update_function_config_none(conn):
    conn.update_function_configuration.return_value = None
    ret = boto3_lambda.update_function_config("f", Timeout=1)
    assert ret == {"updated": False}


def test_update_function_config_error(conn, client_error):
    conn.update_function_configuration.side_effect = client_error("X", "UpdateFunctionConfig")
    ret = boto3_lambda.update_function_config("f", Timeout=1)
    assert ret["updated"] is False


def test_update_function_config_retry_success(conn, client_error):
    conn.update_function_configuration.side_effect = [
        client_error("InvalidParameterValueException", "UpdateFunctionConfig"),
        {"FunctionName": "f"},
    ]
    with (
        patch("saltext.boto3.modules.boto3_lambda.time.sleep"),
        patch("saltext.boto3.modules.boto3_lambda.random.randint", return_value=0),
    ):
        ret = boto3_lambda.update_function_config("f", Timeout=1, WaitForRole=True, RoleRetries=2)
    assert ret["updated"] is True


def test_update_function_code_zip(conn, tmp_path):
    zf = tmp_path / "z.zip"
    zf.write_bytes(b"p")
    conn.update_function_code.return_value = {"FunctionName": "f"}
    ret = boto3_lambda.update_function_code("f", ZipFile=str(zf))
    assert ret["updated"] is True


def test_update_function_code_s3(conn):
    conn.update_function_code.return_value = {"FunctionName": "f"}
    ret = boto3_lambda.update_function_code("f", S3Bucket="b", S3Key="k", S3ObjectVersion="v")
    assert ret["updated"] is True


def test_update_function_code_invalid(conn, tmp_path):
    zf = tmp_path / "z.zip"
    zf.write_bytes(b"p")
    with pytest.raises(SaltInvocationError):
        boto3_lambda.update_function_code("f", ZipFile=str(zf), S3Bucket="b")


def test_update_function_code_missing(conn):
    with pytest.raises(SaltInvocationError):
        boto3_lambda.update_function_code("f")


def test_update_function_code_none(conn):
    conn.update_function_code.return_value = None
    ret = boto3_lambda.update_function_code("f", S3Bucket="b", S3Key="k")
    assert ret == {"updated": False}


def test_update_function_code_error(conn, client_error):
    conn.update_function_code.side_effect = client_error("X", "UpdateFunctionCode")
    ret = boto3_lambda.update_function_code("f", S3Bucket="b", S3Key="k")
    assert ret["updated"] is False


def test_add_permission(conn):
    ret = boto3_lambda.add_permission("f", "sid", "lambda:*", "p", SourceArn="arn", Qualifier="1")
    assert ret == {"updated": True}
    kw = conn.add_permission.call_args.kwargs
    assert kw["StatementId"] == "sid"
    assert kw["SourceArn"] == "arn"
    assert kw["Qualifier"] == "1"


def test_add_permission_error(conn, client_error):
    conn.add_permission.side_effect = client_error("X", "AddPermission")
    ret = boto3_lambda.add_permission("f", "s", "a", "p")
    assert ret["updated"] is False


def test_remove_permission(conn):
    ret = boto3_lambda.remove_permission("f", "sid", Qualifier="1")
    assert ret == {"updated": True}
    conn.remove_permission.assert_called_once_with(
        FunctionName="f", StatementId="sid", Qualifier="1"
    )


def test_remove_permission_no_qualifier(conn):
    boto3_lambda.remove_permission("f", "sid")
    conn.remove_permission.assert_called_once_with(FunctionName="f", StatementId="sid")


def test_remove_permission_error(conn, client_error):
    conn.remove_permission.side_effect = client_error("X", "RemovePermission")
    assert boto3_lambda.remove_permission("f", "s")["updated"] is False


def test_get_permissions(conn):
    policy = {
        "Statement": [
            {
                "Sid": "s1",
                "Action": "lambda:*",
                "Principal": {"Service": "s3.amazonaws.com"},
                "Condition": {
                    "ArnLike": {"AWS:SourceArn": "arn"},
                    "StringEquals": {"AWS:SourceAccount": "acct"},
                },
            },
            {
                "Sid": "s2",
                "Action": "lambda:*",
                "Principal": {"AWS": "arn:aws:iam::111:root"},
                "Condition": {},
            },
        ]
    }
    conn.get_policy.return_value = {"Policy": _json.dumps(policy)}
    ret = boto3_lambda.get_permissions("f", Qualifier="1")
    assert ret["permissions"]["s1"]["SourceArn"] == "arn"
    assert ret["permissions"]["s1"]["SourceAccount"] == "acct"
    assert ret["permissions"]["s1"]["Principal"] == "s3.amazonaws.com"
    assert ret["permissions"]["s2"]["Principal"] == "111"


def test_get_permissions_not_found(conn, client_error):
    conn.get_policy.side_effect = client_error("ResourceNotFoundException", "GetPolicy")
    assert boto3_lambda.get_permissions("f") == {"permissions": None}


def test_get_permissions_error(conn, client_error):
    conn.get_policy.side_effect = client_error("X", "GetPolicy")
    ret = boto3_lambda.get_permissions("f")
    assert ret["permissions"] is None
    assert "error" in ret


def test_list_functions(conn):
    conn.list_functions.side_effect = [
        {"Functions": [{"FunctionName": "a"}], "NextMarker": "n"},
        {"Functions": [{"FunctionName": "b"}]},
    ]
    assert boto3_lambda.list_functions() == [
        {"FunctionName": "a"},
        {"FunctionName": "b"},
    ]


def test_list_function_versions(conn):
    conn.list_versions_by_function.return_value = {"Versions": [{"Version": "1"}]}
    ret = boto3_lambda.list_function_versions("f")
    assert ret == {"Versions": [{"Version": "1"}]}


def test_list_function_versions_empty(conn):
    conn.list_versions_by_function.return_value = {"Versions": []}
    ret = boto3_lambda.list_function_versions("f")
    assert ret == {"Versions": []}


def test_list_function_versions_error(conn, client_error):
    conn.list_versions_by_function.side_effect = client_error("X", "ListVersionsByFunction")
    assert "error" in boto3_lambda.list_function_versions("f")


def test_create_alias(conn):
    conn.create_alias.return_value = {"Name": "a"}
    ret = boto3_lambda.create_alias("f", "a", "1")
    assert ret == {"created": True, "name": "a"}


def test_create_alias_none(conn):
    conn.create_alias.return_value = None
    assert boto3_lambda.create_alias("f", "a", "1") == {"created": False}


def test_create_alias_error(conn, client_error):
    conn.create_alias.side_effect = client_error("X", "CreateAlias")
    assert boto3_lambda.create_alias("f", "a", "1")["created"] is False


def test_delete_alias(conn):
    assert boto3_lambda.delete_alias("f", "a") == {"deleted": True}


def test_delete_alias_error(conn, client_error):
    conn.delete_alias.side_effect = client_error("X", "DeleteAlias")
    assert boto3_lambda.delete_alias("f", "a")["deleted"] is False


def test_find_alias_found(conn):
    conn.list_aliases.return_value = {"Aliases": [{"Name": "a"}]}
    assert boto3_lambda._find_alias("f", "a", FunctionVersion="1")["Name"] == "a"


def test_find_alias_missing(conn):
    conn.list_aliases.return_value = {"Aliases": []}
    assert boto3_lambda._find_alias("f", "a") is None


def test_alias_exists_true(conn):
    conn.list_aliases.return_value = {"Aliases": [{"Name": "a"}]}
    assert boto3_lambda.alias_exists("f", "a") == {"exists": True}


def test_alias_exists_error(conn, client_error):
    conn.list_aliases.side_effect = client_error("X", "ListAliases")
    assert "error" in boto3_lambda.alias_exists("f", "a")


def test_describe_alias_found(conn):
    conn.list_aliases.return_value = {
        "Aliases": [{"Name": "a", "FunctionVersion": "1", "AliasArn": "arn", "Description": "d"}]
    }
    ret = boto3_lambda.describe_alias("f", "a")
    assert ret["alias"]["Name"] == "a"


def test_describe_alias_missing(conn):
    conn.list_aliases.return_value = {"Aliases": []}
    assert boto3_lambda.describe_alias("f", "a") == {"alias": None}


def test_describe_alias_error(conn, client_error):
    conn.list_aliases.side_effect = client_error("X", "ListAliases")
    assert "error" in boto3_lambda.describe_alias("f", "a")


def test_update_alias(conn):
    conn.update_alias.return_value = {"Name": "a", "FunctionVersion": "2"}
    ret = boto3_lambda.update_alias("f", "a", FunctionVersion="2", Description="d")
    assert ret["updated"] is True


def test_update_alias_none(conn):
    conn.update_alias.return_value = None
    assert boto3_lambda.update_alias("f", "a") == {"updated": False}


def test_update_alias_error(conn, client_error):
    conn.update_alias.side_effect = client_error("X", "UpdateAlias")
    assert boto3_lambda.update_alias("f", "a")["updated"] is False


def test_create_esm(conn):
    conn.create_event_source_mapping.return_value = {"UUID": "uuid"}
    ret = boto3_lambda.create_event_source_mapping("arn", "f", "LATEST")
    assert ret == {"created": True, "id": "uuid"}


def test_create_esm_none(conn):
    conn.create_event_source_mapping.return_value = None
    assert boto3_lambda.create_event_source_mapping("a", "f", "L") == {"created": False}


def test_create_esm_error(conn, client_error):
    conn.create_event_source_mapping.side_effect = client_error("X", "CreateEventSourceMapping")
    assert boto3_lambda.create_event_source_mapping("a", "f", "L")["created"] is False


def test_get_esm_ids(conn):
    conn.list_event_source_mappings.return_value = {
        "EventSourceMappings": [{"UUID": "u1"}, {"UUID": "u2"}]
    }
    ret = boto3_lambda.get_event_source_mapping_ids("arn", "f")
    assert ret == ["u1", "u2"]


def test_get_esm_ids_error(conn, client_error):
    conn.list_event_source_mappings.side_effect = client_error("X", "ListEventSourceMappings")
    assert "error" in boto3_lambda.get_event_source_mapping_ids("a", "f")


def test_get_ids_uuid():
    assert boto3_lambda._get_ids(UUID="u") == ["u"]


def test_get_ids_invalid_combo():
    with pytest.raises(SaltInvocationError):
        boto3_lambda._get_ids(UUID="u", EventSourceArn="a")
    with pytest.raises(SaltInvocationError):
        boto3_lambda._get_ids()


def test_delete_esm_by_uuid(conn):
    assert boto3_lambda.delete_event_source_mapping(UUID="u") == {"deleted": True}
    conn.delete_event_source_mapping.assert_called_once_with(UUID="u")


def test_delete_esm_by_arn_and_name(conn):
    conn.list_event_source_mappings.return_value = {"EventSourceMappings": [{"UUID": "u1"}]}
    ret = boto3_lambda.delete_event_source_mapping(EventSourceArn="a", FunctionName="f")
    assert ret == {"deleted": True}


def test_delete_esm_error(conn, client_error):
    conn.delete_event_source_mapping.side_effect = client_error("X", "DeleteEventSourceMapping")
    ret = boto3_lambda.delete_event_source_mapping(UUID="u")
    assert ret["deleted"] is False


def test_describe_esm(conn):
    conn.get_event_source_mapping.return_value = {
        "UUID": "u",
        "BatchSize": 5,
        "EventSourceArn": "arn",
        "FunctionArn": "fa",
        "State": "Enabled",
    }
    ret = boto3_lambda.describe_event_source_mapping(UUID="u")
    assert ret["event_source_mapping"]["UUID"] == "u"


def test_describe_esm_none(conn):
    conn.get_event_source_mapping.return_value = None
    ret = boto3_lambda.describe_event_source_mapping(UUID="u")
    assert ret == {"event_source_mapping": None}


def test_describe_esm_error(conn, client_error):
    conn.get_event_source_mapping.side_effect = client_error("X", "GetEventSourceMapping")
    assert "error" in boto3_lambda.describe_event_source_mapping(UUID="u")


def test_describe_esm_no_ids(conn):
    conn.list_event_source_mappings.return_value = {"EventSourceMappings": []}
    ret = boto3_lambda.describe_event_source_mapping(EventSourceArn="a", FunctionName="f")
    assert ret == {"event_source_mapping": None}


def test_esm_exists_true(conn):
    conn.get_event_source_mapping.return_value = {"UUID": "u"}
    assert boto3_lambda.event_source_mapping_exists(UUID="u") == {"exists": True}


def test_esm_exists_false(conn):
    conn.get_event_source_mapping.return_value = None
    assert boto3_lambda.event_source_mapping_exists(UUID="u") == {"exists": False}


def test_esm_exists_error(conn, client_error):
    conn.get_event_source_mapping.side_effect = client_error("X", "GetEventSourceMapping")
    ret = boto3_lambda.event_source_mapping_exists(UUID="u")
    assert "error" in ret


def test_update_esm(conn):
    conn.update_event_source_mapping.return_value = {"UUID": "u", "BatchSize": 10}
    ret = boto3_lambda.update_event_source_mapping(
        "u", FunctionName="f", Enabled=True, BatchSize=10
    )
    assert ret["updated"] is True


def test_update_esm_none(conn):
    conn.update_event_source_mapping.return_value = None
    assert boto3_lambda.update_event_source_mapping("u") == {"updated": False}


def test_update_esm_error(conn, client_error):
    conn.update_event_source_mapping.side_effect = client_error("X", "UpdateEventSourceMapping")
    ret = boto3_lambda.update_event_source_mapping("u")
    assert ret["updated"] is False
