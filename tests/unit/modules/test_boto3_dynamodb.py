"""
Unit tests for the ``boto3_dynamodb`` execution module.
"""

from unittest.mock import patch

import pytest

from saltext.boto3.modules import boto3_dynamodb

try:
    from botocore.exceptions import ClientError

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
        boto3_dynamodb: {
            "__opts__": {},
            "__context__": {},
            "__salt__": {},
        }
    }


@pytest.fixture
def conn(make_conn):
    with make_conn(boto3_dynamodb) as client:
        yield client


@pytest.fixture(autouse=True)
def _no_sleep():
    with patch.object(boto3_dynamodb.time, "sleep"):
        yield


def test_list_tags_of_resource_success(conn):
    conn.list_tags_of_resource.return_value = {
        "Tags": [{"Key": "a", "Value": "1"}, {"Key": "b", "Value": "2"}],
        "NextToken": None,
    }
    assert boto3_dynamodb.list_tags_of_resource("arn") == {"a": "1", "b": "2"}


def test_list_tags_of_resource_pagination(conn):
    conn.list_tags_of_resource.side_effect = [
        {"Tags": [{"Key": "a", "Value": "1"}], "NextToken": "next"},
        {"Tags": [{"Key": "b", "Value": "2"}]},
    ]
    assert boto3_dynamodb.list_tags_of_resource("arn") == {"a": "1", "b": "2"}


def test_list_tags_of_resource_error(conn, client_error):
    conn.list_tags_of_resource.side_effect = client_error("AccessDenied", "ListTagsOfResource")
    assert boto3_dynamodb.list_tags_of_resource("arn") is False


def test_tag_resource_success(conn):
    assert boto3_dynamodb.tag_resource("arn", {"a": "1"}) is True
    kwargs = conn.tag_resource.call_args.kwargs
    assert kwargs["Tags"] == [{"Key": "a", "Value": "1"}]


def test_tag_resource_list_passthrough(conn):
    boto3_dynamodb.tag_resource("arn", [{"Key": "a", "Value": "1"}])
    assert conn.tag_resource.call_args.kwargs["Tags"] == [{"Key": "a", "Value": "1"}]


def test_tag_resource_error(conn, client_error):
    conn.tag_resource.side_effect = client_error("AccessDenied", "TagResource")
    assert boto3_dynamodb.tag_resource("arn", {}) is False


def test_untag_resource_success(conn):
    assert boto3_dynamodb.untag_resource("arn", ["a"]) is True
    conn.untag_resource.assert_called_once_with(ResourceArn="arn", TagKeys=["a"])


def test_untag_resource_error(conn, client_error):
    conn.untag_resource.side_effect = client_error("AccessDenied", "UntagResource")
    assert boto3_dynamodb.untag_resource("arn", []) is False


def test_exists_true(conn):
    conn.describe_table.return_value = {"Table": {}}
    assert boto3_dynamodb.exists("t") is True


def test_exists_false(conn, client_error):
    conn.describe_table.side_effect = client_error("ResourceNotFoundException", "DescribeTable")
    assert boto3_dynamodb.exists("t") is False


def test_exists_other_error_raises(conn, client_error):
    conn.describe_table.side_effect = client_error("InternalServerError", "DescribeTable")
    with pytest.raises(ClientError):
        boto3_dynamodb.exists("t")


def test_describe(conn):
    conn.describe_table.return_value = {"Table": {"TableName": "t"}}
    assert boto3_dynamodb.describe("t") == {"Table": {"TableName": "t"}}


def test_extract_index_global_with_throughput():
    index = {
        "index": [
            {"name": "gsi1"},
            {"hash_key": "pk"},
            {"hash_key_data_type": "S"},
            {"range_key": "sk"},
            {"range_key_data_type": "N"},
            {"read_capacity_units": 5},
            {"write_capacity_units": 2},
        ]
    }
    spec = boto3_dynamodb.extract_index(index, global_index=True)
    assert spec["IndexName"] == "gsi1"
    assert spec["KeySchema"] == [
        {"AttributeName": "pk", "KeyType": "HASH"},
        {"AttributeName": "sk", "KeyType": "RANGE"},
    ]
    assert spec["Projection"] == {"ProjectionType": "ALL"}
    assert spec["ProvisionedThroughput"] == {
        "ReadCapacityUnits": 5,
        "WriteCapacityUnits": 2,
    }
    assert spec["_AttributeTypes"] == {"pk": "S", "sk": "N"}


def test_extract_index_local():
    index = {
        "index": [
            {"name": "lsi"},
            {"hash_key": "pk"},
            {"hash_key_data_type": "S"},
            {"range_key": "sk"},
            {"range_key_data_type": "S"},
        ]
    }
    spec = boto3_dynamodb.extract_index(index, global_index=False)
    assert "ProvisionedThroughput" not in spec
    assert spec["Projection"] == {"ProjectionType": "ALL"}


def test_extract_index_includes_projection():
    index = {
        "index": [
            {"name": "gsi"},
            {"hash_key": "pk"},
            {"hash_key_data_type": "S"},
            {"includes": ["a", "b"]},
            {"read_capacity_units": 1},
            {"write_capacity_units": 1},
        ]
    }
    spec = boto3_dynamodb.extract_index(index, global_index=True)
    assert spec["Projection"] == {"ProjectionType": "INCLUDE", "NonKeyAttributes": ["a", "b"]}


def test_extract_index_keys_only_projection():
    index = {
        "index": [
            {"name": "gsi"},
            {"hash_key": "pk"},
            {"hash_key_data_type": "S"},
            {"keys_only": True},
            {"read_capacity_units": 1},
            {"write_capacity_units": 1},
        ]
    }
    spec = boto3_dynamodb.extract_index(index, global_index=True)
    assert spec["Projection"] == {"ProjectionType": "KEYS_ONLY"}


def test_extract_index_both_projections_raises():
    index = {
        "index": [
            {"name": "gsi"},
            {"hash_key": "pk"},
            {"hash_key_data_type": "S"},
            {"keys_only": True},
            {"includes": ["a"]},
            {"read_capacity_units": 1},
            {"write_capacity_units": 1},
        ]
    }
    with pytest.raises(Exception):
        boto3_dynamodb.extract_index(index, global_index=True)


def test_create_table_hash_only(conn):
    conn.describe_table.return_value = {"Table": {}}
    result = boto3_dynamodb.create_table(
        "t",
        read_capacity_units=5,
        write_capacity_units=5,
        hash_key="pk",
        hash_key_data_type="S",
    )
    assert result is True
    call_kwargs = conn.create_table.call_args.kwargs
    assert call_kwargs["TableName"] == "t"
    assert call_kwargs["KeySchema"] == [{"AttributeName": "pk", "KeyType": "HASH"}]
    assert call_kwargs["AttributeDefinitions"] == [{"AttributeName": "pk", "AttributeType": "S"}]
    assert call_kwargs["ProvisionedThroughput"] == {
        "ReadCapacityUnits": 5,
        "WriteCapacityUnits": 5,
    }


def test_create_table_with_range_and_gsi(conn):
    conn.describe_table.return_value = {"Table": {}}
    gsi = {
        "index": [
            {"name": "g1"},
            {"hash_key": "email"},
            {"hash_key_data_type": "S"},
            {"read_capacity_units": 1},
            {"write_capacity_units": 1},
        ]
    }
    result = boto3_dynamodb.create_table(
        "t",
        read_capacity_units=5,
        write_capacity_units=5,
        hash_key="pk",
        hash_key_data_type="S",
        range_key="sk",
        range_key_data_type="N",
        global_indexes=[gsi],
    )
    assert result is True
    kwargs = conn.create_table.call_args.kwargs
    assert len(kwargs["GlobalSecondaryIndexes"]) == 1
    attr_names = {a["AttributeName"] for a in kwargs["AttributeDefinitions"]}
    assert attr_names == {"pk", "sk", "email"}
    assert "_AttributeTypes" not in kwargs["GlobalSecondaryIndexes"][0]


def test_create_table_no_hash_key_raises(conn):
    with pytest.raises(Exception):
        boto3_dynamodb.create_table("t")


def test_create_table_create_fails(conn, client_error):
    conn.create_table.side_effect = client_error("ValidationException", "CreateTable")
    result = boto3_dynamodb.create_table(
        "t",
        read_capacity_units=1,
        write_capacity_units=1,
        hash_key="pk",
        hash_key_data_type="S",
    )
    assert result is False


def test_create_table_timeout(conn, client_error):
    conn.describe_table.side_effect = client_error("ResourceNotFoundException", "DescribeTable")
    with patch.object(boto3_dynamodb, "_MAX_WAIT_ATTEMPTS", 2):
        result = boto3_dynamodb.create_table(
            "t",
            read_capacity_units=1,
            write_capacity_units=1,
            hash_key="pk",
            hash_key_data_type="S",
        )
    assert result is False


def test_delete_success(conn, client_error):
    conn.describe_table.side_effect = client_error("ResourceNotFoundException", "DescribeTable")
    assert boto3_dynamodb.delete("t") is True
    conn.delete_table.assert_called_once_with(TableName="t")


def test_delete_error(conn, client_error):
    conn.delete_table.side_effect = client_error("AccessDenied", "DeleteTable")
    assert boto3_dynamodb.delete("t") is False


def test_delete_timeout(conn):
    conn.describe_table.return_value = {"Table": {}}
    with patch.object(boto3_dynamodb, "_MAX_WAIT_ATTEMPTS", 2):
        assert boto3_dynamodb.delete("t") is False


def test_update_throughput_only(conn):
    assert boto3_dynamodb.update("t", throughput={"read": 5, "write": 10}) is True
    kwargs = conn.update_table.call_args.kwargs
    assert kwargs["ProvisionedThroughput"] == {
        "ReadCapacityUnits": 5,
        "WriteCapacityUnits": 10,
    }


def test_update_gsi_passthrough(conn):
    gsi_updates = [{"Update": {"IndexName": "g"}}]
    assert boto3_dynamodb.update("t", global_indexes=gsi_updates) is True
    assert conn.update_table.call_args.kwargs["GlobalSecondaryIndexUpdates"] == gsi_updates


def test_update_error(conn, client_error):
    conn.update_table.side_effect = client_error("AccessDenied", "UpdateTable")
    assert boto3_dynamodb.update("t", throughput={"read": 1, "write": 1}) is False


def test_create_global_secondary_index_success(conn):
    spec = {
        "IndexName": "g1",
        "KeySchema": [{"AttributeName": "email", "KeyType": "HASH"}],
        "Projection": {"ProjectionType": "ALL"},
        "ProvisionedThroughput": {"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
        "_AttributeTypes": {"email": "S"},
    }
    assert boto3_dynamodb.create_global_secondary_index("t", spec) is True
    kwargs = conn.update_table.call_args.kwargs
    assert kwargs["TableName"] == "t"
    update_item = kwargs["GlobalSecondaryIndexUpdates"][0]["Create"]
    assert update_item["IndexName"] == "g1"
    assert "_AttributeTypes" not in update_item
    assert kwargs["AttributeDefinitions"] == [{"AttributeName": "email", "AttributeType": "S"}]


def test_create_global_secondary_index_error(conn, client_error):
    conn.update_table.side_effect = client_error("AccessDenied", "UpdateTable")
    spec = {
        "IndexName": "g1",
        "KeySchema": [{"AttributeName": "email", "KeyType": "HASH"}],
        "Projection": {"ProjectionType": "ALL"},
        "_AttributeTypes": {"email": "S"},
    }
    assert boto3_dynamodb.create_global_secondary_index("t", spec) is False


def test_update_global_secondary_index_success(conn):
    assert (
        boto3_dynamodb.update_global_secondary_index("t", {"g1": {"read": 5, "write": 2}}) is True
    )
    updates = conn.update_table.call_args.kwargs["GlobalSecondaryIndexUpdates"]
    assert updates == [
        {
            "Update": {
                "IndexName": "g1",
                "ProvisionedThroughput": {
                    "ReadCapacityUnits": 5,
                    "WriteCapacityUnits": 2,
                },
            }
        }
    ]


def test_update_global_secondary_index_error(conn, client_error):
    conn.update_table.side_effect = client_error("AccessDenied", "UpdateTable")
    assert (
        boto3_dynamodb.update_global_secondary_index("t", {"g1": {"read": 1, "write": 1}}) is False
    )
