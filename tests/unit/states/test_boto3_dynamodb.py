"""
Unit tests for the ``boto3_dynamodb`` state module.
"""

from unittest.mock import MagicMock

import pytest

from saltext.boto3.states import boto3_dynamodb as ddb_state

try:
    import boto3  # noqa: F401  # pylint: disable=unused-import

    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

pytestmark = [
    pytest.mark.skip_on_fips_enabled_platform,
    pytest.mark.skipif(HAS_BOTO3 is False, reason="boto3 is required for these tests."),
]


_DEFAULT_DESCRIBE = {
    "Table": {
        "ProvisionedThroughput": {
            "ReadCapacityUnits": 5,
            "WriteCapacityUnits": 5,
        }
    }
}


def _base_map(
    exists=False,
    describe=None,
    create=True,
    update=True,
    delete=True,
    extract=None,
    create_gsi=True,
    update_gsi=True,
):
    return {
        "boto3_dynamodb.exists": exists,
        "boto3_dynamodb.describe": describe if describe is not None else _DEFAULT_DESCRIBE,
        "boto3_dynamodb.create_table": create,
        "boto3_dynamodb.update": update,
        "boto3_dynamodb.delete": delete,
        "boto3_dynamodb.extract_index": MagicMock(side_effect=extract or (lambda *a, **k: {})),
        "boto3_dynamodb.create_global_secondary_index": create_gsi,
        "boto3_dynamodb.update_global_secondary_index": update_gsi,
        "config.option": {},
        "pillar.get": [],
    }


@pytest.fixture
def configure_loader_modules():
    return {
        ddb_state: {
            "__opts__": {"test": False},
            "__salt__": {},
            "__states__": {},
        }
    }


def test_virtual(mock_salt):
    with mock_salt(ddb_state, {"boto3_dynamodb.exists": True}):
        assert ddb_state.__virtual__() == "boto3_dynamodb"


def test_virtual_no_exec_module(mock_salt):
    with mock_salt(ddb_state, {}):
        result = ddb_state.__virtual__()
    assert result[0] is False


def test_present_creates_table(mock_salt):
    with mock_salt(ddb_state, _base_map(exists=False)):
        ret = ddb_state.present(
            name="t",
            read_capacity_units=5,
            write_capacity_units=5,
            hash_key="pk",
            hash_key_data_type="S",
        )
    assert ret["result"] is True
    assert "successfully created" in ret["comment"]
    assert ret["changes"]["new"]["table"] == "t"


def test_present_test_mode_create(mock_salt):
    with mock_salt(ddb_state, _base_map(exists=False), test=True):
        ret = ddb_state.present(
            name="t",
            read_capacity_units=5,
            write_capacity_units=5,
            hash_key="pk",
            hash_key_data_type="S",
        )
    assert ret["result"] is None
    assert "would be created" in ret["comment"]


def test_present_create_failure(mock_salt):
    with mock_salt(ddb_state, _base_map(exists=False, create=False)):
        ret = ddb_state.present(
            name="t",
            read_capacity_units=5,
            write_capacity_units=5,
            hash_key="pk",
            hash_key_data_type="S",
        )
    assert ret["result"] is False
    assert "Failed to create" in ret["comment"]


def test_present_already_exists_throughput_matches(mock_salt):
    with mock_salt(ddb_state, _base_map(exists=True)):
        ret = ddb_state.present(
            name="t",
            read_capacity_units=5,
            write_capacity_units=5,
            hash_key="pk",
            hash_key_data_type="S",
        )
    assert ret["result"] is True
    assert "throughput matches" in ret["comment"]


def test_present_throughput_update(mock_salt):
    with mock_salt(ddb_state, _base_map(exists=True)) as salt_mocks:
        ret = ddb_state.present(
            name="t",
            read_capacity_units=10,
            write_capacity_units=10,
            hash_key="pk",
            hash_key_data_type="S",
        )
    assert ret["result"] is True
    salt_mocks["boto3_dynamodb.update"].assert_called_once()
    assert ret["changes"]["new"]["read_capacity_units"] == (10,)
    assert ret["changes"]["old"]["read_capacity_units"] == (5,)


def test_present_throughput_update_test_mode(mock_salt):
    with mock_salt(ddb_state, _base_map(exists=True), test=True) as salt_mocks:
        ret = ddb_state.present(
            name="t",
            read_capacity_units=10,
            write_capacity_units=10,
            hash_key="pk",
            hash_key_data_type="S",
        )
    assert ret["result"] is None
    salt_mocks["boto3_dynamodb.update"].assert_not_called()


def test_present_throughput_update_failure(mock_salt):
    with mock_salt(ddb_state, _base_map(exists=True, update=False)):
        ret = ddb_state.present(
            name="t",
            read_capacity_units=10,
            write_capacity_units=10,
            hash_key="pk",
            hash_key_data_type="S",
        )
    assert ret["result"] is False


def test_present_deprecation_warning_for_table_name(mock_salt):
    with mock_salt(ddb_state, _base_map(exists=True)):
        ret = ddb_state.present(
            table_name="t",
            read_capacity_units=5,
            write_capacity_units=5,
            hash_key="pk",
            hash_key_data_type="S",
        )
    assert ret["result"] is True
    assert "warnings" in ret
    assert ret["name"] == "t"


def test_present_adds_new_gsi(mock_salt):
    describe = {
        "Table": {
            "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
            "GlobalSecondaryIndexes": [],
        }
    }
    salt_map = _base_map(exists=True, describe=describe)
    salt_map["boto3_dynamodb.extract_index"] = MagicMock(
        return_value={
            "IndexName": "new_gsi",
            "KeySchema": [],
            "Projection": {"ProjectionType": "ALL"},
            "ProvisionedThroughput": {
                "ReadCapacityUnits": 1,
                "WriteCapacityUnits": 1,
            },
        }
    )
    gsi = {"index": [{"name": "new_gsi"}]}
    with mock_salt(ddb_state, salt_map) as salt_mocks:
        ret = ddb_state.present(
            name="t",
            read_capacity_units=5,
            write_capacity_units=5,
            hash_key="pk",
            hash_key_data_type="S",
            global_indexes=[gsi],
        )
    assert ret["result"] is True
    salt_mocks["boto3_dynamodb.create_global_secondary_index"].assert_called_once()


def test_present_rejects_multiple_new_gsis(mock_salt):
    describe = {
        "Table": {
            "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
            "GlobalSecondaryIndexes": [],
        }
    }
    salt_map = _base_map(exists=True, describe=describe)
    gsi_a = {"index": [{"name": "a"}]}
    gsi_b = {"index": [{"name": "b"}]}
    with mock_salt(ddb_state, salt_map):
        ret = ddb_state.present(
            name="t",
            read_capacity_units=5,
            write_capacity_units=5,
            hash_key="pk",
            hash_key_data_type="S",
            global_indexes=[gsi_a, gsi_b],
        )
    assert ret["result"] is False
    assert "Creation of multiple GSIs" in ret["comment"]


def test_present_rejects_gsi_deletion(mock_salt):
    describe = {
        "Table": {
            "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
            "GlobalSecondaryIndexes": [
                {
                    "IndexName": "old_gsi",
                    "Projection": {"ProjectionType": "ALL"},
                    "ProvisionedThroughput": {
                        "ReadCapacityUnits": 1,
                        "WriteCapacityUnits": 1,
                    },
                }
            ],
        }
    }
    with mock_salt(ddb_state, _base_map(exists=True, describe=describe)):
        ret = ddb_state.present(
            name="t",
            read_capacity_units=5,
            write_capacity_units=5,
            hash_key="pk",
            hash_key_data_type="S",
            global_indexes=[],
        )
    assert ret["result"] is False
    assert "Deletion of GSIs" in ret["comment"]


def test_present_updates_gsi_throughput(mock_salt):
    describe = {
        "Table": {
            "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
            "GlobalSecondaryIndexes": [
                {
                    "IndexName": "g1",
                    "Projection": {"ProjectionType": "ALL"},
                    "ProvisionedThroughput": {
                        "ReadCapacityUnits": 1,
                        "WriteCapacityUnits": 1,
                    },
                }
            ],
        }
    }
    salt_map = _base_map(exists=True, describe=describe)
    salt_map["boto3_dynamodb.extract_index"] = MagicMock(
        return_value={
            "IndexName": "g1",
            "Projection": {"ProjectionType": "ALL"},
            "ProvisionedThroughput": {
                "ReadCapacityUnits": 10,
                "WriteCapacityUnits": 10,
            },
        }
    )
    gsi = {"index": [{"name": "g1"}]}
    with mock_salt(ddb_state, salt_map) as salt_mocks:
        ret = ddb_state.present(
            name="t",
            read_capacity_units=5,
            write_capacity_units=5,
            hash_key="pk",
            hash_key_data_type="S",
            global_indexes=[gsi],
        )
    assert ret["result"] is True
    salt_mocks["boto3_dynamodb.update_global_secondary_index"].assert_called_once_with(
        "t",
        {"g1": {"read": 10, "write": 10}},
        region=None,
        key=None,
        keyid=None,
        profile=None,
    )


def test_present_gsi_projection_mismatch_fails(mock_salt):
    describe = {
        "Table": {
            "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
            "GlobalSecondaryIndexes": [
                {
                    "IndexName": "g1",
                    "Projection": {"ProjectionType": "ALL"},
                    "ProvisionedThroughput": {
                        "ReadCapacityUnits": 1,
                        "WriteCapacityUnits": 1,
                    },
                }
            ],
        }
    }
    salt_map = _base_map(exists=True, describe=describe)
    salt_map["boto3_dynamodb.extract_index"] = MagicMock(
        return_value={
            "IndexName": "g1",
            "Projection": {"ProjectionType": "KEYS_ONLY"},
            "ProvisionedThroughput": {
                "ReadCapacityUnits": 1,
                "WriteCapacityUnits": 1,
            },
        }
    )
    gsi = {"index": [{"name": "g1"}]}
    with mock_salt(ddb_state, salt_map):
        ret = ddb_state.present(
            name="t",
            read_capacity_units=5,
            write_capacity_units=5,
            hash_key="pk",
            hash_key_data_type="S",
            global_indexes=[gsi],
        )
    assert ret["result"] is False
    assert "GSI projection types do not match" in ret["comment"]


def test_absent_table_does_not_exist(mock_salt):
    with mock_salt(ddb_state, {"boto3_dynamodb.exists": False}):
        ret = ddb_state.absent("t")
    assert ret["result"] is True
    assert "does not exist" in ret["comment"]


def test_absent_test_mode(mock_salt):
    with mock_salt(ddb_state, {"boto3_dynamodb.exists": True}, test=True):
        ret = ddb_state.absent("t")
    assert ret["result"] is None


def test_absent_deletes_table(mock_salt):
    salt_map = {
        "boto3_dynamodb.exists": True,
        "boto3_dynamodb.delete": True,
    }
    with mock_salt(ddb_state, salt_map):
        ret = ddb_state.absent("t")
    assert ret["result"] is True
    assert "Deleted" in ret["comment"]


def test_absent_delete_failure(mock_salt):
    salt_map = {
        "boto3_dynamodb.exists": True,
        "boto3_dynamodb.delete": False,
    }
    with mock_salt(ddb_state, salt_map):
        ret = ddb_state.absent("t")
    assert ret["result"] is False
