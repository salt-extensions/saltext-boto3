"""
Unit tests for the ``boto3_rds`` state module.
"""

import pytest

from saltext.boto3.states import boto3_rds as rds_state

try:
    import botocore  # pylint: disable=unused-import

    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

pytestmark = [
    pytest.mark.skip_on_fips_enabled_platform,
    pytest.mark.skipif(HAS_BOTO3 is False, reason="botocore is required for these tests."),
]


PRESENT_ARGS = ("myrds", 10, "db.t2.micro", "mysql", "u", "p")


@pytest.fixture
def configure_loader_modules():
    return {rds_state: {"__opts__": {"test": False}, "__salt__": {}}}


def test_virtual(mock_salt):
    with mock_salt(rds_state, {"boto3_rds.exists": True}):
        assert rds_state.__virtual__() == "boto3_rds"


def test_virtual_no_exec_module(mock_salt):
    with mock_salt(rds_state, {}):
        result = rds_state.__virtual__()
    assert result[0] is False


def test_present_test_mode(mock_salt):
    salt_map = {"boto3_rds.exists": {"exists": False}}
    with mock_salt(rds_state, salt_map, test=True):
        ret = rds_state.present(*PRESENT_ARGS)
    assert ret["result"] is None
    assert "would be created" in ret["comment"]


def test_present_create_ok(mock_salt):
    salt_map = {
        "boto3_rds.exists": {"exists": False},
        "boto3_rds.create": {"created": True},
        "boto3_rds.describe_db_instances": [{"DBInstanceIdentifier": "myrds"}],
    }
    with mock_salt(rds_state, salt_map):
        ret = rds_state.present(*PRESENT_ARGS)
    assert ret["result"] is True
    assert "created" in ret["comment"]


def test_present_create_fail(mock_salt):
    salt_map = {
        "boto3_rds.exists": {"exists": False},
        "boto3_rds.create": {"created": False, "error": {"message": "bad"}},
    }
    with mock_salt(rds_state, salt_map):
        ret = rds_state.present(*PRESENT_ARGS)
    assert ret["result"] is False


def test_present_already(mock_salt):
    with mock_salt(rds_state, {"boto3_rds.exists": {"exists": True}}):
        ret = rds_state.present(*PRESENT_ARGS)
    assert ret["result"] is True
    assert "exists" in ret["comment"]


def test_replica_present_test_mode(mock_salt):
    with mock_salt(rds_state, {"boto3_rds.exists": {"exists": False}}, test=True):
        ret = rds_state.replica_present("rep", source="src")
    assert ret["result"] is None


def test_replica_present_create(mock_salt):
    salt_map = {
        "boto3_rds.exists": {"exists": False},
        "boto3_rds.create_read_replica": {"exists": True},
        "boto3_rds.describe_db_instances": [{"x": 1}],
    }
    with mock_salt(rds_state, salt_map):
        ret = rds_state.replica_present("rep", source="src")
    assert ret["result"] is True
    assert "created" in ret["comment"]


def test_replica_present_exists_param_group_match(mock_salt):
    salt_map = {
        "boto3_rds.exists": {"exists": True},
        "boto3_rds.describe_db_instances": ["pg"],
    }
    with mock_salt(rds_state, salt_map):
        ret = rds_state.replica_present("rep", source="src", db_parameter_group_name="pg")
    assert ret["result"] is True
    assert "exists" in ret["comment"]


def test_replica_present_exists_param_group_mismatch(mock_salt):
    salt_map = {
        "boto3_rds.exists": {"exists": True},
        "boto3_rds.describe_db_instances": ["old"],
        "boto3_rds.modify_db_instance": {"modified": True},
    }
    with mock_salt(rds_state, salt_map):
        ret = rds_state.replica_present("rep", source="src", db_parameter_group_name="new")
    assert ret["changes"]["old"] == "old"
    assert ret["changes"]["new"] == "new"


def test_subnet_group_present_create(mock_salt):
    salt_map = {
        "boto3_rds.subnet_group_exists": {"exists": False},
        "boto3_rds.create_subnet_group": {"created": True},
    }
    with mock_salt(rds_state, salt_map):
        ret = rds_state.subnet_group_present("sg", "desc", subnet_ids=["subnet-1"])
    assert ret["result"] is True
    assert "created" in ret["comment"].lower()


def test_subnet_group_present_resolves_names(mock_salt):
    salt_map = {
        "boto3_vpc.get_resource_id": {"id": "subnet-abc"},
        "boto3_rds.subnet_group_exists": {"exists": False},
        "boto3_rds.create_subnet_group": {"created": True},
    }
    with mock_salt(rds_state, salt_map) as salt_mocks:
        ret = rds_state.subnet_group_present("sg", "desc", subnet_names=["snA"])
    assert ret["result"] is True
    salt_mocks["boto3_rds.create_subnet_group"].assert_called_once()
    kw = salt_mocks["boto3_rds.create_subnet_group"].call_args.kwargs
    assert kw["subnet_ids"] == ["subnet-abc"]


def test_subnet_group_present_name_lookup_missing(mock_salt):
    salt_map = {"boto3_vpc.get_resource_id": {"id": None}}
    with mock_salt(rds_state, salt_map):
        ret = rds_state.subnet_group_present("sg", "desc", subnet_names=["snA"])
    assert ret["result"] is False


def test_subnet_group_present_already(mock_salt):
    salt_map = {"boto3_rds.subnet_group_exists": {"exists": True}}
    with mock_salt(rds_state, salt_map):
        ret = rds_state.subnet_group_present("sg", "desc", subnet_ids=["subnet-1"])
    assert ret["result"] is True
    assert "present" in ret["comment"]


def test_absent_already(mock_salt):
    with mock_salt(rds_state, {"boto3_rds.describe_db_instances": []}):
        ret = rds_state.absent("r")
    assert ret["result"] is True
    assert "already absent" in ret["comment"]


def test_absent_test_mode(mock_salt):
    with mock_salt(rds_state, {"boto3_rds.describe_db_instances": [{"x": 1}]}, test=True):
        ret = rds_state.absent("r")
    assert ret["result"] is None


def test_absent_delete(mock_salt):
    salt_map = {
        "boto3_rds.describe_db_instances": [{"x": 1}],
        "boto3_rds.delete": {"deleted": True},
    }
    with mock_salt(rds_state, salt_map):
        ret = rds_state.absent("r", skip_final_snapshot=True)
    assert ret["result"] is True
    assert "deleted" in ret["comment"]


def test_subnet_group_absent_missing(mock_salt):
    with mock_salt(rds_state, {"boto3_rds.subnet_group_exists": {}}):
        ret = rds_state.subnet_group_absent("sg")
    assert ret["result"] is True
    assert "does not exist" in ret["comment"]


def test_subnet_group_absent_test_mode(mock_salt):
    salt_map = {"boto3_rds.subnet_group_exists": {"exists": True}}
    with mock_salt(rds_state, salt_map, test=True):
        ret = rds_state.subnet_group_absent("sg")
    assert ret["result"] is None


def test_subnet_group_absent_delete(mock_salt):
    salt_map = {
        "boto3_rds.subnet_group_exists": {"exists": True},
        "boto3_rds.delete_subnet_group": {"deleted": True},
    }
    with mock_salt(rds_state, salt_map):
        ret = rds_state.subnet_group_absent("sg")
    assert ret["result"] is True
    assert "deleted" in ret["comment"]


def test_parameter_present_create_test_mode(mock_salt):
    salt_map = {"boto3_rds.parameter_group_exists": {"exists": False}}
    with mock_salt(rds_state, salt_map, test=True):
        ret = rds_state.parameter_present("pg", "mysql5.6", "desc")
    assert ret["result"] is None


def test_parameter_present_create(mock_salt):
    salt_map = {
        "boto3_rds.parameter_group_exists": {"exists": False},
        "boto3_rds.create_parameter_group": {"exists": True},
    }
    with mock_salt(rds_state, salt_map):
        ret = rds_state.parameter_present("pg", "mysql5.6", "desc")
    assert ret["result"] is True
    assert "created" in ret["comment"]


def test_parameter_present_already_no_params(mock_salt):
    salt_map = {"boto3_rds.parameter_group_exists": {"exists": True}}
    with mock_salt(rds_state, salt_map):
        ret = rds_state.parameter_present("pg", "mysql5.6", "desc")
    assert ret["result"] is True
    assert "present" in ret["comment"]


def test_parameter_present_update_changed(mock_salt):
    options = {
        "result": True,
        "parameters": {
            "back_log": {"ParameterName": "back_log", "ParameterValue": "1"},
        },
    }
    salt_map = {
        "boto3_rds.parameter_group_exists": {"exists": True},
        "boto3_rds.describe_parameters": options,
        "boto3_rds.update_parameter_group": {"results": True},
    }
    with mock_salt(rds_state, salt_map):
        ret = rds_state.parameter_present("pg", "mysql5.6", "desc", parameters=[{"back_log": 5}])
    assert ret["changes"]["Parameters"] == {"back_log": "5"}


def test_parameter_present_update_test_mode(mock_salt):
    options = {
        "result": True,
        "parameters": {
            "back_log": {"ParameterName": "back_log", "ParameterValue": "1"},
        },
    }
    salt_map = {
        "boto3_rds.parameter_group_exists": {"exists": True},
        "boto3_rds.describe_parameters": options,
    }
    with mock_salt(rds_state, salt_map, test=True):
        ret = rds_state.parameter_present("pg", "mysql5.6", "desc", parameters=[{"back_log": 5}])
    assert ret["result"] is None


def test_parameter_present_update_no_change(mock_salt):
    options = {
        "result": True,
        "parameters": {
            "back_log": {"ParameterName": "back_log", "ParameterValue": "1"},
        },
    }
    salt_map = {
        "boto3_rds.parameter_group_exists": {"exists": True},
        "boto3_rds.describe_parameters": options,
    }
    with mock_salt(rds_state, salt_map):
        ret = rds_state.parameter_present("pg", "mysql5.6", "desc", parameters=[{"back_log": 1}])
    assert ret["result"] is True
    assert "are present" in ret["comment"]


def test_parameter_present_describe_fails(mock_salt):
    salt_map = {
        "boto3_rds.parameter_group_exists": {"exists": True},
        "boto3_rds.describe_parameters": {"result": False},
    }
    with mock_salt(rds_state, salt_map):
        ret = rds_state.parameter_present("pg", "mysql5.6", "desc", parameters=[{"back_log": 5}])
    assert ret["result"] is False
