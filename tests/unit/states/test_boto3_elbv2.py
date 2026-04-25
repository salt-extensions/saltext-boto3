"""
Unit tests for the ``boto3_elbv2`` state module.
"""

from unittest.mock import MagicMock

import pytest

from saltext.boto3.states import boto3_elbv2 as elbv2_state

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
    return {elbv2_state: {"__opts__": {"test": False}, "__salt__": {}}}


def test_virtual(mock_salt):
    with mock_salt(elbv2_state, {"boto3_elbv2.target_group_exists": True}):
        assert elbv2_state.__virtual__() == "boto3_elbv2"


def test_virtual_no_exec_module(mock_salt):
    with mock_salt(elbv2_state, {}):
        result = elbv2_state.__virtual__()
    assert result[0] is False
    assert "boto3_elbv2" in result[1]


def test_create_target_group_already_exists(mock_salt):
    with mock_salt(elbv2_state, {"boto3_elbv2.target_group_exists": True}):
        ret = elbv2_state.create_target_group("tg", "HTTP", 80, "vpc-1")
    assert ret["result"] is True
    assert "already exists" in ret["comment"]


def test_create_target_group_test_mode(mock_salt):
    salt_map = {
        "boto3_elbv2.target_group_exists": False,
        "boto3_elbv2.create_target_group": True,
    }
    with mock_salt(elbv2_state, salt_map, test=True):
        ret = elbv2_state.create_target_group("tg", "HTTP", 80, "vpc-1")
    assert ret["result"] is None
    assert "will be created" in ret["comment"]


def test_create_target_group_creates(mock_salt):
    create = MagicMock(return_value=True)
    salt_map = {
        "boto3_elbv2.target_group_exists": False,
        "boto3_elbv2.create_target_group": create,
    }
    with mock_salt(elbv2_state, salt_map):
        ret = elbv2_state.create_target_group("tg", "HTTP", 80, "vpc-1")
    assert ret["result"] is True
    assert ret["changes"]["target_group"] == "tg"
    create.assert_called_once()


def test_create_target_group_failure(mock_salt):
    salt_map = {
        "boto3_elbv2.target_group_exists": False,
        "boto3_elbv2.create_target_group": False,
    }
    with mock_salt(elbv2_state, salt_map):
        ret = elbv2_state.create_target_group("tg", "HTTP", 80, "vpc-1")
    assert ret["result"] is False
    assert "creation failed" in ret["comment"]


def test_delete_target_group_absent(mock_salt):
    with mock_salt(elbv2_state, {"boto3_elbv2.target_group_exists": False}):
        ret = elbv2_state.delete_target_group("tg")
    assert ret["result"] is True
    assert "does not exists" in ret["comment"]


def test_delete_target_group_test_mode(mock_salt):
    with mock_salt(elbv2_state, {"boto3_elbv2.target_group_exists": True}, test=True):
        ret = elbv2_state.delete_target_group("tg")
    assert ret["result"] is None
    assert "will be deleted" in ret["comment"]


def test_delete_target_group_deletes(mock_salt):
    delete = MagicMock(return_value=True)
    salt_map = {
        "boto3_elbv2.target_group_exists": True,
        "boto3_elbv2.delete_target_group": delete,
    }
    with mock_salt(elbv2_state, salt_map):
        ret = elbv2_state.delete_target_group("tg")
    assert ret["result"] is True
    assert ret["changes"]["target_group"] == "tg"


def test_delete_target_group_failure(mock_salt):
    salt_map = {
        "boto3_elbv2.target_group_exists": True,
        "boto3_elbv2.delete_target_group": False,
    }
    with mock_salt(elbv2_state, salt_map):
        ret = elbv2_state.delete_target_group("tg")
    assert ret["result"] is False
    assert "deletion failed" in ret["comment"]


def test_targets_registered_missing_group(mock_salt):
    with mock_salt(elbv2_state, {"boto3_elbv2.target_group_exists": False}):
        ret = elbv2_state.targets_registered("tg", ["i-1"])
    assert "Could not find" in ret["comment"]


def test_targets_registered_already_present(mock_salt):
    salt_map = {
        "boto3_elbv2.target_group_exists": True,
        "boto3_elbv2.describe_target_health": {"i-1": "healthy"},
    }
    with mock_salt(elbv2_state, salt_map):
        ret = elbv2_state.targets_registered("tg", "i-1")
    assert ret["result"] is True
    assert "already registered" in ret["comment"]


def test_targets_registered_registers(mock_salt):
    describe = MagicMock(side_effect=[{"i-1": "draining"}, {"i-1": "initial"}])
    register = MagicMock(return_value=True)
    salt_map = {
        "boto3_elbv2.target_group_exists": True,
        "boto3_elbv2.describe_target_health": describe,
        "boto3_elbv2.register_targets": register,
    }
    with mock_salt(elbv2_state, salt_map):
        ret = elbv2_state.targets_registered("tg", ["i-1"])
    assert ret["result"] is True
    assert ret["changes"]["old"] == {"i-1": "draining"}
    assert ret["changes"]["new"] == {"i-1": "initial"}
    register.assert_called_once()


def test_targets_registered_test_mode(mock_salt):
    salt_map = {
        "boto3_elbv2.target_group_exists": True,
        "boto3_elbv2.describe_target_health": {},
    }
    with mock_salt(elbv2_state, salt_map, test=True):
        ret = elbv2_state.targets_registered("tg", ["i-1"])
    assert ret["result"] is None
    assert ret["changes"]["new"] == {"i-1": "initial"}


def test_targets_deregistered_missing_group(mock_salt):
    with mock_salt(elbv2_state, {"boto3_elbv2.target_group_exists": False}):
        ret = elbv2_state.targets_deregistered("tg", ["i-1"])
    assert "Could not find" in ret["comment"]


def test_targets_deregistered_already_absent(mock_salt):
    salt_map = {
        "boto3_elbv2.target_group_exists": True,
        "boto3_elbv2.describe_target_health": {},
    }
    with mock_salt(elbv2_state, salt_map):
        ret = elbv2_state.targets_deregistered("tg", "i-1")
    assert ret["result"] is True
    assert "already deregistered" in ret["comment"]


def test_targets_deregistered_deregisters(mock_salt):
    describe = MagicMock(side_effect=[{"i-1": "healthy"}, {"i-1": "draining"}])
    deregister = MagicMock(return_value=True)
    salt_map = {
        "boto3_elbv2.target_group_exists": True,
        "boto3_elbv2.describe_target_health": describe,
        "boto3_elbv2.deregister_targets": deregister,
    }
    with mock_salt(elbv2_state, salt_map):
        ret = elbv2_state.targets_deregistered("tg", ["i-1"])
    assert ret["result"] is True
    assert ret["changes"]["old"] == {"i-1": "healthy"}
    assert ret["changes"]["new"] == {"i-1": "draining"}


def test_targets_deregistered_test_mode(mock_salt):
    salt_map = {
        "boto3_elbv2.target_group_exists": True,
        "boto3_elbv2.describe_target_health": {"i-1": "healthy"},
    }
    with mock_salt(elbv2_state, salt_map, test=True):
        ret = elbv2_state.targets_deregistered("tg", ["i-1"])
    assert ret["result"] is None
    assert ret["changes"]["new"] == {"i-1": "draining"}
