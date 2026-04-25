"""
Unit tests for the ``boto3_asg`` state module.
"""

from unittest.mock import MagicMock

import pytest
from salt.exceptions import SaltInvocationError

from saltext.boto3.states import boto3_asg as asg_state

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
        asg_state: {
            "__opts__": {"test": False},
            "__salt__": {},
            "__states__": {},
        }
    }


def _with_defaults(salt_map):
    salt_map.setdefault(
        "config.option",
        MagicMock(side_effect=lambda *a, **k: {} if len(a) < 2 else a[1]),
    )
    salt_map.setdefault("boto3_vpc.get_subnet_association", {"vpc_id": None})
    return salt_map


def test_virtual(mock_salt):
    with mock_salt(asg_state, {"boto3_asg.exists": True}):
        assert asg_state.__virtual__() == "boto3_asg"


def test_virtual_no_exec_module(mock_salt):
    with mock_salt(asg_state, {}):
        result = asg_state.__virtual__()
    assert result[0] is False


def test_present_create_test_mode(mock_salt):
    salt_map = _with_defaults({"boto3_asg.get_config": {}})
    with mock_salt(asg_state, salt_map, test=True):
        result = asg_state.present(
            name="myasg",
            launch_config_name="mylc",
            availability_zones=["us-east-1a"],
            min_size=1,
            max_size=3,
        )
    assert result["result"] is None
    assert "set to be created" in result["comment"]


def test_present_creates_when_missing(mock_salt):
    created_asg = {
        "name": "myasg",
        "launch_config_name": "mylc",
        "min_size": 1,
        "max_size": 3,
        "availability_zones": ["us-east-1a"],
        "default_cooldown": 300,
        "health_check_type": "EC2",
        "health_check_period": 60,
        "vpc_zone_identifier": [],
        "tags": [],
        "termination_policies": ["Default"],
        "suspended_processes": [],
        "scaling_policies": [],
        "scheduled_actions": {},
    }
    salt_map = _with_defaults(
        {
            "boto3_asg.get_config": MagicMock(side_effect=[{}, created_asg]),
            "boto3_asg.create": True,
        }
    )
    with mock_salt(asg_state, salt_map):
        result = asg_state.present(
            name="myasg",
            launch_config_name="mylc",
            availability_zones=["us-east-1a"],
            min_size=1,
            max_size=3,
        )
    assert result["result"] is True
    assert result["changes"]["old"] is None
    assert result["changes"]["new"] == created_asg


def test_present_create_failure(mock_salt):
    salt_map = _with_defaults(
        {
            "boto3_asg.get_config": {},
            "boto3_asg.create": False,
        }
    )
    with mock_salt(asg_state, salt_map):
        result = asg_state.present(
            name="myasg",
            launch_config_name="mylc",
            availability_zones=["us-east-1a"],
            min_size=1,
            max_size=3,
        )
    assert result["result"] is False
    assert "Failed to create" in result["comment"]


def test_present_existing_no_change(mock_salt):
    existing = {
        "name": "myasg",
        "launch_config_name": "mylc",
        "min_size": 1,
        "max_size": 3,
        "availability_zones": ["us-east-1a"],
        "default_cooldown": 300,
        "health_check_type": "EC2",
        "health_check_period": 60,
        "vpc_zone_identifier": [],
        "tags": [],
        "termination_policies": ["Default"],
        "suspended_processes": [],
        "scaling_policies": [],
        "scheduled_actions": {},
    }
    salt_map = _with_defaults({"boto3_asg.get_config": existing})
    with mock_salt(asg_state, salt_map):
        result = asg_state.present(
            name="myasg",
            launch_config_name="mylc",
            availability_zones=["us-east-1a"],
            min_size=1,
            max_size=3,
        )
    assert result["result"] is True
    assert not result["changes"]
    assert "present" in result["comment"]


def test_present_update_test_mode(mock_salt):
    existing = {
        "name": "myasg",
        "launch_config_name": "mylc",
        "min_size": 1,
        "max_size": 3,
        "availability_zones": ["us-east-1a"],
        "termination_policies": ["Default"],
        "suspended_processes": [],
        "scaling_policies": [],
        "scheduled_actions": {},
    }
    salt_map = _with_defaults({"boto3_asg.get_config": existing})
    with mock_salt(asg_state, salt_map, test=True):
        result = asg_state.present(
            name="myasg",
            launch_config_name="mylc",
            availability_zones=["us-east-1a"],
            min_size=1,
            max_size=5,
        )
    assert result["result"] is None
    assert "set to be updated" in result["comment"]
    assert result["changes"]["old"]["max_size"] == 3
    assert result["changes"]["new"]["max_size"] == 5


def test_present_update_apply(mock_salt):
    existing = {
        "name": "myasg",
        "launch_config_name": "mylc",
        "min_size": 1,
        "max_size": 3,
        "availability_zones": ["us-east-1a"],
        "termination_policies": ["Default"],
        "suspended_processes": [],
        "scaling_policies": [],
        "scheduled_actions": {},
    }
    updated = dict(existing, max_size=5)
    salt_map = _with_defaults(
        {
            "boto3_asg.get_config": MagicMock(side_effect=[existing, updated]),
            "boto3_asg.update": MagicMock(return_value=(True, "")),
        }
    )
    with mock_salt(asg_state, salt_map):
        result = asg_state.present(
            name="myasg",
            launch_config_name="mylc",
            availability_zones=["us-east-1a"],
            min_size=1,
            max_size=5,
        )
    assert result["result"] is True
    assert result["changes"]["old"] == existing
    assert result["changes"]["new"] == updated
    assert "Updated" in result["comment"]


def test_present_update_deletes_old_lc_when_name_changes(mock_salt):
    existing = {
        "name": "myasg",
        "launch_config_name": "oldlc",
        "min_size": 1,
        "max_size": 3,
        "availability_zones": ["us-east-1a"],
        "termination_policies": ["Default"],
        "suspended_processes": [],
        "scaling_policies": [],
        "scheduled_actions": {},
    }
    updated = dict(existing, launch_config_name="newlc")
    delete_lc = MagicMock(return_value=True)
    salt_map = _with_defaults(
        {
            "boto3_asg.get_config": MagicMock(side_effect=[existing, updated]),
            "boto3_asg.update": MagicMock(return_value=(True, "")),
            "boto3_asg.delete_launch_configuration": delete_lc,
        }
    )
    with mock_salt(asg_state, salt_map):
        result = asg_state.present(
            name="myasg",
            launch_config_name="newlc",
            availability_zones=["us-east-1a"],
            min_size=1,
            max_size=3,
        )
    assert result["result"] is True
    assert result["changes"]["launch_config"]["deleted"] == "oldlc"
    delete_lc.assert_called_once()


def test_present_vpc_and_subnet_names_mutually_exclusive(mock_salt):
    with mock_salt(asg_state, _with_defaults({})):
        with pytest.raises(SaltInvocationError):
            asg_state.present(
                name="myasg",
                launch_config_name="mylc",
                availability_zones=["us-east-1a"],
                min_size=1,
                max_size=3,
                vpc_zone_identifier=["subnet-1"],
                subnet_names=["my-subnet"],
            )


def test_absent_not_found(mock_salt):
    with mock_salt(asg_state, _with_defaults({"boto3_asg.get_config": {}})):
        result = asg_state.absent("myasg")
    assert result["result"] is True
    assert "does not exist" in result["comment"]


def test_absent_test_mode(mock_salt):
    existing = {"name": "myasg", "launch_config_name": "mylc"}
    salt_map = _with_defaults({"boto3_asg.get_config": existing})
    with mock_salt(asg_state, salt_map, test=True):
        result = asg_state.absent("myasg", remove_lc=True)
    assert result["result"] is None
    assert "set to be deleted" in result["comment"]
    assert "Launch configuration mylc" in result["comment"]


def test_absent_deletes(mock_salt):
    existing = {"name": "myasg", "launch_config_name": "mylc"}
    salt_map = _with_defaults({"boto3_asg.get_config": existing, "boto3_asg.delete": True})
    with mock_salt(asg_state, salt_map):
        result = asg_state.absent("myasg", force=True)
    assert result["result"] is True
    assert result["changes"]["old"] == existing
    assert result["changes"]["new"] is None


def test_absent_delete_failure(mock_salt):
    existing = {"name": "myasg", "launch_config_name": "mylc"}
    salt_map = _with_defaults({"boto3_asg.get_config": existing, "boto3_asg.delete": False})
    with mock_salt(asg_state, salt_map):
        result = asg_state.absent("myasg")
    assert result["result"] is False
    assert "Failed to delete" in result["comment"]


def test_absent_deletes_launch_config_when_requested(mock_salt):
    existing = {"name": "myasg", "launch_config_name": "mylc"}
    salt_map = _with_defaults(
        {
            "boto3_asg.get_config": existing,
            "boto3_asg.delete": True,
            "boto3_asg.delete_launch_configuration": True,
        }
    )
    with mock_salt(asg_state, salt_map):
        result = asg_state.absent("myasg", remove_lc=True)
    assert result["result"] is True
    assert result["changes"]["launch_config"]["deleted"] == "mylc"
