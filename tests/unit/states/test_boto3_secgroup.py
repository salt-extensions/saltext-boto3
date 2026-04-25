"""
Unit tests for the ``boto3_secgroup`` state module.
"""

import pytest
from salt.utils.odict import OrderedDict

from saltext.boto3.states import boto3_secgroup as secgroup_state

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
        secgroup_state: {
            "__opts__": {"test": False},
            "__salt__": {},
        }
    }


@pytest.fixture
def cfg():
    return {
        "name": "mysg",
        "group_id": "sg-1",
        "description": "d",
        "owner_id": "123",
        "tags": {},
        "rules": [],
        "rules_egress": [],
    }


def test_virtual(mock_salt):
    with mock_salt(secgroup_state, {"boto3_secgroup.exists": True}):
        assert secgroup_state.__virtual__() == "boto3_secgroup"


def test_virtual_no_exec_module(mock_salt):
    with mock_salt(secgroup_state, {}):
        result = secgroup_state.__virtual__()
    assert result[0] is False


def test_get_rule_changes_no_change():
    assert secgroup_state._get_rule_changes([], []) == ([], [])


def test_get_rule_changes_create():
    present = [
        OrderedDict(
            [("ip_protocol", "tcp"), ("from_port", 22), ("to_port", 22), ("cidr_ip", "0.0.0.0/0")]
        )
    ]
    desired = [
        present[0],
        OrderedDict(
            [("ip_protocol", "tcp"), ("from_port", 80), ("to_port", 80), ("cidr_ip", "0.0.0.0/0")]
        ),
    ]
    to_delete, to_create = secgroup_state._get_rule_changes(desired, present)
    assert not to_delete
    assert len(to_create) == 1
    assert to_create[0]["from_port"] == 80


def test_get_rule_changes_delete():
    present = [
        OrderedDict(
            [("ip_protocol", "tcp"), ("from_port", 22), ("to_port", 22), ("cidr_ip", "0.0.0.0/0")]
        ),
        OrderedDict(
            [("ip_protocol", "tcp"), ("from_port", 80), ("to_port", 80), ("cidr_ip", "0.0.0.0/0")]
        ),
    ]
    desired = [present[0]]
    to_delete, to_create = secgroup_state._get_rule_changes(desired, present)
    assert not to_create
    assert len(to_delete) == 1
    assert to_delete[0]["from_port"] == 80


def test_get_rule_changes_invalid_protocol():
    with pytest.raises(Exception):
        secgroup_state._get_rule_changes(
            [{"ip_protocol": "bogus", "from_port": 1, "to_port": 1, "cidr_ip": "0.0.0.0/0"}],
            [],
        )


def test_present_creates_new(mock_salt, cfg):
    salt_map = {
        "boto3_secgroup.exists": False,
        "boto3_secgroup.create": True,
        "boto3_secgroup.get_config": cfg,
    }
    with mock_salt(secgroup_state, salt_map):
        ret = secgroup_state.present("mysg", "d")
    assert ret["result"] is True
    assert "created" in ret["comment"].lower()


def test_present_already_exists_no_rules_change(mock_salt, cfg):
    salt_map = {
        "boto3_secgroup.exists": True,
        "boto3_secgroup.get_config": cfg,
    }
    with mock_salt(secgroup_state, salt_map):
        ret = secgroup_state.present("mysg", "d", rules=[], rules_egress=[])
    assert ret["result"] is True
    assert not ret["changes"]


def test_present_test_mode_reports_create(mock_salt):
    with mock_salt(secgroup_state, {"boto3_secgroup.exists": False}, test=True):
        ret = secgroup_state.present("mysg", "d")
    assert ret["result"] is None
    assert "set to be created" in ret["comment"]


def test_present_create_failure(mock_salt):
    salt_map = {
        "boto3_secgroup.exists": False,
        "boto3_secgroup.create": False,
        "boto3_secgroup.get_config": None,
    }
    with mock_salt(secgroup_state, salt_map):
        ret = secgroup_state.present("mysg", "d")
    assert ret["result"] is False


def test_absent_already_gone(mock_salt):
    with mock_salt(secgroup_state, {"boto3_secgroup.get_config": None}):
        ret = secgroup_state.absent("mysg")
    assert ret["result"] is True
    assert "does not exist" in ret["comment"]


def test_absent_deletes(mock_salt, cfg):
    salt_map = {
        "boto3_secgroup.get_config": cfg,
        "boto3_secgroup.delete": True,
    }
    with mock_salt(secgroup_state, salt_map):
        ret = secgroup_state.absent("mysg")
    assert ret["result"] is True
    assert "deleted" in ret["comment"].lower()


def test_absent_test_mode(mock_salt, cfg):
    with mock_salt(secgroup_state, {"boto3_secgroup.get_config": cfg}, test=True):
        ret = secgroup_state.absent("mysg")
    assert ret["result"] is None


def test_absent_delete_failure(mock_salt, cfg):
    salt_map = {
        "boto3_secgroup.get_config": cfg,
        "boto3_secgroup.delete": False,
    }
    with mock_salt(secgroup_state, salt_map):
        ret = secgroup_state.absent("mysg")
    assert ret["result"] is False
