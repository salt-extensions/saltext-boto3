"""
Unit tests for the ``boto3_secgroup`` state module.
"""

from copy import deepcopy
from unittest.mock import MagicMock

import pytest
from salt.exceptions import SaltInvocationError
from salt.utils.odict import OrderedDict

from saltext.boto3.modules import boto3_secgroup as secgroup_module
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
        secgroup_module: {"__opts__": {}, "__context__": {}, "__salt__": {}},
        secgroup_state: {
            "__opts__": {"test": False},
            "__salt__": {},
        },
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


def test_all_protocol_numeric_matches_aws():
    desired = {"ip_protocol": -1, "from_port": -1, "to_port": -1, "cidr_ip": "0.0.0.0/0"}
    current = {"ip_protocol": "-1", "cidr_ip": "0.0.0.0/0"}
    assert secgroup_state._get_rule_changes([desired], [current]) == ([], [])


def test_all_protocol_without_ports_matches_aws():
    desired = {"ip_protocol": "all", "cidr_ip": "0.0.0.0/0"}
    current = {"ip_protocol": "-1", "cidr_ip": "0.0.0.0/0"}
    assert secgroup_state._get_rule_changes([desired], [current]) == ([], [])


def test_all_protocol_create_strips_ports():
    desired = {"ip_protocol": "all", "from_port": -1, "to_port": -1, "cidr_ip": "0.0.0.0/0"}
    to_delete, to_create = secgroup_state._get_rule_changes([desired], [])
    assert not to_delete
    assert to_create == [{"ip_protocol": "-1", "cidr_ip": "0.0.0.0/0"}]


def test_check_rule_missing_ports_both_sides():
    rule = {"ip_protocol": "-1", "cidr_ip": "0.0.0.0/0"}
    _rule = {"ip_protocol": "-1", "from_port": None, "to_port": None, "cidr_ip": "0.0.0.0/0"}
    assert secgroup_state._check_rule(rule, _rule)
    # must not mutate the AWS-side rule
    assert _rule["from_port"] is None
    assert _rule["to_port"] is None


def _rule(description=None):
    rule = {"ip_protocol": -1, "from_port": -1, "to_port": -1, "cidr_ip": "0.0.0.0/0"}
    if description is not None:
        rule["description"] = description
    return rule


@pytest.mark.parametrize("egress", [False, True])
@pytest.mark.parametrize("description", ["New description", ""])
@pytest.mark.parametrize("test_mode", [False, True])
def test_description_update_in_place(mock_salt, cfg, egress, description, test_mode):
    direction = "rules_egress" if egress else "rules"
    cfg[direction] = [_rule("Old description")]
    updated = deepcopy(cfg)
    updated[direction][0]["description"] = description
    get_config = MagicMock(side_effect=[cfg, updated])
    salt_map = {
        "boto3_secgroup.exists": True,
        "boto3_secgroup.get_config": get_config,
        "boto3_secgroup.update_rule_description": True,
        "boto3_secgroup.authorize": True,
        "boto3_secgroup.revoke": True,
    }
    with mock_salt(secgroup_state, salt_map, test=test_mode) as mocks:
        ret = secgroup_state.present(
            "mysg",
            "d",
            **{
                direction: [_rule(description)],
                "delete_egress_rules" if egress else "delete_ingress_rules": False,
            },
        )
        mocks["boto3_secgroup.authorize"].assert_not_called()
        mocks["boto3_secgroup.revoke"].assert_not_called()
        if test_mode:
            assert ret["result"] is None
            assert "Descriptions to be updated" in ret["comment"]
            assert not ret["changes"]
            mocks["boto3_secgroup.update_rule_description"].assert_not_called()
        else:
            assert ret["result"] is True
            mocks["boto3_secgroup.update_rule_description"].assert_called_once()
            arguments = mocks["boto3_secgroup.update_rule_description"].call_args.kwargs
            assert arguments["description"] == description
            assert arguments.get("egress", False) is egress
            assert ret["changes"]["old"][direction][0]["description"] == "Old description"
            assert ret["changes"]["new"][direction][0]["description"] == description
    with mock_salt(
        secgroup_state, {"boto3_secgroup.exists": True, "boto3_secgroup.get_config": updated}
    ):
        ret = secgroup_state.present("mysg", "d", **{direction: [_rule(description)]})
    assert ret["result"] is True
    assert not ret["changes"]


@pytest.mark.parametrize("description", [None, "Old description", "omitted"])
@pytest.mark.parametrize("egress", [False, True])
def test_description_unmanaged_or_matching(mock_salt, cfg, description, egress):
    direction = "rules_egress" if egress else "rules"
    cfg[direction] = [_rule("Old description")]
    desired = _rule(description)
    if description is None:
        desired["description"] = None
    elif description == "omitted":
        desired.pop("description")
    with mock_salt(
        secgroup_state,
        {
            "boto3_secgroup.exists": True,
            "boto3_secgroup.get_config": cfg,
        },
    ):
        ret = secgroup_state.present("mysg", "d", **{direction: [desired]})
    assert ret["result"] is True
    assert not ret["changes"]


@pytest.mark.parametrize("egress", [False, True])
def test_description_update_failure(mock_salt, cfg, egress):
    direction = "rules_egress" if egress else "rules"
    cfg[direction] = [_rule("Old description")]
    with mock_salt(
        secgroup_state,
        {
            "boto3_secgroup.exists": True,
            "boto3_secgroup.get_config": cfg,
            "boto3_secgroup.update_rule_description": False,
        },
    ):
        ret = secgroup_state.present("mysg", "d", **{direction: [_rule("New description")]})
    assert ret["result"] is False
    assert "Failed to update rule description" in ret["comment"]


@pytest.mark.parametrize("egress", [False, True])
def test_description_updates_mixed_changes(mock_salt, cfg, egress):
    direction = "rules_egress" if egress else "rules"
    current = [_rule("Old first"), _rule("Old second"), _rule("Delete me")]
    current[1]["cidr_ip"] = "10.0.0.1/32"
    current[2]["cidr_ip"] = "10.0.0.2/32"
    desired = deepcopy(current[:2])
    desired[0]["description"] = "New first"
    desired[1]["description"] = "New second"
    created = _rule("Create me")
    created["cidr_ip"] = "10.0.0.3/32"
    desired.append(created)
    cfg[direction] = current
    updated = deepcopy(cfg)
    updated[direction] = [deepcopy(desired[0]), deepcopy(current[1]), deepcopy(created)]
    with mock_salt(
        secgroup_state,
        {
            "boto3_secgroup.exists": True,
            "boto3_secgroup.get_config": MagicMock(side_effect=[cfg, updated]),
            "boto3_secgroup.update_rule_description": MagicMock(side_effect=[True, False]),
            "boto3_secgroup.authorize": True,
            "boto3_secgroup.revoke": True,
        },
    ) as mocks:
        ret = secgroup_state.present("mysg", "d", **{direction: desired})
        assert mocks["boto3_secgroup.update_rule_description"].call_count == 2
        mocks["boto3_secgroup.authorize"].assert_called_once()
        mocks["boto3_secgroup.revoke"].assert_called_once()
    assert ret["result"] is False
    assert "Failed to update rule description" in ret["comment"]
    assert "Updated rule description" in ret["comment"]
    assert ret["changes"]["old"][direction] == current
    assert ret["changes"]["new"][direction] == updated[direction]


def test_description_updates_split_cidrs_and_groups():
    desired = _rule("New description")
    desired["cidr_ip"] = ["10.0.0.1/32", "10.0.0.2/32"]
    split = secgroup_state._split_rules([desired])
    current = [dict(rule, description="Old description") for rule in split]
    assert secgroup_state._get_description_updates(split, current) == split
    group_rule = {
        "ip_protocol": "tcp",
        "from_port": 22,
        "to_port": 22,
        "source_group_group_id": "sg-2",
        "description": "New description",
    }
    assert secgroup_state._get_description_updates(
        [group_rule], [dict(group_rule, description="Old description")]
    ) == [group_rule]


@pytest.mark.parametrize("description", [False, 123, [], {}])
@pytest.mark.parametrize("egress", [False, True])
def test_invalid_description_before_group_creation(mock_salt, description, egress):
    direction = "rules_egress" if egress else "rules"
    with mock_salt(secgroup_state, {"boto3_secgroup.create": True}) as mocks:
        with pytest.raises(SaltInvocationError, match="description must be a string"):
            secgroup_state.present("mysg", "d", **{direction: [_rule(description)]})
        mocks["boto3_secgroup.create"].assert_not_called()


@pytest.mark.parametrize("egress", [False, True])
def test_state_creation_real_authorize(mock_salt, make_conn, cfg, egress):
    direction = "rules_egress" if egress else "rules"
    with make_conn(secgroup_module) as conn:
        conn.describe_security_groups.return_value = {"SecurityGroups": [{"GroupId": "sg-1"}]}
        with mock_salt(
            secgroup_state,
            {
                "boto3_secgroup.exists": True,
                "boto3_secgroup.get_config": cfg,
                "boto3_secgroup.authorize": MagicMock(side_effect=secgroup_module.authorize),
            },
        ):
            ret = secgroup_state.present("mysg", "d", **{direction: [_rule("Outbound traffic")]})
        operation = (
            conn.authorize_security_group_egress
            if egress
            else conn.authorize_security_group_ingress
        )
        operation.assert_called_once()
        assert (
            operation.call_args.kwargs["IpPermissions"][0]["IpRanges"][0]["Description"]
            == "Outbound traffic"
        )
    assert ret["result"] is True


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
