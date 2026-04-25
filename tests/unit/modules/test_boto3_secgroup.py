"""
Unit tests for the ``boto3_secgroup`` execution module.
"""

from unittest.mock import MagicMock

import pytest
from salt.exceptions import CommandExecutionError
from salt.exceptions import SaltInvocationError

from saltext.boto3.modules import boto3_secgroup

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
        boto3_secgroup: {
            "__opts__": {"test": False},
            "__context__": {},
            "__salt__": {},
        }
    }


@pytest.fixture
def conn(make_conn):
    with make_conn(boto3_secgroup) as client:
        yield client


def _sg(
    group_id="sg-1",
    name="mysg",
    vpc_id=None,
    description="d",
    owner="123",
    tags=None,
    ingress=None,
    egress=None,
):
    return {
        "GroupId": group_id,
        "GroupName": name,
        "Description": description,
        "OwnerId": owner,
        "VpcId": vpc_id,
        "Tags": tags or [],
        "IpPermissions": ingress or [],
        "IpPermissionsEgress": egress or [],
    }


@pytest.fixture
def sg_exists(conn):
    conn.describe_security_groups.return_value = {"SecurityGroups": [_sg()]}
    return conn


@pytest.fixture
def sg_missing(conn):
    conn.describe_security_groups.return_value = {"SecurityGroups": []}
    return conn


@pytest.mark.usefixtures("sg_exists")
def test_exists():
    assert boto3_secgroup.exists(name="mysg") is True


def test_exists_client_error(conn, client_error):
    conn.describe_security_groups.side_effect = client_error(
        "AuthFailure", "DescribeSecurityGroups"
    )
    assert boto3_secgroup.exists(name="mysg") is False


def test_parse_ip_permissions_splits_grants():
    perms = [
        {
            "IpProtocol": "tcp",
            "FromPort": 80,
            "ToPort": 80,
            "IpRanges": [{"CidrIp": "10.0.0.0/8"}, {"CidrIp": "192.168.0.0/16"}],
            "UserIdGroupPairs": [{"GroupId": "sg-2", "UserId": "123"}],
        }
    ]
    rules = boto3_secgroup._parse_ip_permissions(perms)
    assert len(rules) == 3
    assert rules[0]["cidr_ip"] == "10.0.0.0/8"
    assert rules[2]["source_group_group_id"] == "sg-2"
    assert rules[2]["source_group_owner_id"] == "123"


def test_parse_ip_permissions_empty():
    assert not boto3_secgroup._parse_ip_permissions(None)


def test_get_all_security_groups(conn):
    conn.describe_security_groups.return_value = {
        "SecurityGroups": [
            _sg(
                tags=[{"Key": "Env", "Value": "prod"}],
                ingress=[
                    {
                        "IpProtocol": "tcp",
                        "FromPort": 22,
                        "ToPort": 22,
                        "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
                    }
                ],
            )
        ]
    }
    out = boto3_secgroup.get_all_security_groups(filters={"group-name": "mysg"})
    assert len(out) == 1
    assert out[0]["id"] == "sg-1"
    assert out[0]["tags"] == {"Env": "prod"}
    assert out[0]["rules"][0]["cidr_ip"] == "0.0.0.0/0"


def test_get_all_security_groups_client_error(conn, client_error):
    conn.describe_security_groups.side_effect = client_error(
        "AuthFailure", "DescribeSecurityGroups"
    )
    assert not boto3_secgroup.get_all_security_groups()


@pytest.mark.usefixtures("sg_exists")
def test_get_group_id():
    assert boto3_secgroup.get_group_id("mysg") == "sg-1"


def test_get_group_id_pass_through_sg_prefix(conn):
    assert boto3_secgroup.get_group_id("sg-abc") == "sg-abc"
    conn.describe_security_groups.assert_not_called()


@pytest.mark.usefixtures("sg_missing")
def test_get_group_id_not_found():
    assert boto3_secgroup.get_group_id("mysg") is None


@pytest.mark.usefixtures("sg_exists")
def test_convert_to_group_ids():
    assert boto3_secgroup.convert_to_group_ids(["mysg"]) == ["sg-1"]


@pytest.mark.usefixtures("sg_missing")
def test_convert_to_group_ids_missing_raises():
    with pytest.raises(CommandExecutionError):
        boto3_secgroup.convert_to_group_ids(["mysg"])


@pytest.mark.usefixtures("sg_missing")
def test_convert_to_group_ids_missing_test_mode_returns_empty():
    boto3_secgroup.__opts__["test"] = True
    try:
        assert not boto3_secgroup.convert_to_group_ids(["mysg"])
    finally:
        boto3_secgroup.__opts__["test"] = False


def test_get_config(conn):
    conn.describe_security_groups.return_value = {
        "SecurityGroups": [
            _sg(
                tags=[{"Key": "Env", "Value": "prod"}],
                egress=[{"IpProtocol": "-1", "IpRanges": [{"CidrIp": "0.0.0.0/0"}]}],
            )
        ]
    }
    cfg = boto3_secgroup.get_config(name="mysg")
    assert cfg["group_id"] == "sg-1"
    assert cfg["tags"] == {"Env": "prod"}
    assert cfg["rules_egress"][0]["cidr_ip"] == "0.0.0.0/0"


@pytest.mark.usefixtures("sg_missing")
def test_get_config_missing():
    assert boto3_secgroup.get_config(name="missing") is None


def test_get_config_mutex_raises():
    with pytest.raises(SaltInvocationError):
        boto3_secgroup.get_config(name="x", vpc_id="v", vpc_name="n")


def test_create(conn):
    conn.create_security_group.return_value = {"GroupId": "sg-new"}
    assert boto3_secgroup.create("mysg", "d") is True


def test_create_client_error(conn, client_error):
    conn.create_security_group.side_effect = client_error(
        "InvalidGroup.Duplicate", "CreateSecurityGroup"
    )
    assert boto3_secgroup.create("mysg", "d") is False


@pytest.mark.usefixtures("sg_exists")
def test_delete(conn):
    conn.delete_security_group.return_value = {}
    assert boto3_secgroup.delete(name="mysg") is True


@pytest.mark.usefixtures("sg_missing")
def test_delete_not_found():
    assert boto3_secgroup.delete(name="missing") is False


@pytest.mark.usefixtures("sg_exists")
def test_delete_client_error(conn, client_error):
    conn.delete_security_group.side_effect = client_error(
        "DependencyViolation", "DeleteSecurityGroup"
    )
    assert boto3_secgroup.delete(name="mysg") is False


@pytest.mark.usefixtures("sg_exists")
def test_authorize(conn):
    conn.authorize_security_group_ingress.return_value = {}
    assert (
        boto3_secgroup.authorize(
            name="mysg", ip_protocol="tcp", from_port=80, to_port=80, cidr_ip="0.0.0.0/0"
        )
        is True
    )


@pytest.mark.usefixtures("sg_exists")
def test_authorize_duplicate_returns_true(conn, client_error):
    conn.authorize_security_group_ingress.side_effect = client_error(
        "InvalidPermission.Duplicate", "AuthorizeSecurityGroupIngress"
    )
    assert (
        boto3_secgroup.authorize(
            name="mysg", ip_protocol="tcp", from_port=80, to_port=80, cidr_ip="0.0.0.0/0"
        )
        is True
    )


@pytest.mark.usefixtures("sg_exists")
def test_authorize_egress_uses_egress_api(conn):
    conn.authorize_security_group_egress.return_value = {}
    assert (
        boto3_secgroup.authorize(
            name="mysg",
            ip_protocol="tcp",
            from_port=80,
            to_port=80,
            cidr_ip="0.0.0.0/0",
            egress=True,
        )
        is True
    )
    conn.authorize_security_group_egress.assert_called_once()


@pytest.mark.usefixtures("sg_missing")
def test_authorize_group_missing():
    assert boto3_secgroup.authorize(name="mysg", ip_protocol="tcp") is False


@pytest.mark.usefixtures("sg_exists")
def test_revoke(conn):
    conn.revoke_security_group_ingress.return_value = {}
    assert (
        boto3_secgroup.revoke(
            name="mysg", ip_protocol="tcp", from_port=80, to_port=80, cidr_ip="0.0.0.0/0"
        )
        is True
    )


@pytest.mark.usefixtures("sg_exists")
def test_revoke_client_error(conn, client_error):
    conn.revoke_security_group_ingress.side_effect = client_error(
        "InvalidPermission.NotFound", "RevokeSecurityGroupIngress"
    )
    assert (
        boto3_secgroup.revoke(
            name="mysg", ip_protocol="tcp", from_port=80, to_port=80, cidr_ip="0.0.0.0/0"
        )
        is False
    )


@pytest.mark.usefixtures("sg_exists")
def test_set_tags(conn):
    conn.create_tags.return_value = {}
    assert boto3_secgroup.set_tags({"a": "b"}, name="mysg") is True


def test_set_tags_rejects_non_dict():
    with pytest.raises(SaltInvocationError):
        boto3_secgroup.set_tags(["a"], name="mysg")


@pytest.mark.usefixtures("sg_missing")
def test_set_tags_group_missing():
    with pytest.raises(SaltInvocationError):
        boto3_secgroup.set_tags({"a": "b"}, name="missing")


@pytest.mark.usefixtures("sg_exists")
def test_set_tags_client_error(conn, client_error):
    conn.create_tags.side_effect = client_error("AuthFailure", "CreateTags")
    assert boto3_secgroup.set_tags({"a": "b"}, name="mysg") is False


@pytest.mark.usefixtures("sg_exists")
def test_delete_tags(conn):
    conn.delete_tags.return_value = {}
    assert boto3_secgroup.delete_tags(["a"], name="mysg") is True


def test_delete_tags_rejects_non_list():
    with pytest.raises(SaltInvocationError):
        boto3_secgroup.delete_tags({"a": "b"}, name="mysg")


@pytest.mark.usefixtures("sg_exists")
def test_delete_tags_client_error(conn, client_error):
    conn.delete_tags.side_effect = client_error("AuthFailure", "DeleteTags")
    assert boto3_secgroup.delete_tags(["a"], name="mysg") is False


def test_get_group_vpc_name_resolves_via_salt(conn):
    conn.describe_security_groups.return_value = {"SecurityGroups": [_sg(vpc_id="vpc-1")]}
    boto3_secgroup.__salt__["boto3_vpc.get_id"] = MagicMock(return_value={"id": "vpc-1"})
    try:
        assert boto3_secgroup.exists(name="mysg", vpc_name="myvpc") is True
    finally:
        boto3_secgroup.__salt__.clear()


def test_get_group_vpc_name_unresolved(conn):
    boto3_secgroup.__salt__["boto3_vpc.get_id"] = MagicMock(return_value={"id": None})
    try:
        assert boto3_secgroup.exists(name="mysg", vpc_name="myvpc") is False
    finally:
        boto3_secgroup.__salt__.clear()
    conn.describe_security_groups.assert_not_called()
