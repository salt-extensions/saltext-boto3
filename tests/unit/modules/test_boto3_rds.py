"""
Unit tests for the ``boto3_rds`` execution module.
"""

from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from salt.exceptions import SaltInvocationError

from saltext.boto3.modules import boto3_rds

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
        boto3_rds: {
            "__opts__": {},
            "__context__": {},
            "__salt__": {},
        }
    }


@pytest.fixture
def conn(make_conn):
    with make_conn(boto3_rds) as client:
        yield client


def test_exists_true(conn):
    conn.describe_db_instances.return_value = {"DBInstances": [{"x": 1}]}
    assert boto3_rds.exists("r") == {"exists": True}


def test_exists_client_error(conn, client_error):
    conn.describe_db_instances.side_effect = client_error("Err", "DescribeDBInstances")
    ret = boto3_rds.exists("r")
    assert "error" in ret


def test_option_group_exists_true(conn):
    conn.describe_option_groups.return_value = {"OptionGroupsList": [{}]}
    assert boto3_rds.option_group_exists("og") == {"exists": True}


def test_option_group_exists_error(conn, client_error):
    conn.describe_option_groups.side_effect = client_error("Err", "DescribeOptionGroups")
    assert "error" in boto3_rds.option_group_exists("og")


def test_parameter_group_exists_true(conn):
    conn.describe_db_parameter_groups.return_value = {"DBParameterGroups": [{}]}
    ret = boto3_rds.parameter_group_exists("pg")
    assert ret["exists"] is True


def test_parameter_group_exists_not_found(conn, client_error):
    conn.describe_db_parameter_groups.side_effect = client_error(
        "DBParameterGroupNotFound", "DescribeDBParameterGroups"
    )
    ret = boto3_rds.parameter_group_exists("pg")
    assert ret["exists"] is False


def test_subnet_group_exists_true(conn):
    conn.describe_db_subnet_groups.return_value = {"DBSubnetGroups": [{}]}
    assert boto3_rds.subnet_group_exists("sg") == {"exists": True}


def _create_args(**over):
    args = {
        "name": "r",
        "allocated_storage": 10,
        "db_instance_class": "db.t2.micro",
        "engine": "mysql",
        "master_username": "u",
        "master_user_password": "p",
    }
    args.update(over)
    return args


def test_create_happy(conn):
    conn.create_db_instance.return_value = {"DBInstance": {}}
    ret = boto3_rds.create(**_create_args())
    assert ret["created"] is True
    kw = conn.create_db_instance.call_args.kwargs
    assert kw["DBInstanceIdentifier"] == "r"
    assert kw["AllocatedStorage"] == 10
    assert kw["Engine"] == "mysql"


def test_create_missing_required():
    with pytest.raises(SaltInvocationError):
        boto3_rds.create(
            name="r",
            allocated_storage=0,
            db_instance_class="db.t2.micro",
            engine="mysql",
            master_username="u",
            master_user_password="p",
        )


def test_create_az_and_multi_az_mutually_exclusive():
    with pytest.raises(SaltInvocationError):
        boto3_rds.create(**_create_args(availability_zone="us-east-1a", multi_az=True))


def test_create_bad_wait_status():
    with pytest.raises(SaltInvocationError):
        boto3_rds.create(**_create_args(wait_status="bogus"))


def test_create_client_error(conn, client_error):
    conn.create_db_instance.side_effect = client_error("Err", "CreateDBInstance")
    ret = boto3_rds.create(**_create_args())
    assert "error" in ret


def test_create_wait_status_reached(conn):
    conn.create_db_instance.return_value = {"DBInstance": {}}
    with patch.object(
        boto3_rds,
        "describe_db_instances",
        return_value=["available"],
    ):
        ret = boto3_rds.create(**_create_args(wait_status="available"))
    assert ret["created"] is True
    assert "available" in ret["message"]


def test_create_option_group(conn):
    conn.create_option_group.return_value = {"OptionGroup": {}}
    with patch.dict(
        boto3_rds.__salt__,
        {"boto3_rds.option_group_exists": MagicMock(return_value={"exists": False})},
    ):
        ret = boto3_rds.create_option_group("og", "mysql", "5.6", "desc")
    assert ret["exists"] is True


def test_create_option_group_already_exists():
    with patch.dict(
        boto3_rds.__salt__,
        {"boto3_rds.option_group_exists": MagicMock(return_value={"exists": True})},
    ):
        ret = boto3_rds.create_option_group("og", "mysql", "5.6", "desc")
    assert ret["exists"] is True


def test_create_parameter_group(conn):
    conn.create_db_parameter_group.return_value = {"DBParameterGroup": {}}
    with patch.dict(
        boto3_rds.__salt__,
        {"boto3_rds.parameter_group_exists": MagicMock(return_value={"exists": False})},
    ):
        ret = boto3_rds.create_parameter_group("pg", "mysql5.6", "desc")
    assert ret["exists"] is True
    assert "Created" in ret["message"]


def test_create_subnet_group(conn):
    conn.create_db_subnet_group.return_value = {"DBSubnetGroup": {}}
    with patch.dict(
        boto3_rds.__salt__,
        {"boto3_rds.subnet_group_exists": MagicMock(return_value={"exists": False})},
    ):
        ret = boto3_rds.create_subnet_group("sg", "desc", ["subnet-1", "subnet-2"])
    assert ret["created"] is True


def test_update_parameter_group(conn):
    conn.modify_db_parameter_group.return_value = {"DBParameterGroupName": "pg"}
    with patch.dict(
        boto3_rds.__salt__,
        {"boto3_rds.parameter_group_exists": MagicMock(return_value={"exists": True})},
    ):
        ret = boto3_rds.update_parameter_group(
            "pg", parameters={"back_log": 1, "binlog_checksum": "CRC32", "flag": True}
        )
    assert ret["results"] is True
    sent = conn.modify_db_parameter_group.call_args.kwargs["Parameters"]
    assert len(sent) == 3
    flag = [p for p in sent if p["ParameterName"] == "flag"][0]
    assert flag["ParameterValue"] == "on"


def test_update_parameter_group_not_exists():
    with patch.dict(
        boto3_rds.__salt__,
        {"boto3_rds.parameter_group_exists": MagicMock(return_value={"exists": False})},
    ):
        ret = boto3_rds.update_parameter_group("pg", parameters={"a": 1})
    assert "does not exist" in ret["message"]


def test_describe(conn):
    conn.describe_db_instances.return_value = {
        "DBInstances": [{"DBInstanceIdentifier": "r", "Engine": "mysql", "AllocatedStorage": 10}]
    }
    with patch.dict(
        boto3_rds.__salt__,
        {"boto3_rds.exists": MagicMock(return_value={"exists": True})},
    ):
        ret = boto3_rds.describe("r")
    assert ret["rds"]["DBInstanceIdentifier"] == "r"
    assert ret["rds"]["Engine"] == "mysql"


def test_describe_not_exists():
    with patch.dict(
        boto3_rds.__salt__,
        {"boto3_rds.exists": MagicMock(return_value={"exists": False})},
    ):
        ret = boto3_rds.describe("r")
    assert "does not exist" in ret["message"]


def test_describe_db_instances(conn):
    pag = MagicMock()
    pag.paginate.return_value.search.return_value = iter([{"DBInstanceIdentifier": "r"}])
    conn.get_paginator.return_value = pag
    out = boto3_rds.describe_db_instances(name="r")
    assert out == [{"DBInstanceIdentifier": "r"}]


def test_describe_db_instances_not_found(conn, client_error):
    pag = MagicMock()

    def raiser():
        raise client_error("DBInstanceNotFound", "DescribeDBInstances")
        yield  # pragma: no cover  # pylint: disable=unreachable

    pag.paginate.return_value.search.return_value = raiser()
    conn.get_paginator.return_value = pag
    out = boto3_rds.describe_db_instances(name="r")
    assert not out


def test_describe_db_subnet_groups(conn):
    pag = MagicMock()
    pag.paginate.return_value.search.return_value = iter([{"DBSubnetGroupName": "sg"}])
    conn.get_paginator.return_value = pag
    out = boto3_rds.describe_db_subnet_groups(name="sg")
    assert out == [{"DBSubnetGroupName": "sg"}]


def test_get_endpoint(conn):
    conn.describe_db_instances.return_value = {
        "DBInstances": [{"Endpoint": {"Address": "host.aws"}}]
    }
    with patch.dict(
        boto3_rds.__salt__,
        {"boto3_rds.exists": MagicMock(return_value={"exists": True})},
    ):
        ep = boto3_rds.get_endpoint("r")
    assert ep == "host.aws"


def test_get_endpoint_absent():
    with patch.dict(
        boto3_rds.__salt__,
        {"boto3_rds.exists": MagicMock(return_value={"exists": False})},
    ):
        assert boto3_rds.get_endpoint("r") is False


def test_delete_requires_snapshot_flag():
    with pytest.raises(SaltInvocationError):
        boto3_rds.delete("r")


def test_delete_no_wait(conn):
    conn.delete_db_instance.return_value = {"DBInstance": {}}
    ret = boto3_rds.delete("r", skip_final_snapshot=True, wait_for_deletion=False)
    assert ret["deleted"] is True


def test_delete_wait_for_deletion(conn):
    conn.delete_db_instance.return_value = {"DBInstance": {}}
    with patch.dict(
        boto3_rds.__salt__,
        {"boto3_rds.exists": MagicMock(return_value={"exists": False})},
    ):
        ret = boto3_rds.delete("r", skip_final_snapshot=True)
    assert "completely" in ret["message"]


def test_delete_option_group(conn):
    conn.delete_option_group.return_value = {"ResponseMetadata": {}}
    ret = boto3_rds.delete_option_group("og")
    assert ret["deleted"] is True


def test_delete_parameter_group(conn):
    conn.delete_db_parameter_group.return_value = {"ResponseMetadata": {}}
    ret = boto3_rds.delete_parameter_group("pg")
    assert ret["deleted"] is True


def test_delete_subnet_group(conn):
    conn.delete_db_subnet_group.return_value = {"ResponseMetadata": {}}
    ret = boto3_rds.delete_subnet_group("sg")
    assert ret["deleted"] is True


def test_describe_parameter_group(conn):
    conn.describe_db_parameter_groups.return_value = {"DBParameterGroups": [{}]}
    with patch.dict(
        boto3_rds.__salt__,
        {"boto3_rds.parameter_group_exists": MagicMock(return_value={"exists": True})},
    ):
        ret = boto3_rds.describe_parameter_group("pg")
    assert ret["results"] is True


def test_describe_parameter_group_not_found():
    with patch.dict(
        boto3_rds.__salt__,
        {"boto3_rds.parameter_group_exists": MagicMock(return_value={"exists": False})},
    ):
        ret = boto3_rds.describe_parameter_group("pg")
    assert "exists" in ret


def test_describe_parameters(conn):
    pag = MagicMock()
    pag.paginate.return_value = iter(
        [{"Parameters": [{"ParameterName": "x", "ParameterValue": "1"}]}]
    )
    conn.get_paginator.return_value = pag
    with patch.dict(
        boto3_rds.__salt__,
        {"boto3_rds.parameter_group_exists": MagicMock(return_value={"exists": True})},
    ):
        ret = boto3_rds.describe_parameters("pg")
    assert ret["result"] is True
    assert "x" in ret["parameters"]


def test_describe_parameters_not_found():
    with patch.dict(
        boto3_rds.__salt__,
        {"boto3_rds.parameter_group_exists": MagicMock(return_value={"exists": False})},
    ):
        ret = boto3_rds.describe_parameters("pg")
    assert ret["result"] is False


def test_modify_db_instance(conn):
    conn.modify_db_instance.return_value = {"DBInstance": {"DBInstanceIdentifier": "r"}}
    with patch.dict(
        boto3_rds.__salt__,
        {"boto3_rds.exists": MagicMock(return_value={"exists": True})},
    ):
        ret = boto3_rds.modify_db_instance("r", allocated_storage=20, apply_immediately=True)
    assert ret["modified"] is True
    kw = conn.modify_db_instance.call_args.kwargs
    assert kw["AllocatedStorage"] == 20
    assert kw["ApplyImmediately"] is True


def test_modify_db_instance_not_exists():
    with patch.dict(
        boto3_rds.__salt__,
        {"boto3_rds.exists": MagicMock(return_value={"exists": False})},
    ):
        ret = boto3_rds.modify_db_instance("r", allocated_storage=20)
    assert ret["modified"] is False
