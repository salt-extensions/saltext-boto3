"""
Unit tests for the ``boto3_ec2`` execution module.
"""

from unittest.mock import MagicMock

import pytest

from saltext.boto3.modules import boto3_ec2

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
        boto3_ec2: {
            "__opts__": {},
            "__context__": {},
            "__salt__": {
                "boto3_vpc.get_resource_id": MagicMock(return_value={"id": "subnet-abc"}),
                "boto3_vpc.get_subnet_association": MagicMock(return_value={"vpc_id": "vpc-123"}),
                "boto3_secgroup.convert_to_group_ids": MagicMock(return_value=["sg-1"]),
                "boto3_secgroup.get_group_id": MagicMock(return_value="sg-1"),
            },
        }
    }


@pytest.fixture
def conn(make_conn):
    with make_conn(boto3_ec2) as client:
        yield client


def test_get_unassociated_eip_address(conn):
    conn.describe_addresses.return_value = {
        "Addresses": [
            {
                "PublicIp": "1.2.3.4",
                "Domain": "standard",
                "InstanceId": None,
                "NetworkInterfaceId": None,
            }
        ]
    }
    assert boto3_ec2.get_unassociated_eip_address("standard") == "1.2.3.4"


def test_get_unassociated_eip_address_none(conn):
    conn.describe_addresses.return_value = {"Addresses": []}
    assert boto3_ec2.get_unassociated_eip_address("standard") is None


def test_set_attribute_error(conn, client_error):
    conn.describe_instances.return_value = {
        "Reservations": [{"Instances": [{"InstanceId": "i-1"}]}]
    }
    conn.modify_instance_attribute.side_effect = client_error(
        "AuthFailure", "ModifyInstanceAttribute"
    )
    assert boto3_ec2.set_attribute("sourceDestCheck", False, instance_name="foo") is False
