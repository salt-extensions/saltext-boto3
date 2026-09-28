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


def test_create_launch_template(conn):
    conn.create_launch_template.return_value = {
        "LaunchTemplate": {"LaunchTemplateId": "lt-1", "LaunchTemplateName": "my-lt"}
    }
    result = boto3_ec2.create_launch_template("my-lt", "ami-123", "t3.medium")
    assert result["LaunchTemplateId"] == "lt-1"
    conn.create_launch_template.assert_called_once()


def test_create_launch_template_client_error(conn, client_error):
    conn.create_launch_template.side_effect = client_error("AlreadyExists", "CreateLaunchTemplate")
    result = boto3_ec2.create_launch_template("my-lt", "ami-123", "t3.medium")
    assert "error" in result


def test_delete_launch_template(conn):
    conn.delete_launch_template.return_value = {"LaunchTemplate": {"LaunchTemplateName": "my-lt"}}
    result = boto3_ec2.delete_launch_template("my-lt")
    assert result["LaunchTemplate"]["LaunchTemplateName"] == "my-lt"


def test_delete_launch_template_client_error(conn, client_error):
    conn.delete_launch_template.side_effect = client_error("NotFound", "DeleteLaunchTemplate")
    result = boto3_ec2.delete_launch_template("my-lt")
    assert "error" in result


def test_describe_launch_templates_found(conn):
    conn.describe_launch_templates.return_value = {
        "LaunchTemplates": [{"LaunchTemplateName": "my-lt"}]
    }
    result = boto3_ec2.describe_launch_templates(launch_template_names=["my-lt"])
    assert len(result) == 1
    assert result[0]["LaunchTemplateName"] == "my-lt"


def test_describe_launch_templates_empty(conn):
    conn.describe_launch_templates.return_value = {"LaunchTemplates": []}
    result = boto3_ec2.describe_launch_templates()
    assert result == []


def test_describe_launch_templates_client_error(conn, client_error):
    conn.describe_launch_templates.side_effect = client_error("Boom", "Describe")
    result = boto3_ec2.describe_launch_templates()
    assert "error" in result


def test_get_dns_name_private(conn):
    conn.describe_instances.return_value = {
        "Reservations": [
            {"Instances": [{"InstanceId": "i-1", "PrivateDnsName": "ip-10-0-0-1.ec2.internal"}]}
        ]
    }
    result = boto3_ec2.get_dns_name(name="my-instance")
    assert result == "ip-10-0-0-1.ec2.internal"


def test_get_dns_name_public(conn):
    conn.describe_instances.return_value = {
        "Reservations": [
            {
                "Instances": [
                    {"InstanceId": "i-1", "PublicDnsName": "ec2-1-2-3-4.compute-1.amazonaws.com"}
                ]
            }
        ]
    }
    result = boto3_ec2.get_dns_name(name="my-instance", dns_type="public")
    assert result == "ec2-1-2-3-4.compute-1.amazonaws.com"


def test_get_dns_name_not_found(conn):
    conn.describe_instances.return_value = {"Reservations": []}
    result = boto3_ec2.get_dns_name(name="missing")
    assert "error" in result


def test_get_dns_name_invalid_type(conn):
    result = boto3_ec2.get_dns_name(name="my-instance", dns_type="fqdn")
    assert "error" in result
    conn.describe_instances.assert_not_called()


def test_get_dns_name_no_args(conn):
    result = boto3_ec2.get_dns_name()
    assert "error" in result
    conn.describe_instances.assert_not_called()
