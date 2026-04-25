"""
Unit tests for the ``boto3_vpc`` execution module.
"""

import pytest
from salt.exceptions import CommandExecutionError
from salt.exceptions import SaltInvocationError

from saltext.boto3.modules import boto3_vpc

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
        boto3_vpc: {
            "__opts__": {},
            "__context__": {},
            "__salt__": {},
        }
    }


@pytest.fixture
def conn(make_conn):
    with make_conn(boto3_vpc) as client:
        yield client


@pytest.fixture(autouse=True)
def reset_context():
    boto3_vpc.__context__.clear()
    yield
    boto3_vpc.__context__.clear()


def test_exists_true(conn):
    conn.describe_vpcs.return_value = {"Vpcs": [{"VpcId": "vpc-1"}]}
    assert boto3_vpc.exists(name="myvpc") == {"exists": True}


def test_exists_false(conn):
    conn.describe_vpcs.return_value = {"Vpcs": []}
    assert boto3_vpc.exists(name="missing") == {"exists": False}


def test_exists_invalid_vpc_id_returns_false(conn, client_error):
    conn.describe_vpcs.side_effect = client_error("InvalidVpcID.NotFound", "DescribeVpcs")
    assert boto3_vpc.exists(vpc_id="vpc-bogus") == {"exists": False}


def test_exists_other_client_error_returns_error(conn, client_error):
    conn.describe_vpcs.side_effect = client_error("AuthFailure", "DescribeVpcs")
    result = boto3_vpc.exists(name="x")
    assert "error" in result


def test_exists_requires_a_filter():
    with pytest.raises(SaltInvocationError):
        boto3_vpc.exists()


def test_get_id_uses_cache(conn):
    boto3_vpc.__context__["boto3_ec2:us-east-1:myvpc:id"] = "vpc-cached"
    result = boto3_vpc.get_id(name="myvpc", region="us-east-1")
    assert result == {"id": "vpc-cached"}
    # Cached value short-circuits the API call.
    conn.describe_vpcs.assert_not_called()


def test_get_id_caches_lookup(conn):
    conn.describe_vpcs.return_value = {"Vpcs": [{"VpcId": "vpc-9"}]}
    result = boto3_vpc.get_id(name="myvpc", region="us-east-1")
    assert result == {"id": "vpc-9"}
    assert boto3_vpc.__context__["boto3_ec2:us-east-1:myvpc:id"] == "vpc-9"


def test_get_id_multiple_matches_raises(conn):
    conn.describe_vpcs.return_value = {"Vpcs": [{"VpcId": "vpc-1"}, {"VpcId": "vpc-2"}]}
    with pytest.raises(CommandExecutionError):
        boto3_vpc.get_id(name="myvpc")


def test_check_vpc_by_name(conn):
    conn.describe_vpcs.return_value = {"Vpcs": [{"VpcId": "vpc-7"}]}
    assert boto3_vpc.check_vpc(vpc_name="myvpc") == "vpc-7"


def test_check_vpc_by_id_present(conn):
    conn.describe_vpcs.return_value = {"Vpcs": [{"VpcId": "vpc-7"}]}
    assert boto3_vpc.check_vpc(vpc_id="vpc-7") == "vpc-7"


def test_check_vpc_by_id_missing(conn):
    conn.describe_vpcs.return_value = {"Vpcs": []}
    assert boto3_vpc.check_vpc(vpc_id="vpc-missing") is None


def test_check_vpc_requires_exactly_one():
    with pytest.raises(SaltInvocationError):
        boto3_vpc.check_vpc(vpc_id="a", vpc_name="b")
    with pytest.raises(SaltInvocationError):
        boto3_vpc.check_vpc()


def test_create_minimal(conn):
    conn.create_vpc.return_value = {"Vpc": {"VpcId": "vpc-new"}}
    result = boto3_vpc.create("10.0.0.0/24")
    assert result == {"created": True, "id": "vpc-new"}
    conn.create_vpc.assert_called_once_with(CidrBlock="10.0.0.0/24")


def test_create_with_name_tags_and_dns(conn):
    conn.create_vpc.return_value = {"Vpc": {"VpcId": "vpc-x"}}
    result = boto3_vpc.create(
        "10.0.0.0/24",
        instance_tenancy="default",
        vpc_name="myvpc",
        enable_dns_support=True,
        enable_dns_hostnames=False,
        tags={"Env": "prod"},
        region="us-east-1",
    )
    assert result == {"created": True, "id": "vpc-x"}
    kwargs = conn.create_vpc.call_args.kwargs
    assert kwargs["CidrBlock"] == "10.0.0.0/24"
    assert kwargs["InstanceTenancy"] == "default"
    tag_spec = kwargs["TagSpecifications"][0]
    assert tag_spec["ResourceType"] == "vpc"
    assert {"Key": "Name", "Value": "myvpc"} in tag_spec["Tags"]
    assert {"Key": "Env", "Value": "prod"} in tag_spec["Tags"]
    conn.modify_vpc_attribute.assert_any_call(VpcId="vpc-x", EnableDnsSupport={"Value": True})
    conn.modify_vpc_attribute.assert_any_call(VpcId="vpc-x", EnableDnsHostnames={"Value": False})
    # Caches the new id.
    assert boto3_vpc.__context__["boto3_ec2:us-east-1:myvpc:id"] == "vpc-x"


def test_create_handles_client_error(conn, client_error):
    conn.create_vpc.side_effect = client_error("VpcLimitExceeded", "CreateVpc")
    result = boto3_vpc.create("10.0.0.0/24")
    assert result["created"] is False
    assert "error" in result


def test_delete_by_id(conn):
    result = boto3_vpc.delete(vpc_id="vpc-1")
    assert result == {"deleted": True}
    conn.delete_vpc.assert_called_once_with(VpcId="vpc-1")


def test_delete_by_name_resolves_id(conn):
    conn.describe_vpcs.return_value = {"Vpcs": [{"VpcId": "vpc-2"}]}
    result = boto3_vpc.delete(vpc_name="myvpc", region="us-east-1")
    assert result == {"deleted": True}
    conn.delete_vpc.assert_called_once_with(VpcId="vpc-2")


def test_delete_unknown_name_returns_error(conn):
    conn.describe_vpcs.return_value = {"Vpcs": []}
    result = boto3_vpc.delete(vpc_name="ghost")
    assert result["deleted"] is False
    assert "ghost" in result["error"]["message"]


def test_delete_requires_exactly_one():
    with pytest.raises(SaltInvocationError):
        boto3_vpc.delete()
    with pytest.raises(SaltInvocationError):
        boto3_vpc.delete(vpc_id="a", vpc_name="b")


def test_describe_returns_payload(conn):
    conn.describe_vpcs.side_effect = [
        # First call from _find_vpcs.
        {"Vpcs": [{"VpcId": "vpc-3"}]},
        # Second call from describe() once the ID is known.
        {
            "Vpcs": [
                {
                    "VpcId": "vpc-3",
                    "CidrBlock": "10.0.0.0/24",
                    "IsDefault": False,
                    "State": "available",
                    "Tags": [{"Key": "Name", "Value": "myvpc"}],
                    "DhcpOptionsId": "dopt-1",
                    "InstanceTenancy": "default",
                }
            ]
        },
    ]
    result = boto3_vpc.describe(vpc_name="myvpc")
    assert result["vpc"]["id"] == "vpc-3"
    assert result["vpc"]["tags"] == {"Name": "myvpc"}
    assert result["vpc"]["instance_tenancy"] == "default"


def test_describe_no_match(conn):
    conn.describe_vpcs.return_value = {"Vpcs": []}
    assert boto3_vpc.describe(vpc_name="ghost") == {"vpc": None}


def test_describe_vpcs_payload(conn):
    conn.describe_vpcs.return_value = {
        "Vpcs": [
            {"VpcId": "vpc-1", "CidrBlock": "10.0.0.0/16"},
            {"VpcId": "vpc-2", "CidrBlock": "10.1.0.0/16"},
        ]
    }
    result = boto3_vpc.describe_vpcs()
    assert {v["id"] for v in result["vpcs"]} == {"vpc-1", "vpc-2"}


def test_find_resources_unsupported():
    with pytest.raises(SaltInvocationError):
        boto3_vpc._find_resources("nope", name="x")


def test_find_resources_requires_filter():
    with pytest.raises(SaltInvocationError):
        boto3_vpc._find_resources("subnet")


def test_resource_exists_subnet_true(conn):
    conn.describe_subnets.return_value = {"Subnets": [{"SubnetId": "subnet-1"}]}
    assert boto3_vpc.resource_exists("subnet", name="x") == {"exists": True}


def test_get_resource_id_returns_id_directly(conn):
    # When given an id, no API call is needed.
    result = boto3_vpc.get_resource_id("subnet", resource_id="subnet-7")
    assert result == {"id": "subnet-7"}
    conn.describe_subnets.assert_not_called()


def test_get_resource_id_lookup_caches(conn):
    conn.describe_subnets.return_value = {"Subnets": [{"SubnetId": "subnet-9"}]}
    result = boto3_vpc.get_resource_id("subnet", name="mysub", region="us-east-1")
    assert result == {"id": "subnet-9"}
    assert boto3_vpc.__context__["boto3_ec2:us-east-1:subnet:mysub:id"] == "subnet-9"


def test_create_subnet(conn):
    # check_vpc -> describe_vpcs hit, returns the VPC id.
    # _get_resource_id (subnet name lookup) -> describe_subnets returns no
    # match, so the new subnet is created.
    conn.describe_vpcs.return_value = {"Vpcs": [{"VpcId": "vpc-1"}]}
    conn.describe_subnets.return_value = {"Subnets": []}
    conn.create_subnet.return_value = {"Subnet": {"SubnetId": "subnet-new"}}
    result = boto3_vpc.create_subnet(
        vpc_name="myvpc",
        cidr_block="10.0.0.0/25",
        subnet_name="mysub",
        availability_zone="us-east-1a",
        auto_assign_public_ipv4=True,
    )
    assert result == {"created": True, "id": "subnet-new"}
    conn.create_subnet.assert_called_once()
    kwargs = conn.create_subnet.call_args.kwargs
    assert kwargs["VpcId"] == "vpc-1"
    assert kwargs["CidrBlock"] == "10.0.0.0/25"
    assert kwargs["AvailabilityZone"] == "us-east-1a"
    conn.modify_subnet_attribute.assert_called_once_with(
        SubnetId="subnet-new", MapPublicIpOnLaunch={"Value": True}
    )


def test_create_subnet_vpc_missing(conn):
    conn.describe_vpcs.return_value = {"Vpcs": []}
    result = boto3_vpc.create_subnet(
        vpc_name="ghost", cidr_block="10.0.0.0/25", subnet_name="mysub"
    )
    assert result["created"] is False
    assert "ghost" in result["error"]["message"]


def test_create_subnet_already_exists(conn):
    conn.describe_vpcs.return_value = {"Vpcs": [{"VpcId": "vpc-1"}]}
    conn.describe_subnets.return_value = {"Subnets": [{"SubnetId": "subnet-x"}]}
    result = boto3_vpc.create_subnet(vpc_name="myvpc", cidr_block="10.0.0.0/25", subnet_name="dup")
    assert result["created"] is False
    assert "already exists" in result["error"]["message"]


def test_delete_subnet_by_id(conn):
    result = boto3_vpc.delete_subnet(subnet_id="subnet-1")
    assert result == {"deleted": True}
    conn.delete_subnet.assert_called_once_with(SubnetId="subnet-1")


def test_delete_subnet_by_name(conn):
    conn.describe_subnets.return_value = {"Subnets": [{"SubnetId": "subnet-2"}]}
    result = boto3_vpc.delete_subnet(subnet_name="mysub")
    assert result == {"deleted": True}
    conn.delete_subnet.assert_called_once_with(SubnetId="subnet-2")


def test_subnet_exists_zones_filter(conn):
    conn.describe_subnets.return_value = {"Subnets": [{"SubnetId": "subnet-1"}]}
    result = boto3_vpc.subnet_exists(zones="us-east-1a")
    assert result == {"exists": True}
    kwargs = conn.describe_subnets.call_args.kwargs
    assert {"Name": "availability-zone", "Values": ["us-east-1a"]} in kwargs["Filters"]


def test_subnet_exists_invalid_id_false(conn, client_error):
    conn.describe_subnets.side_effect = client_error("InvalidSubnetID.NotFound", "DescribeSubnets")
    assert boto3_vpc.subnet_exists(subnet_id="subnet-bad") == {"exists": False}


def test_get_subnet_association_single(conn):
    conn.describe_subnets.return_value = {
        "Subnets": [
            {"SubnetId": "s-1", "VpcId": "vpc-1"},
            {"SubnetId": "s-2", "VpcId": "vpc-1"},
        ]
    }
    assert boto3_vpc.get_subnet_association(["s-1", "s-2"]) == {"vpc_id": "vpc-1"}


def test_get_subnet_association_multiple(conn):
    conn.describe_subnets.return_value = {
        "Subnets": [
            {"SubnetId": "s-1", "VpcId": "vpc-1"},
            {"SubnetId": "s-2", "VpcId": "vpc-2"},
        ]
    }
    result = boto3_vpc.get_subnet_association(["s-1", "s-2"])
    assert "vpc_ids" in result
    assert set(result["vpc_ids"]) == {"vpc-1", "vpc-2"}


def test_describe_subnet(conn):
    conn.describe_subnets.return_value = {
        "Subnets": [
            {
                "SubnetId": "subnet-1",
                "CidrBlock": "10.0.0.0/24",
                "AvailabilityZone": "us-east-1a",
                "Tags": [{"Key": "Name", "Value": "sub"}],
                "VpcId": "vpc-1",
            }
        ]
    }
    result = boto3_vpc.describe_subnet(subnet_id="subnet-1")
    assert result["subnet"] == {
        "id": "subnet-1",
        "cidr_block": "10.0.0.0/24",
        "availability_zone": "us-east-1a",
        "tags": {"Name": "sub"},
        "vpc_id": "vpc-1",
    }


def test_create_no_vpc(conn):
    conn.create_internet_gateway.return_value = {"InternetGateway": {"InternetGatewayId": "igw-1"}}
    result = boto3_vpc.create_internet_gateway(internet_gateway_name="myigw")
    assert result == {"created": True, "id": "igw-1"}
    conn.attach_internet_gateway.assert_not_called()


def test_create_and_attach(conn):
    conn.describe_vpcs.return_value = {"Vpcs": [{"VpcId": "vpc-1"}]}
    conn.create_internet_gateway.return_value = {"InternetGateway": {"InternetGatewayId": "igw-2"}}
    result = boto3_vpc.create_internet_gateway(internet_gateway_name="myigw", vpc_name="myvpc")
    assert result == {"created": True, "id": "igw-2"}
    conn.attach_internet_gateway.assert_called_once_with(InternetGatewayId="igw-2", VpcId="vpc-1")


def test_igw_create_vpc_missing(conn):
    conn.describe_vpcs.return_value = {"Vpcs": []}
    result = boto3_vpc.create_internet_gateway(vpc_name="ghost")
    assert result["created"] is False
    assert "ghost" in result["error"]["message"]


def test_igw_delete_by_id(conn):
    result = boto3_vpc.delete_internet_gateway(internet_gateway_id="igw-1")
    assert result == {"deleted": True}
    conn.delete_internet_gateway.assert_called_once_with(InternetGatewayId="igw-1")


def test_igw_delete_by_name(conn):
    conn.describe_internet_gateways.return_value = {
        "InternetGateways": [{"InternetGatewayId": "igw-7"}]
    }
    result = boto3_vpc.delete_internet_gateway(internet_gateway_name="myigw")
    assert result == {"deleted": True}
    conn.delete_internet_gateway.assert_called_once_with(InternetGatewayId="igw-7")


def test_delete_missing_name(conn):
    conn.describe_internet_gateways.return_value = {"InternetGateways": []}
    result = boto3_vpc.delete_internet_gateway(internet_gateway_name="ghost")
    assert result["deleted"] is False


def test_delete_with_detach(conn):
    conn.describe_internet_gateways.return_value = {
        "InternetGateways": [{"InternetGatewayId": "igw-1", "Attachments": [{"VpcId": "vpc-1"}]}]
    }
    result = boto3_vpc.delete_internet_gateway(internet_gateway_id="igw-1", detach=True)
    assert result == {"deleted": True}
    conn.detach_internet_gateway.assert_called_once_with(InternetGatewayId="igw-1", VpcId="vpc-1")


def test_nat_gateway_exists_true(conn):
    conn.describe_nat_gateways.return_value = {
        "NatGateways": [{"NatGatewayId": "nat-1", "State": "available"}]
    }
    assert boto3_vpc.nat_gateway_exists(nat_gateway_id="nat-1") is True


def test_nat_gateway_exists_filters_state(conn):
    conn.describe_nat_gateways.return_value = {
        "NatGateways": [{"NatGatewayId": "nat-1", "State": "deleted"}]
    }
    assert boto3_vpc.nat_gateway_exists(nat_gateway_id="nat-1") is False


def test_describe_nat_gateways_requires_filter():
    with pytest.raises(SaltInvocationError):
        boto3_vpc.describe_nat_gateways()


def test_describe_nat_gateways_subnet_name_unknown(conn):
    conn.describe_subnets.return_value = {"Subnets": []}
    assert not boto3_vpc.describe_nat_gateways(subnet_name="ghost")


def test_describe_nat_gateways_paginates(conn):
    conn.describe_subnets.return_value = {"Subnets": [{"SubnetId": "subnet-1"}]}
    conn.describe_nat_gateways.side_effect = [
        {
            "NatGateways": [{"NatGatewayId": "nat-1", "State": "available"}],
            "NextToken": "tok",
        },
        {"NatGateways": [{"NatGatewayId": "nat-2", "State": "pending"}]},
    ]
    result = boto3_vpc.describe_nat_gateways(subnet_name="mysub")
    assert [g["NatGatewayId"] for g in result] == ["nat-1", "nat-2"]


def test_create_nat_gateway_allocates_eip(conn):
    conn.allocate_address.return_value = {"AllocationId": "eipalloc-1"}
    conn.create_nat_gateway.return_value = {"NatGateway": {"NatGatewayId": "nat-1"}}
    result = boto3_vpc.create_nat_gateway(subnet_id="subnet-1")
    assert result == {"created": True, "id": "nat-1"}
    conn.create_nat_gateway.assert_called_once_with(SubnetId="subnet-1", AllocationId="eipalloc-1")


def test_create_nat_gateway_subnet_name_missing(conn):
    conn.describe_subnets.return_value = {"Subnets": []}
    result = boto3_vpc.create_nat_gateway(subnet_name="ghost")
    assert result["created"] is False


def test_create_nat_gateway_both_args_invalid():
    with pytest.raises(SaltInvocationError):
        boto3_vpc.create_nat_gateway(subnet_id="s", subnet_name="n")


def test_delete_nat_gateway_and_release(conn):
    conn.describe_nat_gateways.return_value = {
        "NatGateways": [
            {
                "NatGatewayId": "nat-1",
                "State": "deleted",
                "NatGatewayAddresses": [{"AllocationId": "eipalloc-1"}],
            }
        ]
    }
    result = boto3_vpc.delete_nat_gateway(
        nat_gateway_id="nat-1", release_eips=True, wait_for_delete=False
    )
    assert result == {"deleted": True}
    conn.delete_nat_gateway.assert_called_once_with(NatGatewayId="nat-1")
    conn.release_address.assert_called_once_with(AllocationId="eipalloc-1")


def test_delete_nat_gateway_error(conn, client_error):
    conn.describe_nat_gateways.side_effect = client_error(
        "NatGatewayNotFound", "DescribeNatGateways"
    )
    result = boto3_vpc.delete_nat_gateway(nat_gateway_id="nat-1")
    assert result["deleted"] is False


def test_cgw_create(conn):
    conn.create_customer_gateway.return_value = {"CustomerGateway": {"CustomerGatewayId": "cgw-1"}}
    result = boto3_vpc.create_customer_gateway(
        "ipsec.1", "1.2.3.4", 65000, customer_gateway_name="mycgw"
    )
    assert result == {"created": True, "id": "cgw-1"}
    kwargs = conn.create_customer_gateway.call_args.kwargs
    assert kwargs["Type"] == "ipsec.1"
    assert kwargs["PublicIp"] == "1.2.3.4"
    assert kwargs["BgpAsn"] == 65000


def test_cgw_create_error(conn, client_error):
    conn.create_customer_gateway.side_effect = client_error(
        "InvalidIpAddress", "CreateCustomerGateway"
    )
    result = boto3_vpc.create_customer_gateway("ipsec.1", "bad", 65000)
    assert result["created"] is False


def test_cgw_delete_by_id(conn):
    result = boto3_vpc.delete_customer_gateway(customer_gateway_id="cgw-1")
    assert result == {"deleted": True}
    conn.delete_customer_gateway.assert_called_once_with(CustomerGatewayId="cgw-1")


def test_delete_by_name_missing(conn):
    conn.describe_customer_gateways.return_value = {"CustomerGateways": []}
    result = boto3_vpc.delete_customer_gateway(customer_gateway_name="ghost")
    assert result["deleted"] is False


def test_exists(conn):
    conn.describe_customer_gateways.return_value = {
        "CustomerGateways": [{"CustomerGatewayId": "cgw-1"}]
    }
    assert boto3_vpc.customer_gateway_exists(customer_gateway_name="mycgw") == {"exists": True}


def test_create_without_subnet(conn):
    conn.describe_vpcs.return_value = {"Vpcs": [{"VpcId": "vpc-1"}]}
    conn.create_network_acl.return_value = {"NetworkAcl": {"NetworkAclId": "acl-1"}}
    result = boto3_vpc.create_network_acl(vpc_name="myvpc", network_acl_name="myacl")
    assert result == {"created": True, "id": "acl-1"}


def test_create_and_associate_subnet(conn):
    conn.describe_vpcs.return_value = {"Vpcs": [{"VpcId": "vpc-1"}]}
    conn.create_network_acl.return_value = {"NetworkAcl": {"NetworkAclId": "acl-1"}}
    conn.describe_network_acls.return_value = {
        "NetworkAcls": [
            {
                "Associations": [
                    {
                        "SubnetId": "subnet-1",
                        "NetworkAclAssociationId": "aclassoc-old",
                    }
                ]
            }
        ]
    }
    conn.replace_network_acl_association.return_value = {"NewAssociationId": "aclassoc-new"}
    result = boto3_vpc.create_network_acl(
        vpc_id="vpc-1", subnet_id="subnet-1", network_acl_name="myacl"
    )
    assert result["created"] is True
    assert result["association_id"] == "aclassoc-new"
    conn.replace_network_acl_association.assert_called_once_with(
        AssociationId="aclassoc-old", NetworkAclId="acl-1"
    )


def test_acl_create_vpc_missing(conn):
    conn.describe_vpcs.return_value = {"Vpcs": []}
    result = boto3_vpc.create_network_acl(vpc_name="ghost")
    assert result["created"] is False


def test_acl_delete_by_id(conn):
    result = boto3_vpc.delete_network_acl(network_acl_id="acl-1")
    assert result == {"deleted": True}
    conn.delete_network_acl.assert_called_once_with(NetworkAclId="acl-1")


def test_delete_with_disassociate(conn):
    conn.describe_network_acls.side_effect = [
        # describe(NetworkAclIds=[acl-1])
        {
            "NetworkAcls": [
                {
                    "VpcId": "vpc-1",
                    "Associations": [
                        {
                            "SubnetId": "subnet-1",
                            "NetworkAclAssociationId": "aclassoc-1",
                        }
                    ],
                }
            ]
        },
        # describe default acl for vpc
        {"NetworkAcls": [{"NetworkAclId": "acl-default"}]},
    ]
    result = boto3_vpc.delete_network_acl(network_acl_id="acl-1", disassociate=True)
    assert result == {"deleted": True}
    conn.replace_network_acl_association.assert_called_once_with(
        AssociationId="aclassoc-1", NetworkAclId="acl-default"
    )


def test_network_acl_exists_true(conn):
    conn.describe_network_acls.return_value = {"NetworkAcls": [{"NetworkAclId": "acl-1"}]}
    assert boto3_vpc.network_acl_exists(network_acl_name="myacl") == {"exists": True}


def test_associate_to_subnet(conn):
    conn.describe_network_acls.return_value = {
        "NetworkAcls": [
            {
                "Associations": [
                    {
                        "SubnetId": "subnet-1",
                        "NetworkAclAssociationId": "aclassoc-old",
                    }
                ]
            }
        ]
    }
    conn.replace_network_acl_association.return_value = {"NewAssociationId": "aclassoc-new"}
    result = boto3_vpc.associate_network_acl_to_subnet(network_acl_id="acl-1", subnet_id="subnet-1")
    assert result == {"associated": True, "id": "aclassoc-new"}


def test_associate_no_existing_association(conn):
    conn.describe_network_acls.return_value = {"NetworkAcls": []}
    result = boto3_vpc.associate_network_acl_to_subnet(network_acl_id="acl-1", subnet_id="subnet-1")
    assert result["associated"] is False


def test_disassociate_requires_one_subnet():
    with pytest.raises(SaltInvocationError):
        boto3_vpc.disassociate_network_acl()


def test_create_network_acl_entry(conn):
    result = boto3_vpc.create_network_acl_entry(
        network_acl_id="acl-1",
        rule_number=100,
        protocol="tcp",
        rule_action="allow",
        cidr_block="0.0.0.0/0",
        egress=False,
        port_range_from=80,
        port_range_to=80,
    )
    assert result == {"created": True}
    kwargs = conn.create_network_acl_entry.call_args.kwargs
    assert kwargs["RuleNumber"] == 100
    assert kwargs["Protocol"] == "6"
    assert kwargs["RuleAction"] == "allow"
    assert kwargs["Egress"] is False
    assert kwargs["PortRange"] == {"From": 80, "To": 80}


def test_replace_network_acl_entry(conn):
    result = boto3_vpc.replace_network_acl_entry(
        network_acl_id="acl-1",
        rule_number=100,
        protocol="all",
        rule_action="deny",
        cidr_block="0.0.0.0/0",
        egress=True,
    )
    assert result == {"replaced": True}
    conn.replace_network_acl_entry.assert_called_once()


def test_delete_network_acl_entry(conn):
    result = boto3_vpc.delete_network_acl_entry(
        network_acl_id="acl-1", rule_number=100, egress=False
    )
    assert result == {"deleted": True}
    conn.delete_network_acl_entry.assert_called_once_with(
        NetworkAclId="acl-1", RuleNumber=100, Egress=False
    )


def test_delete_network_acl_entry_requires_rule_number():
    with pytest.raises(SaltInvocationError):
        boto3_vpc.delete_network_acl_entry(network_acl_id="acl-1", egress=False)


def test_rt_create(conn):
    conn.describe_vpcs.return_value = {"Vpcs": [{"VpcId": "vpc-1"}]}
    conn.create_route_table.return_value = {"RouteTable": {"RouteTableId": "rtb-1"}}
    result = boto3_vpc.create_route_table(vpc_name="myvpc", route_table_name="myrt")
    assert result == {"created": True, "id": "rtb-1"}


def test_rt_create_vpc_missing(conn):
    conn.describe_vpcs.return_value = {"Vpcs": []}
    result = boto3_vpc.create_route_table(vpc_name="ghost")
    assert result["created"] is False


def test_rt_delete_by_id(conn):
    result = boto3_vpc.delete_route_table(route_table_id="rtb-1")
    assert result == {"deleted": True}
    conn.delete_route_table.assert_called_once_with(RouteTableId="rtb-1")


def test_route_table_exists(conn):
    conn.describe_route_tables.return_value = {"RouteTables": [{"RouteTableId": "rtb-1"}]}
    assert boto3_vpc.route_table_exists(route_table_name="myrt") == {"exists": True}


def test_describe_route_tables(conn):
    conn.describe_route_tables.return_value = {
        "RouteTables": [
            {
                "RouteTableId": "rtb-1",
                "VpcId": "vpc-1",
                "Routes": [
                    {"DestinationCidrBlock": "10.0.0.0/16", "GatewayId": "local"},
                    {
                        "DestinationCidrBlock": "0.0.0.0/0",
                        "GatewayId": "igw-1",
                    },
                ],
                "Associations": [
                    {
                        "RouteTableAssociationId": "rtbassoc-1",
                        "RouteTableId": "rtb-1",
                        "SubnetId": "subnet-1",
                    }
                ],
                "Tags": [{"Key": "Name", "Value": "myrt"}],
            }
        ]
    }
    result = boto3_vpc.describe_route_tables(route_table_name="myrt")
    assert len(result) == 1
    rt = result[0]
    assert rt["id"] == "rtb-1"
    assert rt["vpc_id"] == "vpc-1"
    assert len(rt["routes"]) == 2
    assert rt["associations"][0]["subnet_id"] == "subnet-1"


def test_describe_route_tables_error(conn, client_error):
    conn.describe_route_tables.side_effect = client_error("AuthFailure", "DescribeRouteTables")
    result = boto3_vpc.describe_route_tables(route_table_name="myrt")
    assert "error" in result


def test_describe_route_tables_requires_filter():
    with pytest.raises(SaltInvocationError):
        boto3_vpc.describe_route_tables()


def test_route_exists_true(conn):
    conn.describe_route_tables.return_value = {
        "RouteTables": [
            {
                "RouteTableId": "rtb-1",
                "VpcId": "vpc-1",
                "Routes": [{"DestinationCidrBlock": "10.0.0.0/16", "GatewayId": "igw-1"}],
                "Associations": [],
            }
        ]
    }
    assert boto3_vpc.route_exists(
        destination_cidr_block="10.0.0.0/16",
        route_table_name="myrt",
        gateway_id="igw-1",
    ) == {"exists": True}


def test_associate_route_table(conn):
    conn.associate_route_table.return_value = {"AssociationId": "rtbassoc-1"}
    result = boto3_vpc.associate_route_table(route_table_id="rtb-1", subnet_id="subnet-1")
    assert result == {"association_id": "rtbassoc-1"}


def test_disassociate_route_table(conn):
    result = boto3_vpc.disassociate_route_table("rtbassoc-1")
    assert result == {"disassociated": True}
    conn.disassociate_route_table.assert_called_once_with(AssociationId="rtbassoc-1")


def test_replace_route_table_association(conn):
    conn.replace_route_table_association.return_value = {"NewAssociationId": "rtbassoc-new"}
    result = boto3_vpc.replace_route_table_association("rtbassoc-old", "rtb-1")
    assert result == {"replaced": True, "association_id": "rtbassoc-new"}


def test_create_route(conn):
    result = boto3_vpc.create_route(
        route_table_id="rtb-1",
        destination_cidr_block="0.0.0.0/0",
        gateway_id="igw-1",
    )
    assert result == {"created": True}
    conn.create_route.assert_called_once_with(
        RouteTableId="rtb-1", DestinationCidrBlock="0.0.0.0/0", GatewayId="igw-1"
    )


def test_create_route_requires_target():
    with pytest.raises(SaltInvocationError):
        boto3_vpc.create_route(route_table_id="rtb-1", destination_cidr_block="0.0.0.0/0")


def test_create_route_internet_gateway_name_lookup(conn):
    conn.describe_internet_gateways.return_value = {
        "InternetGateways": [{"InternetGatewayId": "igw-1"}]
    }
    result = boto3_vpc.create_route(
        route_table_id="rtb-1",
        destination_cidr_block="0.0.0.0/0",
        internet_gateway_name="myigw",
    )
    assert result == {"created": True}
    kwargs = conn.create_route.call_args.kwargs
    assert kwargs["GatewayId"] == "igw-1"


def test_create_route_internet_gateway_name_missing(conn):
    conn.describe_internet_gateways.return_value = {"InternetGateways": []}
    result = boto3_vpc.create_route(
        route_table_id="rtb-1",
        destination_cidr_block="0.0.0.0/0",
        internet_gateway_name="ghost",
    )
    assert result["created"] is False


def test_delete_route(conn):
    result = boto3_vpc.delete_route(route_table_id="rtb-1", destination_cidr_block="0.0.0.0/0")
    assert result == {"deleted": True}
    conn.delete_route.assert_called_once_with(
        RouteTableId="rtb-1", DestinationCidrBlock="0.0.0.0/0"
    )


def test_replace_route(conn):
    result = boto3_vpc.replace_route(
        route_table_id="rtb-1",
        destination_cidr_block="0.0.0.0/0",
        gateway_id="igw-2",
    )
    assert result == {"replaced": True}
    conn.replace_route.assert_called_once()


def test_request_by_ids(conn):
    conn.describe_vpc_peering_connections.return_value = {"VpcPeeringConnections": []}
    conn.create_vpc_peering_connection.return_value = {
        "VpcPeeringConnection": {"VpcPeeringConnectionId": "pcx-1"}
    }
    result = boto3_vpc.request_vpc_peering_connection(
        requester_vpc_id="vpc-1", peer_vpc_id="vpc-2", name="mypeer"
    )
    assert "pcx-1" in result["msg"]
    conn.create_tags.assert_called_once()


def test_request_requires_exactly_one_requester():
    with pytest.raises(SaltInvocationError):
        boto3_vpc.request_vpc_peering_connection(peer_vpc_id="vpc-2")


def test_describe_vpc_peering_connection(conn):
    conn.describe_vpc_peering_connections.return_value = {
        "VpcPeeringConnections": [
            {"VpcPeeringConnectionId": "pcx-1"},
            {"VpcPeeringConnectionId": "pcx-2"},
        ]
    }
    result = boto3_vpc.describe_vpc_peering_connection("mypeer")
    assert result == {"VPC-Peerings": ["pcx-1", "pcx-2"]}


def test_accept_by_id(conn):
    result = boto3_vpc.accept_vpc_peering_connection(conn_id="pcx-1")
    assert "accepted" in result["msg"]
    conn.accept_vpc_peering_connection.assert_called_once_with(
        DryRun=False, VpcPeeringConnectionId="pcx-1"
    )


def test_accept_by_name(conn):
    conn.describe_vpc_peering_connections.return_value = {
        "VpcPeeringConnections": [{"VpcPeeringConnectionId": "pcx-1"}]
    }
    result = boto3_vpc.accept_vpc_peering_connection(name="mypeer")
    assert "accepted" in result["msg"]


def test_accept_name_unknown(conn):
    conn.describe_vpc_peering_connections.return_value = {"VpcPeeringConnections": []}
    with pytest.raises(SaltInvocationError):
        boto3_vpc.accept_vpc_peering_connection(name="ghost")


def test_peering_delete_by_name(conn):
    conn.describe_vpc_peering_connections.return_value = {
        "VpcPeeringConnections": [{"VpcPeeringConnectionId": "pcx-1"}]
    }
    result = boto3_vpc.delete_vpc_peering_connection(conn_name="mypeer")
    assert "deleted" in result["msg"]


def test_is_peering_connection_pending_true(conn):
    conn.describe_vpc_peering_connections.return_value = {
        "VpcPeeringConnections": [{"Status": {"Code": "pending-acceptance"}}]
    }
    assert boto3_vpc.is_peering_connection_pending(conn_id="pcx-1") is True


def test_is_peering_connection_pending_false(conn):
    conn.describe_vpc_peering_connections.return_value = {
        "VpcPeeringConnections": [{"Status": {"Code": "active"}}]
    }
    assert boto3_vpc.is_peering_connection_pending(conn_id="pcx-1") is False


def test_peering_connection_pending_from_vpc_true(conn):
    # check_vpc to resolve vpc_name
    conn.describe_vpcs.return_value = {"Vpcs": [{"VpcId": "vpc-1"}]}
    conn.describe_vpc_peering_connections.return_value = {
        "VpcPeeringConnections": [{"Status": {"Code": "pending-acceptance"}}]
    }
    assert (
        boto3_vpc.peering_connection_pending_from_vpc(conn_name="mypeer", vpc_name="myvpc") is True
    )
