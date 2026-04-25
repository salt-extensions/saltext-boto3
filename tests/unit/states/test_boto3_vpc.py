"""
Unit tests for the ``boto3_vpc`` state module.
"""

from unittest.mock import MagicMock

import pytest

from saltext.boto3.states import boto3_vpc as boto3_vpc_state

pytestmark = [
    pytest.mark.skip_on_fips_enabled_platform,
]


@pytest.fixture
def configure_loader_modules():
    return {boto3_vpc_state: {"__opts__": {"test": False}, "__salt__": {}}}


def test_virtual(mock_salt):
    with mock_salt(boto3_vpc_state, {"boto3_vpc.exists": True}):
        assert boto3_vpc_state.__virtual__() == "boto3_vpc"


def test_virtual_no_exec_module(mock_salt):
    with mock_salt(boto3_vpc_state, {}):
        result = boto3_vpc_state.__virtual__()
    assert result[0] is False


def test_vpc_present_already_present(mock_salt):
    with mock_salt(boto3_vpc_state, {"boto3_vpc.exists": {"exists": True}}):
        result = boto3_vpc_state.present("myvpc", "10.0.0.0/24")
    assert result["result"] is True
    assert not result["changes"]
    assert result["comment"] == "VPC present."


def test_vpc_present_test_mode(mock_salt):
    with mock_salt(boto3_vpc_state, {"boto3_vpc.exists": {"exists": False}}, test=True):
        result = boto3_vpc_state.present("myvpc", "10.0.0.0/24")
    assert result["result"] is None
    assert result["changes"]["new"] == {"vpc": "myvpc"}


def test_vpc_present_create(mock_salt):
    salt_map = {
        "boto3_vpc.exists": {"exists": False},
        "boto3_vpc.create": {"created": True, "id": "vpc-1"},
        "boto3_vpc.describe": {"vpc": {"id": "vpc-1"}},
    }
    with mock_salt(boto3_vpc_state, salt_map):
        result = boto3_vpc_state.present("myvpc", "10.0.0.0/24")
    assert result["result"] is True
    assert result["changes"]["new"] == {"vpc": {"id": "vpc-1"}}


def test_vpc_present_create_failure(mock_salt):
    salt_map = {
        "boto3_vpc.exists": {"exists": False},
        "boto3_vpc.create": {"created": False, "error": {"message": "boom"}},
    }
    with mock_salt(boto3_vpc_state, salt_map):
        result = boto3_vpc_state.present("myvpc", "10.0.0.0/24")
    assert result["result"] is False
    assert "boom" in result["comment"]


def test_vpc_present_exists_error(mock_salt):
    with mock_salt(
        boto3_vpc_state,
        {"boto3_vpc.exists": {"error": {"message": "denied"}}},
    ):
        result = boto3_vpc_state.present("myvpc", "10.0.0.0/24")
    assert result["result"] is False
    assert "denied" in result["comment"]


def test_vpc_absent_already_absent(mock_salt):
    with mock_salt(boto3_vpc_state, {"boto3_vpc.get_id": {"id": None}}):
        result = boto3_vpc_state.absent("myvpc")
    assert result["result"] is True
    assert not result["changes"]


def test_vpc_absent_test_mode(mock_salt):
    with mock_salt(boto3_vpc_state, {"boto3_vpc.get_id": {"id": "vpc-1"}}, test=True):
        result = boto3_vpc_state.absent("myvpc")
    assert result["result"] is None
    assert result["changes"]["old"] == {"vpc": "vpc-1"}


def test_vpc_absent_delete(mock_salt):
    salt_map = {
        "boto3_vpc.get_id": {"id": "vpc-1"},
        "boto3_vpc.delete": {"deleted": True},
    }
    with mock_salt(boto3_vpc_state, salt_map):
        result = boto3_vpc_state.absent("myvpc")
    assert result["result"] is True
    assert result["changes"]["old"] == {"vpc": "vpc-1"}


def test_vpc_absent_delete_failure(mock_salt):
    salt_map = {
        "boto3_vpc.get_id": {"id": "vpc-1"},
        "boto3_vpc.delete": {"deleted": False, "error": {"message": "kaboom"}},
    }
    with mock_salt(boto3_vpc_state, salt_map):
        result = boto3_vpc_state.absent("myvpc")
    assert result["result"] is False
    assert "kaboom" in result["comment"]


def test_dhcp_test_mode(mock_salt):
    salt_map = {"boto3_vpc.dhcp_options_exists": {"exists": False}}
    with mock_salt(boto3_vpc_state, salt_map, test=True):
        result = boto3_vpc_state.dhcp_options_present("myopts", domain_name="example.com")
    assert result["result"] is None
    assert result["changes"]["new"]["dhcp_options"]["domain_name"] == "example.com"


def test_dhcp_create(mock_salt):
    salt_map = {
        "boto3_vpc.dhcp_options_exists": {"exists": False},
        "boto3_vpc.create_dhcp_options": {"created": True, "id": "dopt-1"},
    }
    with mock_salt(boto3_vpc_state, salt_map):
        result = boto3_vpc_state.dhcp_options_present("myopts", domain_name="example.com")
    assert result["result"] is True
    assert result["changes"]["new"]["dhcp_options"]["domain_name"] == "example.com"


def test_dhcp_delete(mock_salt):
    salt_map = {
        "boto3_vpc.get_resource_id": {"id": "dopt-1"},
        "boto3_vpc.delete_dhcp_options": {"deleted": True},
    }
    with mock_salt(boto3_vpc_state, salt_map):
        result = boto3_vpc_state.dhcp_options_absent(name="myopts")
    assert result["result"] is True
    assert result["changes"]["new"] == {"dhcp_options": None}


def test_igw_created(mock_salt):
    salt_map = {
        "boto3_vpc.resource_exists": {"exists": False},
        "boto3_vpc.create_internet_gateway": {"created": True, "id": "igw-1"},
    }
    with mock_salt(boto3_vpc_state, salt_map):
        result = boto3_vpc_state.internet_gateway_present("myigw", vpc_name="myvpc")
    assert result["result"] is True
    assert result["changes"]["new"] == {"internet_gateway": "igw-1"}


def test_igw_absent_missing(mock_salt):
    with mock_salt(boto3_vpc_state, {"boto3_vpc.get_resource_id": {"id": None}}):
        result = boto3_vpc_state.internet_gateway_absent("myigw")
    assert result["result"] is True
    assert "does not exist" in result["comment"]


def test_igw_absent_delete(mock_salt):
    salt_map = {
        "boto3_vpc.get_resource_id": {"id": "igw-1"},
        "boto3_vpc.delete_internet_gateway": {"deleted": True},
    }
    with mock_salt(boto3_vpc_state, salt_map):
        result = boto3_vpc_state.internet_gateway_absent("myigw")
    assert result["result"] is True
    assert result["changes"]["old"] == {"internet_gateway": "igw-1"}


def test_route_table_create(mock_salt):
    describe = MagicMock(
        side_effect=[
            [{"id": "rtb-1", "routes": [], "associations": []}],
            [{"id": "rtb-1", "routes": [], "associations": []}],
        ]
    )
    salt_map = {
        "boto3_vpc.get_resource_id": {"id": None},
        "boto3_vpc.create_route_table": {"created": True, "id": "rtb-1"},
        "boto3_vpc.describe_route_tables": describe,
    }
    with mock_salt(boto3_vpc_state, salt_map):
        result = boto3_vpc_state.route_table_present("myrt", vpc_name="myvpc")
    assert result["result"] is True
    assert result["changes"]["new"] == {"route_table": "rtb-1"}


def test_route_table_create_with_routes(mock_salt):
    describe = MagicMock(
        side_effect=[
            [
                {
                    "id": "rtb-1",
                    "routes": [{"destination_cidr_block": "10.0.0.0/16", "gateway_id": "local"}],
                    "associations": [],
                }
            ],
            [
                {
                    "id": "rtb-1",
                    "routes": [
                        {"destination_cidr_block": "10.0.0.0/16", "gateway_id": "local"},
                        {"destination_cidr_block": "0.0.0.0/0", "gateway_id": "igw-1"},
                    ],
                    "associations": [],
                }
            ],
            [{"id": "rtb-1", "routes": [], "associations": []}],
        ]
    )
    salt_map = {
        "boto3_vpc.get_resource_id": MagicMock(side_effect=[{"id": "rtb-1"}, {"id": "igw-1"}]),
        "boto3_vpc.describe_route_tables": describe,
        "boto3_vpc.create_route": {"created": True},
    }
    routes = [{"destination_cidr_block": "0.0.0.0/0", "internet_gateway_name": "myigw"}]
    with mock_salt(boto3_vpc_state, salt_map):
        result = boto3_vpc_state.route_table_present("myrt", vpc_name="myvpc", routes=routes)
    assert result["result"] is True


def test_route_table_absent_missing(mock_salt):
    with mock_salt(boto3_vpc_state, {"boto3_vpc.get_resource_id": {"id": None}}):
        result = boto3_vpc_state.route_table_absent("myrt")
    assert result["result"] is True
    assert "does not exist" in result["comment"]


def test_route_table_absent_delete(mock_salt):
    salt_map = {
        "boto3_vpc.get_resource_id": {"id": "rtb-1"},
        "boto3_vpc.delete_route_table": {"deleted": True},
    }
    with mock_salt(boto3_vpc_state, salt_map):
        result = boto3_vpc_state.route_table_absent("myrt")
    assert result["result"] is True
    assert result["changes"]["old"] == {"route_table": "rtb-1"}


def test_nat_present_created(mock_salt):
    salt_map = {
        "boto3_vpc.describe_nat_gateways": [],
        "boto3_vpc.create_nat_gateway": {"created": True, "id": "nat-1"},
    }
    with mock_salt(boto3_vpc_state, salt_map):
        result = boto3_vpc_state.nat_gateway_present("mynat", subnet_name="mysub")
    assert result["result"] is True
    assert result["changes"]["new"] == {"nat_gateway": "nat-1"}


def test_nat_present_already_exists(mock_salt):
    salt_map = {"boto3_vpc.describe_nat_gateways": [{"NatGatewayId": "nat-1"}]}
    with mock_salt(boto3_vpc_state, salt_map):
        result = boto3_vpc_state.nat_gateway_present("mynat", subnet_name="mysub")
    assert result["result"] is True
    assert "nat-1" in result["comment"]


def test_peering_accept_no_pending(mock_salt):
    salt_map = {"boto3_vpc.is_peering_connection_pending": False}
    with mock_salt(boto3_vpc_state, salt_map):
        result = boto3_vpc_state.accept_vpc_peering_connection(name="mystate", conn_name="mypeer")
    assert result["result"] is True
    assert "Nothing to be done" in result["changes"]["old"]


def test_peering_accept_pending(mock_salt):
    salt_map = {
        "boto3_vpc.is_peering_connection_pending": True,
        "boto3_vpc.accept_vpc_peering_connection": {"msg": "accepted"},
    }
    with mock_salt(boto3_vpc_state, salt_map):
        result = boto3_vpc_state.accept_vpc_peering_connection(name="mystate", conn_name="mypeer")
    assert result["result"] is True
    assert result["changes"]["new"] == "accepted"


def test_peering_request_new(mock_salt):
    salt_map = {
        "boto3_vpc.describe_vpc_peering_connection": {"VPC-Peerings": []},
        "boto3_vpc.request_vpc_peering_connection": {"msg": "requested"},
    }
    with mock_salt(boto3_vpc_state, salt_map):
        result = boto3_vpc_state.request_vpc_peering_connection(
            "mystate",
            requester_vpc_id="vpc-1",
            peer_vpc_id="vpc-2",
            conn_name="mypeer",
        )
    assert result["result"] is True
    assert result["changes"]["new"] == "requested"


def test_peering_request_existing(mock_salt):
    salt_map = {"boto3_vpc.describe_vpc_peering_connection": {"VPC-Peerings": ["pcx-1"]}}
    with mock_salt(boto3_vpc_state, salt_map):
        result = boto3_vpc_state.request_vpc_peering_connection(
            "mystate",
            requester_vpc_id="vpc-1",
            peer_vpc_id="vpc-2",
            conn_name="mypeer",
        )
    assert result["result"] is True
    assert "already exists" in result["comment"]


def test_peering_delete_by_name(mock_salt):
    salt_map = {
        "boto3_vpc.describe_vpc_peering_connection": {"VPC-Peerings": ["pcx-1"]},
        "boto3_vpc.delete_vpc_peering_connection": {"msg": "deleted"},
    }
    with mock_salt(boto3_vpc_state, salt_map):
        result = boto3_vpc_state.delete_vpc_peering_connection("mystate", conn_name="mypeer")
    assert result["result"] is True


def test_peering_delete_not_found(mock_salt):
    salt_map = {"boto3_vpc.describe_vpc_peering_connection": {"VPC-Peerings": []}}
    with mock_salt(boto3_vpc_state, salt_map):
        result = boto3_vpc_state.delete_vpc_peering_connection("mystate", conn_name="mypeer")
    assert result["result"] is True
    assert "nothing to be done" in result["comment"].lower()


def test_peering_pending_from_vpc(mock_salt):
    salt_map = {
        "boto3_vpc.is_peering_connection_pending": True,
        "boto3_vpc.peering_connection_pending_from_vpc": True,
    }
    with mock_salt(boto3_vpc_state, salt_map):
        result = boto3_vpc_state.vpc_peering_connection_present(
            "mystate", requester_vpc_id="vpc-1", peer_vpc_id="vpc-2", conn_name="mypeer"
        )
    assert result["result"] is True
    assert "pending acceptance" in result["comment"]


def test_peering_absent_delegates(mock_salt):
    salt_map = {"boto3_vpc.describe_vpc_peering_connection": {"VPC-Peerings": []}}
    with mock_salt(boto3_vpc_state, salt_map):
        result = boto3_vpc_state.vpc_peering_connection_absent("mystate", conn_name="mypeer")
    assert result["result"] is True
