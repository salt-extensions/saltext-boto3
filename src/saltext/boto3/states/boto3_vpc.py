"""
Manage VPCs using boto3.
========================

    Renamed from ``boto_vpc`` to ``boto3_vpc`` and updated to call the
    refactored ``boto3_vpc`` execution module.

Create and destroy VPCs. Be aware that this interacts with Amazon's
services, and so may incur charges.

:depends:
  - boto3 >= 1.28.0
  - botocore >= 1.31.0

This module uses ``boto3``, which can be installed via package, or pip.

This module accepts explicit VPC credentials but can also utilize
IAM roles assigned to the instance through Instance Profiles. Dynamic
credentials are then automatically obtained from AWS API and no further
configuration is necessary. More information available `here
<http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html>`_.

If IAM roles are not used you need to specify them either in a pillar file or
in the minion's config file:

.. code-block:: yaml

    vpc.keyid: GKTADJGHEIQSXMKKRBJ08H
    vpc.key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs

It's also possible to specify ``key``, ``keyid`` and ``region`` via a profile, either
passed in as a dict, or as a string to pull from pillars or minion config:

.. code-block:: yaml

    myprofile:
        keyid: GKTADJGHEIQSXMKKRBJ08H
        key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs
        region: us-east-1

.. code-block:: yaml

    Ensure VPC exists:
      boto3_vpc.present:
        - name: myvpc
        - cidr_block: 10.10.11.0/24
        - region: us-east-1

    Ensure subnet exists:
      boto3_vpc.subnet_present:
        - name: mysubnet
        - vpc_id: vpc-123456
        - cidr_block: 10.0.0.0/16
        - region: us-east-1
        - profile: myprofile

.. versionadded:: 1.0.0
"""

import logging

from salt.utils import dictupdate

__virtualname__ = "boto3_vpc"

log = logging.getLogger(__name__)


def __virtual__():
    """
    Only load if the boto3_vpc execution module is available.
    """
    if "boto3_vpc.exists" in __salt__:
        return __virtualname__
    return (
        False,
        "The boto3_vpc state module requires the boto3_vpc execution module.",
    )


def present(
    name,
    cidr_block,
    instance_tenancy=None,
    dns_support=None,
    dns_hostnames=None,
    tags=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Ensure a VPC with the supplied properties exists.

    name
        Name of the VPC.

    cidr_block
        The range of IPs in CIDR format, e.g. ``10.0.0.0/24``.

    instance_tenancy
        Tenancy for instances launched in this VPC (``default`` or
        ``dedicated``).

    dns_support
        Whether DNS resolution is supported for the VPC.

    dns_hostnames
        Whether instances launched in the VPC receive DNS hostnames.

    tags
        Dict of tag key/values to apply.

    region, key, keyid, profile
        Standard boto3 connection arguments.

    Example:

    .. code-block:: yaml

        ensure-present:
          boto3_vpc.present:
            - name: example

    """
    ret = {"name": name, "result": True, "comment": "", "changes": {}}

    r = __salt__["boto3_vpc.exists"](
        name=name, tags=tags, region=region, key=key, keyid=keyid, profile=profile
    )
    if "error" in r:
        ret["result"] = False
        ret["comment"] = "Failed to create VPC: {}.".format(r["error"]["message"])
        return ret

    if r.get("exists"):
        ret["comment"] = "VPC present."
        return ret

    if __opts__["test"]:
        ret["comment"] = f"VPC {name} is set to be created."
        ret["result"] = None
        ret["changes"] = {"old": {"vpc": None}, "new": {"vpc": name}}
        return ret

    r = __salt__["boto3_vpc.create"](
        cidr_block,
        instance_tenancy=instance_tenancy,
        vpc_name=name,
        enable_dns_support=dns_support,
        enable_dns_hostnames=dns_hostnames,
        tags=tags,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )
    if not r.get("created"):
        ret["result"] = False
        ret["comment"] = "Error in creating VPC: {}.".format(r["error"]["message"])
        return ret
    described = __salt__["boto3_vpc.describe"](
        vpc_id=r["id"], region=region, key=key, keyid=keyid, profile=profile
    )
    ret["changes"]["old"] = {"vpc": None}
    ret["changes"]["new"] = described
    ret["comment"] = f"VPC {name} created."
    return ret


def absent(name, tags=None, region=None, key=None, keyid=None, profile=None):
    """
    Ensure the named VPC is absent.

    name
        Name of the VPC.

    tags
        Optional tag filter; all tags must match.

    Example:

    .. code-block:: yaml

        ensure-absent:
          boto3_vpc.absent:
            - name: example

    """
    ret = {"name": name, "result": True, "comment": "", "changes": {}}

    r = __salt__["boto3_vpc.get_id"](
        name=name, tags=tags, region=region, key=key, keyid=keyid, profile=profile
    )
    if "error" in r:
        ret["result"] = False
        ret["comment"] = "Failed to delete VPC: {}.".format(r["error"]["message"])
        return ret
    vpc_id = r.get("id")
    if not vpc_id:
        ret["comment"] = f"{name} VPC does not exist."
        return ret

    if __opts__["test"]:
        ret["comment"] = f"VPC {name} is set to be removed."
        ret["result"] = None
        ret["changes"] = {"old": {"vpc": vpc_id}, "new": {"vpc": None}}
        return ret

    r = __salt__["boto3_vpc.delete"](
        vpc_name=name, tags=tags, region=region, key=key, keyid=keyid, profile=profile
    )
    if not r.get("deleted"):
        ret["result"] = False
        ret["comment"] = "Failed to delete VPC: {}.".format(r["error"]["message"])
        return ret
    ret["changes"]["old"] = {"vpc": vpc_id}
    ret["changes"]["new"] = {"vpc": None}
    ret["comment"] = f"VPC {name} deleted."
    return ret


def dhcp_options_present(
    name,
    dhcp_options_id=None,
    vpc_name=None,
    vpc_id=None,
    domain_name=None,
    domain_name_servers=None,
    ntp_servers=None,
    netbios_name_servers=None,
    netbios_node_type=None,
    tags=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Ensure a DHCP options set with the given settings exists.

    .. note::
        This implementation only sets values during option set creation. It
        cannot update an existing option set in place.

    name
        Name of the DHCP options set.

    Example:

    .. code-block:: yaml

        ensure-dhcp-options-present:
          boto3_vpc.dhcp_options_present:
            - name: example

    """
    ret = {"name": name, "result": True, "comment": "", "changes": {}}
    new = {
        "domain_name": domain_name,
        "domain_name_servers": domain_name_servers,
        "ntp_servers": ntp_servers,
        "netbios_name_servers": netbios_name_servers,
        "netbios_node_type": netbios_node_type,
    }

    r = __salt__["boto3_vpc.dhcp_options_exists"](
        dhcp_options_id=dhcp_options_id,
        dhcp_options_name=name,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )
    if "error" in r:
        ret["result"] = False
        ret["comment"] = "Failed to validate DHCP options: {}.".format(r["error"]["message"])
        return ret

    if r.get("exists"):
        ret["comment"] = "DHCP options already present."
        return ret

    if __opts__["test"]:
        ret["comment"] = f"DHCP options {name} are set to be created."
        ret["result"] = None
        ret["changes"] = {"old": {"dhcp_options": None}, "new": {"dhcp_options": new}}
        return ret

    r = __salt__["boto3_vpc.create_dhcp_options"](
        domain_name=domain_name,
        domain_name_servers=domain_name_servers,
        ntp_servers=ntp_servers,
        netbios_name_servers=netbios_name_servers,
        netbios_node_type=netbios_node_type,
        dhcp_options_name=name,
        tags=tags,
        vpc_id=vpc_id,
        vpc_name=vpc_name,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )
    if not r.get("created"):
        ret["result"] = False
        ret["comment"] = "Failed to create DHCP options: {}".format(r["error"]["message"])
        return ret
    ret["changes"]["old"] = {"dhcp_options": None}
    ret["changes"]["new"] = {"dhcp_options": new}
    ret["comment"] = f"DHCP options {name} created."
    return ret


def dhcp_options_absent(
    name=None, dhcp_options_id=None, region=None, key=None, keyid=None, profile=None
):
    """
    Ensure a DHCP options set is absent.

    Example:

    .. code-block:: yaml

        ensure-dhcp-options-absent:
          boto3_vpc.dhcp_options_absent:
            - name: example

    """
    ret = {"name": name, "result": True, "comment": "", "changes": {}}

    if dhcp_options_id:
        r = __salt__["boto3_vpc.dhcp_options_exists"](
            dhcp_options_id=dhcp_options_id,
            region=region,
            key=key,
            keyid=keyid,
            profile=profile,
        )
        rid = dhcp_options_id if r.get("exists") else None
    else:
        r = __salt__["boto3_vpc.get_resource_id"](
            "dhcp_options",
            name=name,
            region=region,
            key=key,
            keyid=keyid,
            profile=profile,
        )
        rid = r.get("id")
    if "error" in r:
        ret["result"] = False
        ret["comment"] = "Failed to delete DHCP options: {}.".format(r["error"]["message"])
        return ret
    if not rid:
        ret["comment"] = f"DHCP options {name} do not exist."
        return ret

    if __opts__["test"]:
        ret["comment"] = f"DHCP options {name} are set to be deleted."
        ret["result"] = None
        ret["changes"] = {
            "old": {"dhcp_options": rid},
            "new": {"dhcp_options": None},
        }
        return ret

    r = __salt__["boto3_vpc.delete_dhcp_options"](
        dhcp_options_id=rid,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )
    if not r.get("deleted"):
        ret["result"] = False
        ret["comment"] = "Failed to delete DHCP options: {}".format(r["error"]["message"])
        return ret
    ret["changes"]["old"] = {"dhcp_options": rid}
    ret["changes"]["new"] = {"dhcp_options": None}
    ret["comment"] = f"DHCP options {name} deleted."
    return ret


def subnet_present(
    name,
    cidr_block,
    vpc_name=None,
    vpc_id=None,
    availability_zone=None,
    tags=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
    auto_assign_public_ipv4=False,
):
    """
    Ensure a subnet exists.

    .. note::
        Route table association is not handled by the boto3_vpc subnet
        states yet; that will land with the route_table port.

    name
        Name of the subnet.

    cidr_block
        The range of IPs for the subnet, in CIDR format.

    vpc_name / vpc_id
        Identify the VPC the subnet belongs to (one is required).

    availability_zone
        Optional AZ to place the subnet in.

    auto_assign_public_ipv4
        If ``True``, instances launched into this subnet will be assigned a
        public IPv4 address by default.

    Example:

    .. code-block:: yaml

        ensure-subnet-present:
          boto3_vpc.subnet_present:
            - name: example

    """
    ret = {"name": name, "result": True, "comment": "", "changes": {}}

    r = __salt__["boto3_vpc.subnet_exists"](
        subnet_name=name,
        tags=tags,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )
    if "error" in r:
        ret["result"] = False
        ret["comment"] = "Failed to create subnet: {}.".format(r["error"]["message"])
        return ret

    if r.get("exists"):
        ret["comment"] = "Subnet present."
        return ret

    if __opts__["test"]:
        ret["comment"] = f"Subnet {name} is set to be created."
        ret["result"] = None
        ret["changes"] = {"old": {"subnet": None}, "new": {"subnet": name}}
        return ret

    r = __salt__["boto3_vpc.create_subnet"](
        subnet_name=name,
        cidr_block=cidr_block,
        availability_zone=availability_zone,
        auto_assign_public_ipv4=auto_assign_public_ipv4,
        vpc_name=vpc_name,
        vpc_id=vpc_id,
        tags=tags,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )
    if not r.get("created"):
        ret["result"] = False
        ret["comment"] = "Failed to create subnet: {}".format(r["error"]["message"])
        return ret
    described = __salt__["boto3_vpc.describe_subnet"](
        subnet_id=r["id"], region=region, key=key, keyid=keyid, profile=profile
    )
    ret["changes"]["old"] = {"subnet": None}
    ret["changes"]["new"] = described
    ret["comment"] = f"Subnet {name} created."
    return ret


def subnet_absent(name=None, subnet_id=None, region=None, key=None, keyid=None, profile=None):
    """
    Ensure a subnet is absent.

    Example:

    .. code-block:: yaml

        ensure-subnet-absent:
          boto3_vpc.subnet_absent:
            - name: example

    """
    ret = {"name": name, "result": True, "comment": "", "changes": {}}

    if subnet_id:
        r = __salt__["boto3_vpc.subnet_exists"](
            subnet_id=subnet_id, region=region, key=key, keyid=keyid, profile=profile
        )
        if "error" in r:
            ret["result"] = False
            ret["comment"] = "Failed to delete subnet: {}.".format(r["error"]["message"])
            return ret
        rid = subnet_id if r.get("exists") else None
    else:
        r = __salt__["boto3_vpc.get_resource_id"](
            "subnet", name=name, region=region, key=key, keyid=keyid, profile=profile
        )
        if "error" in r:
            ret["result"] = False
            ret["comment"] = "Failed to delete subnet: {}.".format(r["error"]["message"])
            return ret
        rid = r.get("id")
    if not rid:
        ret["comment"] = f"{name} subnet does not exist."
        return ret

    if __opts__["test"]:
        ret["comment"] = f"Subnet {name} ({rid}) is set to be removed."
        ret["result"] = None
        ret["changes"] = {"old": {"subnet": rid}, "new": {"subnet": None}}
        return ret

    r = __salt__["boto3_vpc.delete_subnet"](
        subnet_name=name, region=region, key=key, keyid=keyid, profile=profile
    )
    if not r.get("deleted"):
        ret["result"] = False
        ret["comment"] = "Failed to delete subnet: {}".format(r["error"]["message"])
        return ret
    ret["changes"]["old"] = {"subnet": rid}
    ret["changes"]["new"] = {"subnet": None}
    ret["comment"] = f"Subnet {name} deleted."
    return ret


def internet_gateway_present(
    name,
    vpc_name=None,
    vpc_id=None,
    tags=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Ensure an internet gateway exists.

    name
        Name of the internet gateway.

    vpc_name
        Name of the VPC to which the internet gateway should be attached.

    vpc_id
        Id of the VPC to which the internet_gateway should be attached.
        Only one of vpc_name or vpc_id may be provided.

    tags
        A list of tags.

    region, key, keyid, profile
        Standard boto3 connection arguments.

    Example:

    .. code-block:: yaml

        ensure-internet-gateway-present:
          boto3_vpc.internet_gateway_present:
            - name: example

    """
    ret = {"name": name, "result": True, "comment": "", "changes": {}}

    r = __salt__["boto3_vpc.resource_exists"](
        "internet_gateway",
        name=name,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )
    if "error" in r:
        ret["result"] = False
        ret["comment"] = "Failed to create internet gateway: {}.".format(r["error"]["message"])
        return ret

    if not r.get("exists"):
        if __opts__["test"]:
            ret["comment"] = f"Internet gateway {name} is set to be created."
            ret["result"] = None
            return ret
        r = __salt__["boto3_vpc.create_internet_gateway"](
            internet_gateway_name=name,
            vpc_name=vpc_name,
            vpc_id=vpc_id,
            tags=tags,
            region=region,
            key=key,
            keyid=keyid,
            profile=profile,
        )
        if not r.get("created"):
            ret["result"] = False
            ret["comment"] = "Failed to create internet gateway: {}".format(r["error"]["message"])
            return ret
        ret["changes"]["old"] = {"internet_gateway": None}
        ret["changes"]["new"] = {"internet_gateway": r["id"]}
        ret["comment"] = f"Internet gateway {name} created."
        return ret
    ret["comment"] = f"Internet gateway {name} present."
    return ret


def internet_gateway_absent(name, detach=False, region=None, key=None, keyid=None, profile=None):
    """
    Ensure the named internet gateway is absent.

    name
        Name of the internet gateway.

    detach
        First detach the internet gateway from a VPC, if attached.

    Example:

    .. code-block:: yaml

        ensure-internet-gateway-absent:
          boto3_vpc.internet_gateway_absent:
            - name: example

    """
    ret = {"name": name, "result": True, "comment": "", "changes": {}}

    r = __salt__["boto3_vpc.get_resource_id"](
        "internet_gateway",
        name=name,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )
    if "error" in r:
        ret["result"] = False
        ret["comment"] = "Failed to delete internet gateway: {}.".format(r["error"]["message"])
        return ret

    igw_id = r.get("id")
    if not igw_id:
        ret["comment"] = f"Internet gateway {name} does not exist."
        return ret

    if __opts__["test"]:
        ret["comment"] = f"Internet gateway {name} is set to be removed."
        ret["result"] = None
        return ret

    r = __salt__["boto3_vpc.delete_internet_gateway"](
        internet_gateway_name=name,
        detach=detach,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )
    if not r.get("deleted"):
        ret["result"] = False
        ret["comment"] = "Failed to delete internet gateway: {}.".format(r["error"]["message"])
        return ret
    ret["changes"]["old"] = {"internet_gateway": igw_id}
    ret["changes"]["new"] = {"internet_gateway": None}
    ret["comment"] = f"Internet gateway {name} deleted."
    return ret


def route_table_present(
    name,
    vpc_name=None,
    vpc_id=None,
    routes=None,
    subnet_ids=None,
    subnet_names=None,
    tags=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Ensure route table with routes exists and is associated to a VPC.

    Example:

    .. code-block:: yaml

        boto3_vpc.route_table_present:
          - name: my_route_table
          - vpc_id: vpc-123456
          - routes:
            - destination_cidr_block: 0.0.0.0/0
              internet_gateway_name: InternetGateway
            - destination_cidr_block: 10.10.11.0/24
              instance_id: i-123456
            - destination_cidr_block: 10.10.12.0/24
              interface_id: eni-123456
          - subnet_names:
            - subnet1
            - subnet2
    """
    ret = {"name": name, "result": True, "comment": "", "changes": {}}

    _ret = _route_table_present(
        name=name,
        vpc_name=vpc_name,
        vpc_id=vpc_id,
        tags=tags,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )
    ret["changes"] = _ret["changes"]
    ret["comment"] = " ".join([ret["comment"], _ret["comment"]])
    if not _ret["result"]:
        ret["result"] = _ret["result"]
        if ret["result"] is False:
            return ret
        if ret["result"] is None and __opts__["test"]:
            return ret

    _ret = _routes_present(
        route_table_name=name,
        routes=routes,
        tags=tags,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )
    ret["changes"] = dictupdate.update(ret["changes"], _ret["changes"])
    ret["comment"] = " ".join([ret["comment"], _ret["comment"]])
    if not _ret["result"]:
        ret["result"] = _ret["result"]
        if ret["result"] is False:
            return ret

    _ret = _subnets_present(
        route_table_name=name,
        subnet_ids=subnet_ids,
        subnet_names=subnet_names,
        tags=tags,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )
    ret["changes"] = dictupdate.update(ret["changes"], _ret["changes"])
    ret["comment"] = " ".join([ret["comment"], _ret["comment"]])
    if not _ret["result"]:
        ret["result"] = _ret["result"]
        if ret["result"] is False:
            return ret
    return ret


def _route_table_present(
    name,
    vpc_name=None,
    vpc_id=None,
    tags=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    ret = {"name": name, "result": True, "comment": "", "changes": {}}

    r = __salt__["boto3_vpc.get_resource_id"](
        resource="route_table",
        name=name,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )
    if "error" in r:
        ret["result"] = False
        ret["comment"] = "Failed to create route table: {}.".format(r["error"]["message"])
        return ret

    _id = r.get("id")

    if not _id:
        if __opts__["test"]:
            ret["comment"] = f"Route table {name} is set to be created."
            ret["result"] = None
            return ret

        r = __salt__["boto3_vpc.create_route_table"](
            route_table_name=name,
            vpc_name=vpc_name,
            vpc_id=vpc_id,
            tags=tags,
            region=region,
            key=key,
            keyid=keyid,
            profile=profile,
        )
        if not r.get("created"):
            ret["result"] = False
            ret["comment"] = "Failed to create route table: {}.".format(r["error"]["message"])
            return ret

        ret["changes"]["old"] = {"route_table": None}
        ret["changes"]["new"] = {"route_table": r["id"]}
        ret["comment"] = f"Route table {name} created."
        return ret
    ret["comment"] = f"Route table {name} ({_id}) present."
    return ret


def _routes_present(
    route_table_name, routes, tags=None, region=None, key=None, keyid=None, profile=None
):
    ret = {"name": route_table_name, "result": True, "comment": "", "changes": {}}

    tables = __salt__["boto3_vpc.describe_route_tables"](
        route_table_name=route_table_name,
        tags=tags,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )
    if isinstance(tables, dict) and "error" in tables:
        ret["comment"] = (
            f"Could not retrieve configuration for route table {route_table_name}: "
            f"{tables['error']['message']}."
        )
        ret["result"] = False
        return ret
    if not tables:
        ret["comment"] = f"Could not retrieve configuration for route table {route_table_name}."
        ret["result"] = False
        return ret

    route_table = tables[0]

    _routes = []
    if routes:
        route_keys = {
            "gateway_id",
            "instance_id",
            "destination_cidr_block",
            "interface_id",
            "vpc_peering_connection_id",
            "nat_gateway_id",
        }
        for i in routes:
            _r = {k: v for k, v in i.items() if k in route_keys}
            if i.get("internet_gateway_name"):
                r = __salt__["boto3_vpc.get_resource_id"](
                    "internet_gateway",
                    name=i["internet_gateway_name"],
                    region=region,
                    key=key,
                    keyid=keyid,
                    profile=profile,
                )
                if "error" in r:
                    ret["comment"] = (
                        "Error looking up id for internet gateway "
                        f"{i['internet_gateway_name']}: {r['error']['message']}"
                    )
                    ret["result"] = False
                    return ret
                if r["id"] is None:
                    ret["comment"] = (
                        f"Internet gateway {i['internet_gateway_name']} does not exist."
                    )
                    ret["result"] = False
                    return ret
                _r["gateway_id"] = r["id"]
            if i.get("vpc_peering_connection_name"):
                r = __salt__["boto3_vpc.get_resource_id"](
                    "vpc_peering_connection",
                    name=i["vpc_peering_connection_name"],
                    region=region,
                    key=key,
                    keyid=keyid,
                    profile=profile,
                )
                if "error" in r:
                    ret["comment"] = (
                        "Error looking up id for VPC peering connection "
                        f"{i['vpc_peering_connection_name']}: {r['error']['message']}"
                    )
                    ret["result"] = False
                    return ret
                if r["id"] is None:
                    ret["comment"] = (
                        f"VPC peering connection {i['vpc_peering_connection_name']} does not exist."
                    )
                    ret["result"] = False
                    return ret
                _r["vpc_peering_connection_id"] = r["id"]
            if i.get("instance_name"):
                running_states = ("pending", "rebooting", "running", "stopping", "stopped")
                r = __salt__["boto3_ec2.get_id"](
                    name=i["instance_name"],
                    region=region,
                    key=key,
                    keyid=keyid,
                    profile=profile,
                    in_states=running_states,
                )
                if r is None:
                    ret["comment"] = f"Instance {i['instance_name']} does not exist."
                    ret["result"] = False
                    return ret
                _r["instance_id"] = r
            if i.get("nat_gateway_subnet_name"):
                r = __salt__["boto3_vpc.describe_nat_gateways"](
                    subnet_name=i["nat_gateway_subnet_name"],
                    region=region,
                    key=key,
                    keyid=keyid,
                    profile=profile,
                )
                if not r:
                    ret["comment"] = "Nat gateway does not exist."
                    ret["result"] = False
                    return ret
                _r["nat_gateway_id"] = r[0]["NatGatewayId"]
            _routes.append(_r)

    to_delete = []
    to_create = []
    for route in _routes:
        if route not in route_table["routes"]:
            to_create.append(dict(route))
    for route in route_table["routes"]:
        if route not in _routes:
            if route.get("gateway_id") != "local":
                to_delete.append(route)

    if to_create or to_delete:
        if __opts__["test"]:
            ret["comment"] = f"Route table {route_table_name} set to have routes modified."
            ret["result"] = None
            return ret
        if to_delete:
            for r in to_delete:
                res = __salt__["boto3_vpc.delete_route"](
                    route_table_id=route_table["id"],
                    destination_cidr_block=r["destination_cidr_block"],
                    region=region,
                    key=key,
                    keyid=keyid,
                    profile=profile,
                )
                if not res.get("deleted"):
                    ret["comment"] = (
                        f"Failed to delete route {r['destination_cidr_block']} "
                        f"from route table {route_table_name}: {res['error']['message']}."
                    )
                    ret["result"] = False
                    return ret
                ret["comment"] = (
                    f"Deleted route {r['destination_cidr_block']} from route table "
                    f"{route_table_name}."
                )
        if to_create:
            for r in to_create:
                res = __salt__["boto3_vpc.create_route"](
                    route_table_id=route_table["id"],
                    region=region,
                    key=key,
                    keyid=keyid,
                    profile=profile,
                    **r,
                )
                if not res.get("created"):
                    ret["comment"] = (
                        f"Failed to create route {r['destination_cidr_block']} "
                        f"in route table {route_table_name}: {res['error']['message']}."
                    )
                    ret["result"] = False
                    return ret
                ret["comment"] = (
                    f"Created route {r['destination_cidr_block']} in route table "
                    f"{route_table_name}."
                )
        ret["changes"]["old"] = {"routes": route_table["routes"]}
        new_tables = __salt__["boto3_vpc.describe_route_tables"](
            route_table_name=route_table_name,
            tags=tags,
            region=region,
            key=key,
            keyid=keyid,
            profile=profile,
        )
        ret["changes"]["new"] = {"routes": new_tables[0]["routes"]}
    return ret


def _subnets_present(
    route_table_name,
    subnet_ids=None,
    subnet_names=None,
    tags=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    ret = {"name": route_table_name, "result": True, "comment": "", "changes": {}}

    if not subnet_ids:
        subnet_ids = []

    if subnet_names:
        for i in subnet_names:
            r = __salt__["boto3_vpc.get_resource_id"](
                "subnet", name=i, region=region, key=key, keyid=keyid, profile=profile
            )
            if "error" in r:
                ret["comment"] = "Error looking up subnet ids: {}".format(r["error"]["message"])
                ret["result"] = False
                return ret
            if r["id"] is None:
                ret["comment"] = f"Subnet {i} does not exist."
                ret["result"] = False
                return ret
            subnet_ids.append(r["id"])

    tables = __salt__["boto3_vpc.describe_route_tables"](
        route_table_name=route_table_name,
        tags=tags,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )
    if isinstance(tables, dict) and "error" in tables:
        ret["comment"] = (
            f"Could not retrieve configuration for route table {route_table_name}: "
            f"{tables['error']['message']}."
        )
        ret["result"] = False
        return ret
    if not tables:
        ret["comment"] = f"Could not retrieve configuration for route table {route_table_name}."
        ret["result"] = False
        return ret

    route_table = tables[0]
    assoc_ids = [x.get("subnet_id") for x in route_table["associations"]]

    to_create = [x for x in subnet_ids if x not in assoc_ids]
    to_delete = []
    for x in route_table["associations"]:
        if x.get("subnet_id") not in subnet_ids and x.get("subnet_id") is not None:
            to_delete.append(x["id"])

    if to_create or to_delete:
        if __opts__["test"]:
            ret["comment"] = (
                f"Subnet associations for route table {route_table_name} set to be modified."
            )
            ret["result"] = None
            return ret
        if to_delete:
            for r_asc in to_delete:
                r = __salt__["boto3_vpc.disassociate_route_table"](
                    r_asc, region, key, keyid, profile
                )
                if "error" in r:
                    ret["comment"] = (
                        f"Failed to dissociate {r_asc} from route table "
                        f"{route_table_name}: {r['error']['message']}."
                    )
                    ret["result"] = False
                    return ret
                ret["comment"] = f"Dissociated subnet {r_asc} from route table {route_table_name}."
        if to_create:
            for sn in to_create:
                r = __salt__["boto3_vpc.associate_route_table"](
                    route_table_id=route_table["id"],
                    subnet_id=sn,
                    region=region,
                    key=key,
                    keyid=keyid,
                    profile=profile,
                )
                if "error" in r:
                    ret["comment"] = (
                        f"Failed to associate subnet {sn} with route table "
                        f"{route_table_name}: {r['error']['message']}."
                    )
                    ret["result"] = False
                    return ret
                ret["comment"] = f"Associated subnet {sn} with route table {route_table_name}."
        ret["changes"]["old"] = {"subnets_associations": route_table["associations"]}
        new_tables = __salt__["boto3_vpc.describe_route_tables"](
            route_table_name=route_table_name,
            tags=tags,
            region=region,
            key=key,
            keyid=keyid,
            profile=profile,
        )
        ret["changes"]["new"] = {"subnets_associations": new_tables[0]["associations"]}
    return ret


def route_table_absent(name, region=None, key=None, keyid=None, profile=None):
    """
    Ensure the named route table is absent.

    Example:

    .. code-block:: yaml

        ensure-route-table-absent:
          boto3_vpc.route_table_absent:
            - name: example

    """
    ret = {"name": name, "result": True, "comment": "", "changes": {}}

    r = __salt__["boto3_vpc.get_resource_id"](
        "route_table", name=name, region=region, key=key, keyid=keyid, profile=profile
    )
    if "error" in r:
        ret["result"] = False
        ret["comment"] = r["error"]["message"]
        return ret

    rtbl_id = r.get("id")
    if not rtbl_id:
        ret["comment"] = f"Route table {name} does not exist."
        return ret

    if __opts__["test"]:
        ret["comment"] = f"Route table {name} is set to be removed."
        ret["result"] = None
        return ret

    r = __salt__["boto3_vpc.delete_route_table"](
        route_table_name=name, region=region, key=key, keyid=keyid, profile=profile
    )
    if "error" in r:
        ret["result"] = False
        ret["comment"] = "Failed to delete route table: {}".format(r["error"]["message"])
        return ret
    ret["changes"]["old"] = {"route_table": rtbl_id}
    ret["changes"]["new"] = {"route_table": None}
    ret["comment"] = f"Route table {name} deleted."
    return ret


def nat_gateway_present(
    name,
    subnet_name=None,
    subnet_id=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
    allocation_id=None,
):
    """
    Ensure a nat gateway exists within the specified subnet.


    Example:

    .. code-block:: yaml

        boto3_vpc.nat_gateway_present:
          - subnet_name: my-subnet
    """
    ret = {"name": name, "result": True, "comment": "", "changes": {}}

    r = __salt__["boto3_vpc.describe_nat_gateways"](
        subnet_name=subnet_name,
        subnet_id=subnet_id,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )
    if not r:
        if __opts__["test"]:
            ret["comment"] = "Nat gateway is set to be created."
            ret["result"] = None
            return ret

        r = __salt__["boto3_vpc.create_nat_gateway"](
            subnet_name=subnet_name,
            subnet_id=subnet_id,
            region=region,
            key=key,
            keyid=keyid,
            profile=profile,
            allocation_id=allocation_id,
        )
        if not r.get("created"):
            ret["result"] = False
            ret["comment"] = "Failed to create nat gateway: {}.".format(r["error"]["message"])
            return ret

        ret["changes"]["old"] = {"nat_gateway": None}
        ret["changes"]["new"] = {"nat_gateway": r["id"]}
        ret["comment"] = "Nat gateway created."
        return ret

    inst = r[0]
    _id = inst.get("NatGatewayId")
    ret["comment"] = f"Nat gateway {_id} present."
    return ret


def nat_gateway_absent(
    name=None,
    subnet_name=None,
    subnet_id=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
    wait_for_delete_retries=0,
):
    """
    Ensure the nat gateway in the named subnet is absent.

    Example:

    .. code-block:: yaml

        ensure-nat-gateway-absent:
          boto3_vpc.nat_gateway_absent:
            - name: example

    """
    ret = {"name": name, "result": True, "comment": "", "changes": {}}

    r = __salt__["boto3_vpc.describe_nat_gateways"](
        subnet_name=subnet_name,
        subnet_id=subnet_id,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )
    if not r:
        ret["comment"] = "Nat gateway does not exist."
        return ret

    if __opts__["test"]:
        ret["comment"] = "Nat gateway is set to be removed."
        ret["result"] = None
        return ret

    rtbl_id = None
    for gw in r:
        rtbl_id = gw.get("NatGatewayId")
        res = __salt__["boto3_vpc.delete_nat_gateway"](
            nat_gateway_id=rtbl_id,
            release_eips=True,
            region=region,
            key=key,
            keyid=keyid,
            profile=profile,
            wait_for_delete=True,
            wait_for_delete_retries=wait_for_delete_retries,
        )
        if "error" in res:
            ret["result"] = False
            ret["comment"] = "Failed to delete nat gateway: {}".format(res["error"]["message"])
            return ret
        ret["comment"] = ", ".join((ret["comment"], f"Nat gateway {rtbl_id} deleted."))
    ret["changes"]["old"] = {"nat_gateway": rtbl_id}
    ret["changes"]["new"] = {"nat_gateway": None}
    return ret


def accept_vpc_peering_connection(
    name=None,
    conn_id=None,
    conn_name=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Accept a VPC pending requested peering connection between two VPCs.


    Example:

    .. code-block:: yaml

        boto3_vpc.accept_vpc_peering_connection:
          - conn_name: salt_peering_connection
    """
    log.debug("Called state to accept VPC peering connection")
    pending = __salt__["boto3_vpc.is_peering_connection_pending"](
        conn_id=conn_id,
        conn_name=conn_name,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )

    ret = {
        "name": name,
        "result": True,
        "changes": {},
        "comment": "Boto VPC peering state",
    }

    if not pending:
        ret["result"] = True
        ret["changes"].update(
            {"old": "No pending VPC peering connection found. Nothing to be done."}
        )
        return ret

    if __opts__["test"]:
        ret["changes"].update({"old": "Pending VPC peering connection found and can be accepted"})
        return ret

    result = __salt__["boto3_vpc.accept_vpc_peering_connection"](
        conn_id=conn_id,
        name=conn_name,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )

    if "error" in result:
        ret["comment"] = "Failed to accept VPC peering: {}".format(result["error"])
        ret["result"] = False
        return ret

    ret["changes"].update({"old": "", "new": result["msg"]})
    return ret


def request_vpc_peering_connection(
    name,
    requester_vpc_id=None,
    requester_vpc_name=None,
    peer_vpc_id=None,
    peer_vpc_name=None,
    conn_name=None,
    peer_owner_id=None,
    peer_region=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Request a VPC peering connection between two VPCs.


    Example:

    .. code-block:: yaml

        request a vpc peering connection:
          boto3_vpc.request_vpc_peering_connection:
            - requester_vpc_id: vpc-4b3522e
            - peer_vpc_id: vpc-ae83f9ca
            - conn_name: salt_peering_connection
    """
    log.debug("Called state to request VPC peering connection")
    ret = {
        "name": name,
        "result": True,
        "changes": {},
        "comment": "Boto VPC peering state",
    }
    if conn_name:
        vpc_ids = __salt__["boto3_vpc.describe_vpc_peering_connection"](
            conn_name, region=region, key=key, keyid=keyid, profile=profile
        ).get("VPC-Peerings", [])
    else:
        vpc_ids = []

    if vpc_ids:
        ret["comment"] = "VPC peering connection already exists, nothing to be done."
        return ret

    if __opts__["test"]:
        if not vpc_ids:
            ret["comment"] = "VPC peering connection will be created"
        return ret

    result = __salt__["boto3_vpc.request_vpc_peering_connection"](
        requester_vpc_id=requester_vpc_id,
        requester_vpc_name=requester_vpc_name,
        peer_vpc_id=peer_vpc_id,
        peer_vpc_name=peer_vpc_name,
        name=conn_name,
        peer_owner_id=peer_owner_id,
        peer_region=peer_region,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )

    if "error" in result:
        ret["comment"] = "Failed to request VPC peering: {}".format(result["error"])
        ret["result"] = False
        return ret

    ret["changes"].update({"old": "", "new": result["msg"]})
    return ret


def vpc_peering_connection_present(
    name,
    requester_vpc_id=None,
    requester_vpc_name=None,
    peer_vpc_id=None,
    peer_vpc_name=None,
    conn_name=None,
    peer_owner_id=None,
    peer_region=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Ensure a VPC peering connection is present.

    Example:

    .. code-block:: yaml

        ensure-vpc-peering-connection-present:
          boto3_vpc.vpc_peering_connection_present:
            - name: example

    """
    ret = {"name": name, "result": True, "comment": "", "changes": {}}
    if __salt__["boto3_vpc.is_peering_connection_pending"](
        conn_name=conn_name, region=region, key=key, keyid=keyid, profile=profile
    ):
        if __salt__["boto3_vpc.peering_connection_pending_from_vpc"](
            conn_name=conn_name,
            vpc_id=requester_vpc_id,
            vpc_name=requester_vpc_name,
            region=region,
            key=key,
            keyid=keyid,
            profile=profile,
        ):
            ret["comment"] = (
                f"VPC peering {conn_name} already requested - pending acceptance by "
                f"{peer_owner_id or peer_vpc_name or peer_vpc_id}"
            )
            log.info(ret["comment"])
            return ret
        return accept_vpc_peering_connection(
            name=name,
            conn_name=conn_name,
            region=region,
            key=key,
            keyid=keyid,
            profile=profile,
        )
    return request_vpc_peering_connection(
        name=name,
        requester_vpc_id=requester_vpc_id,
        requester_vpc_name=requester_vpc_name,
        peer_vpc_id=peer_vpc_id,
        peer_vpc_name=peer_vpc_name,
        conn_name=conn_name,
        peer_owner_id=peer_owner_id,
        peer_region=peer_region,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )


def vpc_peering_connection_absent(
    name, conn_id=None, conn_name=None, region=None, key=None, keyid=None, profile=None
):
    """
    Ensure a VPC peering connection is absent.

    Example:

    .. code-block:: yaml

        ensure-vpc-peering-connection-absent:
          boto3_vpc.vpc_peering_connection_absent:
            - name: example

    """
    return delete_vpc_peering_connection(name, conn_id, conn_name, region, key, keyid, profile)


def delete_vpc_peering_connection(
    name, conn_id=None, conn_name=None, region=None, key=None, keyid=None, profile=None
):
    """
    Delete a VPC peering connection.

    Example:

    .. code-block:: yaml

        ensure-delete-vpc-peering-connection:
          boto3_vpc.delete_vpc_peering_connection:
            - name: example

    """
    log.debug("Called state to delete VPC peering connection")
    ret = {
        "name": name,
        "result": True,
        "changes": {},
        "comment": "Boto VPC peering state",
    }
    if conn_name:
        vpc_ids = __salt__["boto3_vpc.describe_vpc_peering_connection"](
            conn_name, region=region, key=key, keyid=keyid, profile=profile
        ).get("VPC-Peerings", [])
    else:
        vpc_ids = [conn_id]

    if not vpc_ids:
        ret["comment"] = "No VPC connection found, nothing to be done."
        return ret

    if __opts__["test"]:
        if vpc_ids:
            ret["comment"] = "VPC peering connection would be deleted"
        return ret

    result = __salt__["boto3_vpc.delete_vpc_peering_connection"](
        conn_id=conn_id,
        conn_name=conn_name,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )

    if "error" in result:
        ret["comment"] = "Failed to delete VPC peering: {}".format(result["error"])
        ret["result"] = False
        return ret

    ret["changes"].update({"old": "", "new": result["msg"]})
    return ret
