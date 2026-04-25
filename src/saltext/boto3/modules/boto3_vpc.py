"""
Connection module for Amazon VPC using boto3.
=============================================

    Renamed from ``boto_vpc`` to ``boto3_vpc`` and rewritten to use the
    boto3 EC2 client API directly via :py:mod:`saltext.boto3.utils.boto3mod`.
    The legacy boto2 code path has been removed.

:depends:
  - boto3 >= 1.28.0
  - botocore >= 1.31.0

:configuration: This module accepts explicit VPC credentials but can also
    utilize IAM roles assigned to the instance through Instance Profiles.
    Dynamic credentials are then automatically obtained from AWS API and no
    further configuration is necessary. More information available at:

    .. code-block:: text

        http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html

    If IAM roles are not used you need to specify them either in the minion's
    config file or as a profile. For example, to specify them in the minion's
    config file:

.. code-block:: yaml

    vpc.keyid: GKTADJGHEIQSXMKKRBJ08H
    vpc.key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs

A region may also be specified in the configuration:

.. code-block:: yaml

    vpc.region: us-east-1

It's also possible to specify key, keyid and region via a profile, either
as a passed in dict, or as a string to pull from pillars or minion config:

.. code-block:: yaml

    myprofile:
        keyid: GKTADJGHEIQSXMKKRBJ08H
        key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs
        region: us-east-1

.. versionadded:: 1.0.0
"""

import logging
import random
import socket
import time

from salt.exceptions import CommandExecutionError
from salt.exceptions import SaltInvocationError
from salt.utils.data import exactly_one

from saltext.boto3.utils import boto3mod

try:
    import botocore

    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

log = logging.getLogger(__name__)

__virtualname__ = "boto3_vpc"


def _get_conn(service, region=None, key=None, keyid=None, profile=None):
    """
    Return a boto3 client for ``service`` using this module's dunders.
    """
    return boto3mod.get_connection(
        service,
        opts=__opts__,
        context=__context__,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )


def __virtual__():
    """
    Only load if boto3 is available. Minimum version is enforced via the
    project's ``pyproject.toml`` dependency declaration.
    """
    if HAS_BOTO3:
        return __virtualname__
    return (False, "The boto3_vpc module could not be loaded: boto3 is not available.")


def _tags_dict(tags):
    """Convert AWS tag list to a flat ``{key: value}`` dict."""
    return {t["Key"]: t["Value"] for t in (tags or [])}


def _tag_specifications(resource_type, name=None, tags=None):
    """Build ``TagSpecifications`` for a create_* call."""
    items = []
    if name:
        items.append({"Key": "Name", "Value": name})
    if tags:
        for key, value in tags.items():
            if key == "Name" and name:
                continue
            items.append({"Key": key, "Value": value})
    if not items:
        return None
    return [{"ResourceType": resource_type, "Tags": items}]


def _client_error_code(exc):
    if isinstance(exc, botocore.exceptions.ClientError):
        return exc.response.get("Error", {}).get("Code")
    return None


def _find_vpcs(
    *,
    vpc_id=None,
    vpc_name=None,
    cidr=None,
    tags=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Return a list of VPC ids matching the supplied filters. When no filters
    are supplied, returns the id of the default VPC (if any).
    """
    if vpc_id and vpc_name:
        raise SaltInvocationError("Only one of vpc_name or vpc_id may be provided.")

    conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)

    kwargs = {}
    if vpc_id:
        kwargs["VpcIds"] = [vpc_id]
    filters = []
    if cidr:
        filters.append({"Name": "cidr", "Values": [cidr]})
    if vpc_name:
        filters.append({"Name": "tag:Name", "Values": [vpc_name]})
    for tag_name, tag_value in (tags or {}).items():
        filters.append({"Name": f"tag:{tag_name}", "Values": [tag_value]})
    if filters:
        kwargs["Filters"] = filters

    vpcs = conn.describe_vpcs(**kwargs).get("Vpcs", [])
    log.debug("describe_vpcs(%s) matched: %s", kwargs, vpcs)

    if not vpcs:
        return []
    if not any((vpc_id, vpc_name, cidr, tags)):
        return [v["VpcId"] for v in vpcs if v.get("IsDefault")]
    return [v["VpcId"] for v in vpcs]


def _get_id(
    vpc_name=None,
    cidr=None,
    tags=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    if not any((vpc_name, tags, cidr)):
        raise SaltInvocationError(
            "At least one of the following must be provided: vpc_name, cidr or tags."
        )

    if vpc_name and not any((cidr, tags)):
        cached = boto3mod.cache_id(
            "ec2",
            vpc_name,
            opts=__opts__,
            context=__context__,
            region=region,
            key=key,
            keyid=keyid,
            profile=profile,
        )
        if cached:
            return cached

    vpc_ids = _find_vpcs(
        vpc_name=vpc_name,
        cidr=cidr,
        tags=tags,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )
    if not vpc_ids:
        return None
    if len(vpc_ids) > 1:
        raise CommandExecutionError("Found more than one VPC matching the criteria.")
    vpc_id = vpc_ids[0]
    if vpc_name:
        boto3mod.cache_id(
            "ec2",
            vpc_name,
            opts=__opts__,
            context=__context__,
            resource_id=vpc_id,
            region=region,
            key=key,
            keyid=keyid,
            profile=profile,
        )
    return vpc_id


def get_id(
    name=None,
    cidr=None,
    tags=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Return the id of the VPC matching the supplied filters.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_vpc.get_id myvpc
    """
    try:
        return {
            "id": _get_id(
                vpc_name=name,
                cidr=cidr,
                tags=tags,
                region=region,
                key=key,
                keyid=keyid,
                profile=profile,
            )
        }
    except botocore.exceptions.ClientError as exc:
        return {"error": boto3mod.get_error(exc)}


def exists(
    vpc_id=None,
    name=None,
    cidr=None,
    tags=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Return ``{"exists": True}`` if a VPC matching the supplied filters exists.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_vpc.exists myvpc
    """
    if not any((vpc_id, name, tags, cidr)):
        raise SaltInvocationError(
            "At least one of the following must be provided: vpc_id, name, cidr or tags."
        )
    try:
        vpc_ids = _find_vpcs(
            vpc_id=vpc_id,
            vpc_name=name,
            cidr=cidr,
            tags=tags,
            region=region,
            key=key,
            keyid=keyid,
            profile=profile,
        )
    except botocore.exceptions.ClientError as exc:
        if _client_error_code(exc) == "InvalidVpcID.NotFound":
            return {"exists": False}
        return {"error": boto3mod.get_error(exc)}
    return {"exists": bool(vpc_ids)}


def check_vpc(
    vpc_id=None,
    vpc_name=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Return the VPC id if a VPC with the supplied id or name exists, else
    ``None``.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_vpc.check_vpc vpc_name=myvpc
    """
    if not exactly_one((vpc_name, vpc_id)):
        raise SaltInvocationError("One (but not both) of vpc_id or vpc_name must be provided.")
    try:
        if vpc_name:
            return _get_id(vpc_name=vpc_name, region=region, key=key, keyid=keyid, profile=profile)
        if not _find_vpcs(vpc_id=vpc_id, region=region, key=key, keyid=keyid, profile=profile):
            log.info("VPC %s does not exist.", vpc_id)
            return None
        return vpc_id
    except botocore.exceptions.ClientError as exc:
        log.error("Failed to look up VPC: %s", exc)
        return None


def create(
    cidr_block,
    instance_tenancy=None,
    vpc_name=None,
    enable_dns_support=None,
    enable_dns_hostnames=None,
    tags=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Create a VPC with the given CIDR block.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_vpc.create '10.0.0.0/24'
    """
    try:
        conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
        kwargs = {"CidrBlock": cidr_block}
        if instance_tenancy is not None:
            kwargs["InstanceTenancy"] = instance_tenancy
        tag_spec = _tag_specifications("vpc", name=vpc_name, tags=tags)
        if tag_spec:
            kwargs["TagSpecifications"] = tag_spec
        vpc = conn.create_vpc(**kwargs).get("Vpc") or {}
        vpc_id = vpc.get("VpcId")
        if not vpc_id:
            log.warning("VPC was not created")
            return {"created": False}
        log.info("Created VPC %s", vpc_id)

        if enable_dns_support is not None:
            conn.modify_vpc_attribute(
                VpcId=vpc_id, EnableDnsSupport={"Value": bool(enable_dns_support)}
            )
        if enable_dns_hostnames is not None:
            conn.modify_vpc_attribute(
                VpcId=vpc_id, EnableDnsHostnames={"Value": bool(enable_dns_hostnames)}
            )
        if vpc_name:
            boto3mod.cache_id(
                "ec2",
                vpc_name,
                opts=__opts__,
                context=__context__,
                resource_id=vpc_id,
                region=region,
                key=key,
                keyid=keyid,
                profile=profile,
            )
        return {"created": True, "id": vpc_id}
    except botocore.exceptions.ClientError as exc:
        return {"created": False, "error": boto3mod.get_error(exc)}


def delete(
    vpc_id=None,
    name=None,
    vpc_name=None,
    tags=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Delete a VPC by id or name.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_vpc.delete vpc_id='vpc-6b1fe402'
        salt myminion boto3_vpc.delete vpc_name='myvpc'
    """
    if name:
        log.warning("boto3_vpc.delete: name parameter is deprecated; use vpc_name instead.")
        vpc_name = name
    if not exactly_one((vpc_name, vpc_id)):
        raise SaltInvocationError("One (but not both) of vpc_name or vpc_id must be provided.")
    try:
        conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
        if not vpc_id:
            vpc_id = _get_id(
                vpc_name=vpc_name,
                tags=tags,
                region=region,
                key=key,
                keyid=keyid,
                profile=profile,
            )
            if not vpc_id:
                return {
                    "deleted": False,
                    "error": {"message": f"VPC {vpc_name} not found"},
                }
        conn.delete_vpc(VpcId=vpc_id)
        log.info("Deleted VPC %s", vpc_id)
        if vpc_name:
            boto3mod.cache_id(
                "ec2",
                vpc_name,
                opts=__opts__,
                context=__context__,
                resource_id=vpc_id,
                invalidate=True,
                region=region,
                key=key,
                keyid=keyid,
                profile=profile,
            )
        return {"deleted": True}
    except botocore.exceptions.ClientError as exc:
        return {"deleted": False, "error": boto3mod.get_error(exc)}


def _vpc_payload(vpc):
    return {
        "id": vpc.get("VpcId"),
        "cidr_block": vpc.get("CidrBlock"),
        "is_default": vpc.get("IsDefault"),
        "state": vpc.get("State"),
        "tags": _tags_dict(vpc.get("Tags")),
        "dhcp_options_id": vpc.get("DhcpOptionsId"),
        "instance_tenancy": vpc.get("InstanceTenancy"),
    }


def describe(
    vpc_id=None,
    vpc_name=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Describe a VPC's properties. If neither id nor name is provided the
    default VPC (if any) is described.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_vpc.describe vpc_id=vpc-123456
    """
    try:
        vpc_ids = _find_vpcs(
            vpc_id=vpc_id,
            vpc_name=vpc_name,
            region=region,
            key=key,
            keyid=keyid,
            profile=profile,
        )
        if not vpc_ids:
            return {"vpc": None}
        conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
        vpcs = conn.describe_vpcs(VpcIds=vpc_ids).get("Vpcs", [])
    except botocore.exceptions.ClientError as exc:
        if _client_error_code(exc) == "InvalidVpcID.NotFound":
            return {"vpc": None}
        return {"error": boto3mod.get_error(exc)}
    if not vpcs:
        return {"vpc": None}
    return {"vpc": _vpc_payload(vpcs[0])}


def describe_vpcs(
    vpc_id=None,
    name=None,
    cidr=None,
    tags=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Describe all VPCs matching the supplied filters.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_vpc.describe_vpcs
    """
    try:
        conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
        kwargs = {}
        if vpc_id:
            kwargs["VpcIds"] = [vpc_id]
        filters = []
        if cidr:
            filters.append({"Name": "cidr", "Values": [cidr]})
        if name:
            filters.append({"Name": "tag:Name", "Values": [name]})
        for tag_name, tag_value in (tags or {}).items():
            filters.append({"Name": f"tag:{tag_name}", "Values": [tag_value]})
        if filters:
            kwargs["Filters"] = filters
        vpcs = conn.describe_vpcs(**kwargs).get("Vpcs", [])
    except botocore.exceptions.ClientError as exc:
        return {"error": boto3mod.get_error(exc)}
    return {"vpcs": [_vpc_payload(v) for v in vpcs]}


_SUPPORTED_RESOURCES = {
    "vpc": ("describe_vpcs", "VpcIds", "Vpcs", "VpcId"),
    "subnet": ("describe_subnets", "SubnetIds", "Subnets", "SubnetId"),
    "dhcp_options": (
        "describe_dhcp_options",
        "DhcpOptionsIds",
        "DhcpOptions",
        "DhcpOptionsId",
    ),
    "internet_gateway": (
        "describe_internet_gateways",
        "InternetGatewayIds",
        "InternetGateways",
        "InternetGatewayId",
    ),
    "customer_gateway": (
        "describe_customer_gateways",
        "CustomerGatewayIds",
        "CustomerGateways",
        "CustomerGatewayId",
    ),
    "network_acl": (
        "describe_network_acls",
        "NetworkAclIds",
        "NetworkAcls",
        "NetworkAclId",
    ),
    "route_table": (
        "describe_route_tables",
        "RouteTableIds",
        "RouteTables",
        "RouteTableId",
    ),
    "vpc_peering_connection": (
        "describe_vpc_peering_connections",
        "VpcPeeringConnectionIds",
        "VpcPeeringConnections",
        "VpcPeeringConnectionId",
    ),
}


def _find_resources(
    resource,
    name=None,
    resource_id=None,
    tags=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    if resource not in _SUPPORTED_RESOURCES:
        raise SaltInvocationError(f"Resource type {resource!r} is not supported by boto3_vpc.")
    if resource_id and name:
        raise SaltInvocationError("Only one of name or id may be provided.")
    if not any((resource_id, name, tags)):
        raise SaltInvocationError(
            "At least one of the following must be provided: id, name, or tags."
        )

    describe_method, ids_kw, items_key, _ = _SUPPORTED_RESOURCES[resource]
    conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
    kwargs = {}
    if resource_id:
        kwargs[ids_kw] = [resource_id]
    filters = []
    if name:
        filters.append({"Name": "tag:Name", "Values": [name]})
    for tag_name, tag_value in (tags or {}).items():
        filters.append({"Name": f"tag:{tag_name}", "Values": [tag_value]})
    if filters:
        kwargs["Filters"] = filters
    try:
        return getattr(conn, describe_method)(**kwargs).get(items_key, [])
    except botocore.exceptions.ClientError as exc:
        if (_client_error_code(exc) or "").endswith(".NotFound"):
            return []
        raise


def _get_resource_id(resource, name, region=None, key=None, keyid=None, profile=None):
    cached = boto3mod.cache_id(
        "ec2",
        name,
        opts=__opts__,
        context=__context__,
        sub_resource=resource,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )
    if cached:
        return cached
    items = _find_resources(
        resource,
        name=name,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )
    if not items:
        return None
    if len(items) > 1:
        raise CommandExecutionError(f'Found more than one {resource} named "{name}"')
    _, _, _, id_key = _SUPPORTED_RESOURCES[resource]
    rid = items[0].get(id_key)
    if rid:
        boto3mod.cache_id(
            "ec2",
            name,
            opts=__opts__,
            context=__context__,
            sub_resource=resource,
            resource_id=rid,
            region=region,
            key=key,
            keyid=keyid,
            profile=profile,
        )
    return rid


def get_resource_id(
    resource,
    name=None,
    resource_id=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Return ``{"id": "..."}`` for a VPC resource looked up by name or id.

    Currently supported ``resource`` values: ``vpc``, ``subnet``,
    ``dhcp_options``.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_vpc.get_resource_id subnet mysubnet
    """
    try:
        if resource_id and not name:
            return {"id": resource_id}
        return {
            "id": _get_resource_id(
                resource, name, region=region, key=key, keyid=keyid, profile=profile
            )
        }
    except botocore.exceptions.ClientError as exc:
        return {"error": boto3mod.get_error(exc)}


def resource_exists(
    resource,
    name=None,
    resource_id=None,
    tags=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Return ``{"exists": True}`` if a resource of ``resource`` matching the
    supplied filters exists.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_vpc.resource_exists subnet name=mysubnet
    """
    try:
        return {
            "exists": bool(
                _find_resources(
                    resource,
                    name=name,
                    resource_id=resource_id,
                    tags=tags,
                    region=region,
                    key=key,
                    keyid=keyid,
                    profile=profile,
                )
            )
        }
    except botocore.exceptions.ClientError as exc:
        return {"error": boto3mod.get_error(exc)}


def create_subnet(
    vpc_id=None,
    cidr_block=None,
    vpc_name=None,
    availability_zone=None,
    subnet_name=None,
    tags=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
    auto_assign_public_ipv4=False,
):
    """
    Create a subnet inside an existing VPC.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_vpc.create_subnet vpc_name=myvpc \\
            subnet_name=mysubnet cidr_block=10.0.0.0/25
    """
    try:
        vpc_id = check_vpc(vpc_id, vpc_name, region, key, keyid, profile)
        if not vpc_id:
            return {
                "created": False,
                "error": {"message": f"VPC {vpc_name or vpc_id} does not exist."},
            }
        conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
        if subnet_name and _get_resource_id(
            "subnet", subnet_name, region=region, key=key, keyid=keyid, profile=profile
        ):
            return {
                "created": False,
                "error": {"message": f"A subnet named {subnet_name} already exists."},
            }
        kwargs = {"VpcId": vpc_id, "CidrBlock": cidr_block}
        if availability_zone:
            kwargs["AvailabilityZone"] = availability_zone
        tag_spec = _tag_specifications("subnet", name=subnet_name, tags=tags)
        if tag_spec:
            kwargs["TagSpecifications"] = tag_spec
        subnet = conn.create_subnet(**kwargs).get("Subnet") or {}
        subnet_id = subnet.get("SubnetId")
        if not subnet_id:
            return {"created": False}
        if auto_assign_public_ipv4:
            conn.modify_subnet_attribute(SubnetId=subnet_id, MapPublicIpOnLaunch={"Value": True})
        if subnet_name:
            boto3mod.cache_id(
                "ec2",
                subnet_name,
                opts=__opts__,
                context=__context__,
                sub_resource="subnet",
                resource_id=subnet_id,
                region=region,
                key=key,
                keyid=keyid,
                profile=profile,
            )
        return {"created": True, "id": subnet_id}
    except botocore.exceptions.ClientError as exc:
        return {"created": False, "error": boto3mod.get_error(exc)}


def delete_subnet(
    subnet_id=None, subnet_name=None, region=None, key=None, keyid=None, profile=None
):
    """
    Delete a subnet by id or name.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_vpc.delete_subnet 'subnet-6a1fe403'
    """
    if not exactly_one((subnet_name, subnet_id)):
        raise SaltInvocationError(
            "One (but not both) of subnet_name or subnet_id must be provided."
        )
    try:
        conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
        if not subnet_id:
            subnet_id = _get_resource_id(
                "subnet",
                subnet_name,
                region=region,
                key=key,
                keyid=keyid,
                profile=profile,
            )
            if not subnet_id:
                return {
                    "deleted": False,
                    "error": {"message": f"subnet {subnet_name} does not exist."},
                }
        conn.delete_subnet(SubnetId=subnet_id)
        if subnet_name:
            boto3mod.cache_id(
                "ec2",
                subnet_name,
                opts=__opts__,
                context=__context__,
                sub_resource="subnet",
                resource_id=subnet_id,
                invalidate=True,
                region=region,
                key=key,
                keyid=keyid,
                profile=profile,
            )
        return {"deleted": True}
    except botocore.exceptions.ClientError as exc:
        return {"deleted": False, "error": boto3mod.get_error(exc)}


def subnet_exists(
    subnet_id=None,
    name=None,
    subnet_name=None,
    cidr=None,
    tags=None,
    zones=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Return ``{"exists": True}`` when a subnet matching the filters exists.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_vpc.subnet_exists subnet_id='subnet-6a1fe403'
    """
    if name:
        log.warning(
            "boto3_vpc.subnet_exists: name parameter is deprecated; use subnet_name instead."
        )
        subnet_name = name
    if not any((subnet_id, subnet_name, cidr, tags, zones)):
        raise SaltInvocationError(
            "At least one of the following must be specified: "
            "subnet_id, subnet_name, cidr, tags or zones."
        )
    try:
        conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
        kwargs = {}
        if subnet_id:
            kwargs["SubnetIds"] = [subnet_id]
        filters = []
        if subnet_name:
            filters.append({"Name": "tag:Name", "Values": [subnet_name]})
        if cidr:
            filters.append({"Name": "cidr-block", "Values": [cidr]})
        for tag_name, tag_value in (tags or {}).items():
            filters.append({"Name": f"tag:{tag_name}", "Values": [tag_value]})
        if zones:
            zones_values = [zones] if isinstance(zones, str) else list(zones)
            filters.append({"Name": "availability-zone", "Values": zones_values})
        if filters:
            kwargs["Filters"] = filters
        subnets = conn.describe_subnets(**kwargs).get("Subnets", [])
    except botocore.exceptions.ClientError as exc:
        if _client_error_code(exc) == "InvalidSubnetID.NotFound":
            return {"exists": False}
        return {"error": boto3mod.get_error(exc)}
    return {"exists": bool(subnets)}


def get_subnet_association(subnets, region=None, key=None, keyid=None, profile=None):
    """
    Return the VPC id (or list of VPC ids) associated with the given subnet
    id or list of subnet ids.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_vpc.get_subnet_association subnet-61b47516
    """
    try:
        conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
        ids = [subnets] if isinstance(subnets, str) else list(subnets)
        described = conn.describe_subnets(SubnetIds=ids).get("Subnets", [])
    except botocore.exceptions.ClientError as exc:
        return {"error": boto3mod.get_error(exc)}
    vpc_ids = {s["VpcId"] for s in described if s.get("VpcId")}
    if not vpc_ids:
        return {"vpc_id": None}
    if len(vpc_ids) == 1:
        return {"vpc_id": vpc_ids.pop()}
    return {"vpc_ids": list(vpc_ids)}


def _subnet_payload(subnet):
    return {
        "id": subnet.get("SubnetId"),
        "cidr_block": subnet.get("CidrBlock"),
        "availability_zone": subnet.get("AvailabilityZone"),
        "tags": _tags_dict(subnet.get("Tags")),
        "vpc_id": subnet.get("VpcId"),
    }


def describe_subnet(
    subnet_id=None, subnet_name=None, region=None, key=None, keyid=None, profile=None
):
    """
    Describe a single subnet by id or name.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_vpc.describe_subnet subnet_id=subnet-123456
    """
    if not exactly_one((subnet_name, subnet_id)):
        raise SaltInvocationError(
            "One (but not both) of subnet_name or subnet_id must be provided."
        )
    try:
        items = _find_resources(
            "subnet",
            name=subnet_name,
            resource_id=subnet_id,
            region=region,
            key=key,
            keyid=keyid,
            profile=profile,
        )
    except botocore.exceptions.ClientError as exc:
        return {"error": boto3mod.get_error(exc)}
    if not items:
        return {"subnet": None}
    if len(items) > 1:
        raise CommandExecutionError(f'Found more than one subnet named "{subnet_name}"')
    return {"subnet": _subnet_payload(items[0])}


def describe_subnets(
    subnet_ids=None,
    subnet_names=None,
    vpc_id=None,
    cidr=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Describe subnets matching the supplied filters.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_vpc.describe_subnets vpc_id=vpc-123456
    """
    try:
        conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
        kwargs = {}
        if subnet_ids:
            kwargs["SubnetIds"] = [subnet_ids] if isinstance(subnet_ids, str) else list(subnet_ids)
        filters = []
        if vpc_id:
            filters.append({"Name": "vpc-id", "Values": [vpc_id]})
        if cidr:
            filters.append({"Name": "cidr-block", "Values": [cidr]})
        if subnet_names:
            names = [subnet_names] if isinstance(subnet_names, str) else list(subnet_names)
            filters.append({"Name": "tag:Name", "Values": names})
        if filters:
            kwargs["Filters"] = filters
        subnets = conn.describe_subnets(**kwargs).get("Subnets", [])
    except botocore.exceptions.ClientError as exc:
        return {"error": boto3mod.get_error(exc)}
    if not subnets:
        return {"subnets": None}
    return {"subnets": [_subnet_payload(s) for s in subnets]}


_DHCP_KEY_MAP = {
    "domain_name": "domain-name",
    "domain_name_servers": "domain-name-servers",
    "ntp_servers": "ntp-servers",
    "netbios_name_servers": "netbios-name-servers",
    "netbios_node_type": "netbios-node-type",
}


def _dhcp_configurations(
    domain_name=None,
    domain_name_servers=None,
    ntp_servers=None,
    netbios_name_servers=None,
    netbios_node_type=None,
):
    config = []

    def _add(api_key, value):
        if value is None:
            return
        if isinstance(value, (list, tuple)):
            values = [str(v) for v in value]
        else:
            values = [str(value)]
        config.append({"Key": api_key, "Values": values})

    _add("domain-name", domain_name)
    _add("domain-name-servers", domain_name_servers)
    _add("ntp-servers", ntp_servers)
    _add("netbios-name-servers", netbios_name_servers)
    _add("netbios-node-type", netbios_node_type)
    return config


def create_dhcp_options(
    domain_name=None,
    domain_name_servers=None,
    ntp_servers=None,
    netbios_name_servers=None,
    netbios_node_type=None,
    dhcp_options_name=None,
    tags=None,
    vpc_id=None,
    vpc_name=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Create a DHCP options set, optionally associating it with an existing VPC.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_vpc.create_dhcp_options domain_name='example.com' \\
            domain_name_servers='[1.2.3.4]' vpc_name='myvpc'
    """
    try:
        if vpc_id or vpc_name:
            vpc_id = check_vpc(vpc_id, vpc_name, region, key, keyid, profile)
            if not vpc_id:
                return {
                    "created": False,
                    "error": {"message": f"VPC {vpc_name or vpc_id} does not exist."},
                }
        conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
        if dhcp_options_name and _get_resource_id(
            "dhcp_options",
            dhcp_options_name,
            region=region,
            key=key,
            keyid=keyid,
            profile=profile,
        ):
            return {
                "created": False,
                "error": {"message": f"A dhcp_options named {dhcp_options_name} already exists."},
            }
        kwargs = {
            "DhcpConfigurations": _dhcp_configurations(
                domain_name=domain_name,
                domain_name_servers=domain_name_servers,
                ntp_servers=ntp_servers,
                netbios_name_servers=netbios_name_servers,
                netbios_node_type=netbios_node_type,
            )
        }
        tag_spec = _tag_specifications("dhcp-options", name=dhcp_options_name, tags=tags)
        if tag_spec:
            kwargs["TagSpecifications"] = tag_spec
        dhcp = conn.create_dhcp_options(**kwargs).get("DhcpOptions") or {}
        dhcp_id = dhcp.get("DhcpOptionsId")
        if not dhcp_id:
            return {"created": False}
        if vpc_id:
            conn.associate_dhcp_options(DhcpOptionsId=dhcp_id, VpcId=vpc_id)
            log.info("Associated DHCP options %s with VPC %s", dhcp_id, vpc_id)
        if dhcp_options_name:
            boto3mod.cache_id(
                "ec2",
                dhcp_options_name,
                opts=__opts__,
                context=__context__,
                sub_resource="dhcp_options",
                resource_id=dhcp_id,
                region=region,
                key=key,
                keyid=keyid,
                profile=profile,
            )
        return {"created": True, "id": dhcp_id}
    except botocore.exceptions.ClientError as exc:
        return {"created": False, "error": boto3mod.get_error(exc)}


def get_dhcp_options(
    dhcp_options_name=None,
    dhcp_options_id=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Return the configured options for the named DHCP options set.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_vpc.get_dhcp_options 'myfunnydhcpoptionsname'
    """
    if not any((dhcp_options_name, dhcp_options_id)):
        raise SaltInvocationError(
            "At least one of the following must be specified: "
            "dhcp_options_name or dhcp_options_id."
        )
    if not dhcp_options_id and dhcp_options_name:
        dhcp_options_id = _get_resource_id(
            "dhcp_options",
            dhcp_options_name,
            region=region,
            key=key,
            keyid=keyid,
            profile=profile,
        )
    if not dhcp_options_id:
        return {"dhcp_options": {}}
    try:
        conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
        items = conn.describe_dhcp_options(DhcpOptionsIds=[dhcp_options_id]).get("DhcpOptions", [])
    except botocore.exceptions.ClientError as exc:
        return {"error": boto3mod.get_error(exc)}
    if not items:
        return {"dhcp_options": None}
    by_api_key = {
        cfg["Key"]: [v["Value"] for v in cfg.get("Values", [])]
        for cfg in items[0].get("DhcpConfigurations", [])
    }
    options = {}
    for legacy_key, api_key in _DHCP_KEY_MAP.items():
        values = by_api_key.get(api_key)
        if values is None:
            options[legacy_key] = None
        elif legacy_key in ("domain_name", "netbios_node_type"):
            options[legacy_key] = values[0] if values else None
        else:
            options[legacy_key] = values
    return {"dhcp_options": options}


def delete_dhcp_options(
    dhcp_options_id=None,
    dhcp_options_name=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Delete a DHCP options set by id or name.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_vpc.delete_dhcp_options 'dopt-b6a247df'
    """
    if not exactly_one((dhcp_options_name, dhcp_options_id)):
        raise SaltInvocationError(
            "One (but not both) of dhcp_options_name or dhcp_options_id must be provided."
        )
    try:
        conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
        if not dhcp_options_id:
            dhcp_options_id = _get_resource_id(
                "dhcp_options",
                dhcp_options_name,
                region=region,
                key=key,
                keyid=keyid,
                profile=profile,
            )
            if not dhcp_options_id:
                return {
                    "deleted": False,
                    "error": {"message": f"dhcp_options {dhcp_options_name} does not exist."},
                }
        conn.delete_dhcp_options(DhcpOptionsId=dhcp_options_id)
        if dhcp_options_name:
            boto3mod.cache_id(
                "ec2",
                dhcp_options_name,
                opts=__opts__,
                context=__context__,
                sub_resource="dhcp_options",
                resource_id=dhcp_options_id,
                invalidate=True,
                region=region,
                key=key,
                keyid=keyid,
                profile=profile,
            )
        return {"deleted": True}
    except botocore.exceptions.ClientError as exc:
        return {"deleted": False, "error": boto3mod.get_error(exc)}


def associate_dhcp_options_to_vpc(
    dhcp_options_id,
    vpc_id=None,
    vpc_name=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Associate a DHCP options set with a VPC.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_vpc.associate_dhcp_options_to_vpc 'dopt-a0bl34pp' 'vpc-6b1fe402'
    """
    try:
        vpc_id = check_vpc(vpc_id, vpc_name, region, key, keyid, profile)
        if not vpc_id:
            return {
                "associated": False,
                "error": {"message": f"VPC {vpc_name or vpc_id} does not exist."},
            }
        conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
        conn.associate_dhcp_options(DhcpOptionsId=dhcp_options_id, VpcId=vpc_id)
        log.info("Associated DHCP options %s with VPC %s", dhcp_options_id, vpc_id)
        return {"associated": True}
    except botocore.exceptions.ClientError as exc:
        return {"associated": False, "error": boto3mod.get_error(exc)}


def dhcp_options_exists(
    dhcp_options_id=None,
    name=None,
    dhcp_options_name=None,
    tags=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Return ``{"exists": True}`` if a DHCP options set matching the filters exists.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_vpc.dhcp_options_exists dhcp_options_id='dopt-a0bl34pp'
    """
    if name:
        log.warning(
            "boto3_vpc.dhcp_options_exists: name parameter is deprecated; "
            "use dhcp_options_name instead."
        )
        dhcp_options_name = name
    return resource_exists(
        "dhcp_options",
        name=dhcp_options_name,
        resource_id=dhcp_options_id,
        tags=tags,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )


def create_internet_gateway(
    internet_gateway_name=None,
    vpc_id=None,
    vpc_name=None,
    tags=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Create an internet gateway, optionally attaching it to an existing VPC.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_vpc.create_internet_gateway \\
            internet_gateway_name=myigw vpc_name=myvpc
    """
    try:
        if vpc_id or vpc_name:
            vpc_id = check_vpc(vpc_id, vpc_name, region, key, keyid, profile)
            if not vpc_id:
                return {
                    "created": False,
                    "error": {"message": f"VPC {vpc_name or vpc_id} does not exist."},
                }
        conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
        kwargs = {}
        tag_spec = _tag_specifications("internet-gateway", name=internet_gateway_name, tags=tags)
        if tag_spec:
            kwargs["TagSpecifications"] = tag_spec
        gw = conn.create_internet_gateway(**kwargs).get("InternetGateway") or {}
        gw_id = gw.get("InternetGatewayId")
        if not gw_id:
            return {"created": False}
        if vpc_id:
            conn.attach_internet_gateway(InternetGatewayId=gw_id, VpcId=vpc_id)
            log.info("Attached internet gateway %s to VPC %s", gw_id, vpc_id)
        if internet_gateway_name:
            boto3mod.cache_id(
                "ec2",
                internet_gateway_name,
                opts=__opts__,
                context=__context__,
                sub_resource="internet_gateway",
                resource_id=gw_id,
                region=region,
                key=key,
                keyid=keyid,
                profile=profile,
            )
        return {"created": True, "id": gw_id}
    except botocore.exceptions.ClientError as exc:
        return {"created": False, "error": boto3mod.get_error(exc)}


def delete_internet_gateway(
    internet_gateway_id=None,
    internet_gateway_name=None,
    detach=False,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Delete an internet gateway by id or name. If ``detach`` is ``True``,
    any VPC attachment is detached first.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_vpc.delete_internet_gateway internet_gateway_id=igw-1a2b3c
    """
    try:
        conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
        if internet_gateway_name and not internet_gateway_id:
            internet_gateway_id = _get_resource_id(
                "internet_gateway",
                internet_gateway_name,
                region=region,
                key=key,
                keyid=keyid,
                profile=profile,
            )
        if not internet_gateway_id:
            return {
                "deleted": False,
                "error": {"message": f"internet gateway {internet_gateway_name} does not exist."},
            }
        if detach:
            gws = conn.describe_internet_gateways(InternetGatewayIds=[internet_gateway_id]).get(
                "InternetGateways", []
            )
            if not gws:
                return {
                    "deleted": False,
                    "error": {"message": f"internet gateway {internet_gateway_id} does not exist."},
                }
            for att in gws[0].get("Attachments", []):
                conn.detach_internet_gateway(
                    InternetGatewayId=internet_gateway_id, VpcId=att["VpcId"]
                )
        conn.delete_internet_gateway(InternetGatewayId=internet_gateway_id)
        if internet_gateway_name:
            boto3mod.cache_id(
                "ec2",
                internet_gateway_name,
                opts=__opts__,
                context=__context__,
                sub_resource="internet_gateway",
                resource_id=internet_gateway_id,
                invalidate=True,
                region=region,
                key=key,
                keyid=keyid,
                profile=profile,
            )
        return {"deleted": True}
    except botocore.exceptions.ClientError as exc:
        return {"deleted": False, "error": boto3mod.get_error(exc)}


def _find_nat_gateways(
    nat_gateway_id=None,
    subnet_id=None,
    subnet_name=None,
    vpc_id=None,
    vpc_name=None,
    states=("pending", "available"),
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    if not any((nat_gateway_id, subnet_id, subnet_name, vpc_id, vpc_name)):
        raise SaltInvocationError(
            "At least one of the following must be provided: "
            "nat_gateway_id, subnet_id, subnet_name, vpc_id, or vpc_name."
        )
    if subnet_name:
        subnet_id = _get_resource_id(
            "subnet", subnet_name, region=region, key=key, keyid=keyid, profile=profile
        )
        if not subnet_id:
            return []
    if vpc_name:
        vpc_id = _get_resource_id(
            "vpc", vpc_name, region=region, key=key, keyid=keyid, profile=profile
        )
        if not vpc_id:
            return []
    conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
    kwargs = {"Filter": []}
    if nat_gateway_id:
        kwargs["NatGatewayIds"] = [nat_gateway_id]
    if subnet_id:
        kwargs["Filter"].append({"Name": "subnet-id", "Values": [subnet_id]})
    if vpc_id:
        kwargs["Filter"].append({"Name": "vpc-id", "Values": [vpc_id]})
    matches = []
    next_token = None
    while True:
        call_kwargs = dict(kwargs)
        if next_token:
            call_kwargs["NextToken"] = next_token
        resp = conn.describe_nat_gateways(**call_kwargs)
        for gw in resp.get("NatGateways", []):
            if gw.get("State") in states:
                matches.append(gw)
        next_token = resp.get("NextToken")
        if not next_token:
            break
    return matches


def nat_gateway_exists(
    nat_gateway_id=None,
    subnet_id=None,
    subnet_name=None,
    vpc_id=None,
    vpc_name=None,
    states=("pending", "available"),
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Return ``True`` if a NAT gateway matching the filter criteria exists.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_vpc.nat_gateway_exists nat_gateway_id='nat-03b02643b43216fe7'
    """
    try:
        return bool(
            _find_nat_gateways(
                nat_gateway_id=nat_gateway_id,
                subnet_id=subnet_id,
                subnet_name=subnet_name,
                vpc_id=vpc_id,
                vpc_name=vpc_name,
                states=states,
                region=region,
                key=key,
                keyid=keyid,
                profile=profile,
            )
        )
    except botocore.exceptions.ClientError as exc:
        log.error("Failed to look up NAT gateway: %s", exc)
        return False


def describe_nat_gateways(
    nat_gateway_id=None,
    subnet_id=None,
    subnet_name=None,
    vpc_id=None,
    vpc_name=None,
    states=("pending", "available"),
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Return a list of NAT gateway descriptions matching the selection criteria.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_vpc.describe_nat_gateways subnet_id='subnet-5b05942d'
    """
    try:
        return _find_nat_gateways(
            nat_gateway_id=nat_gateway_id,
            subnet_id=subnet_id,
            subnet_name=subnet_name,
            vpc_id=vpc_id,
            vpc_name=vpc_name,
            states=states,
            region=region,
            key=key,
            keyid=keyid,
            profile=profile,
        )
    except botocore.exceptions.ClientError as exc:
        log.error("Failed to describe NAT gateways: %s", exc)
        return []


def create_nat_gateway(
    subnet_id=None,
    subnet_name=None,
    allocation_id=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Create a NAT gateway inside an existing subnet. If ``allocation_id`` is
    not supplied a new Elastic IP is allocated and used.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_vpc.create_nat_gateway subnet_name=mysubnet
    """
    if all((subnet_id, subnet_name)):
        raise SaltInvocationError("Only one of subnet_name or subnet_id may be provided.")
    try:
        if subnet_name:
            subnet_id = _get_resource_id(
                "subnet", subnet_name, region=region, key=key, keyid=keyid, profile=profile
            )
            if not subnet_id:
                return {
                    "created": False,
                    "error": {"message": f"Subnet {subnet_name} does not exist."},
                }
        conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
        if not allocation_id:
            address = conn.allocate_address(Domain="vpc")
            allocation_id = address.get("AllocationId")
        r = conn.create_nat_gateway(SubnetId=subnet_id, AllocationId=allocation_id)
        return {"created": True, "id": r.get("NatGateway", {}).get("NatGatewayId")}
    except botocore.exceptions.ClientError as exc:
        return {"created": False, "error": boto3mod.get_error(exc)}


def delete_nat_gateway(
    nat_gateway_id,
    release_eips=False,
    region=None,
    key=None,
    keyid=None,
    profile=None,
    wait_for_delete=False,
    wait_for_delete_retries=5,
):
    """
    Delete a NAT gateway by id, optionally releasing any associated EIPs.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_vpc.delete_nat_gateway nat_gateway_id=nat-1a2b3c
    """
    try:
        conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
        info = conn.describe_nat_gateways(NatGatewayIds=[nat_gateway_id]).get(
            "NatGateways", [None]
        )[0]
        conn.delete_nat_gateway(NatGatewayId=nat_gateway_id)
        if wait_for_delete:
            for retry in range(wait_for_delete_retries, 0, -1):
                if info and info.get("State") not in ("deleted", "failed"):
                    time.sleep(
                        (2 ** (wait_for_delete_retries - retry))
                        + (random.randint(0, 1000) / 1000.0)
                    )
                    info = conn.describe_nat_gateways(NatGatewayIds=[nat_gateway_id]).get(
                        "NatGateways", [None]
                    )[0]
                    continue
                break
        if release_eips and info:
            for addr in info.get("NatGatewayAddresses") or []:
                alloc = addr.get("AllocationId")
                if alloc:
                    conn.release_address(AllocationId=alloc)
        return {"deleted": True}
    except botocore.exceptions.ClientError as exc:
        return {"deleted": False, "error": boto3mod.get_error(exc)}


def create_customer_gateway(
    vpn_connection_type,
    ip_address,
    bgp_asn,
    customer_gateway_name=None,
    tags=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Create a customer gateway.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_vpc.create_customer_gateway 'ipsec.1' '12.1.2.3' 65534
    """
    try:
        conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
        kwargs = {"Type": vpn_connection_type, "PublicIp": ip_address, "BgpAsn": int(bgp_asn)}
        tag_spec = _tag_specifications("customer-gateway", name=customer_gateway_name, tags=tags)
        if tag_spec:
            kwargs["TagSpecifications"] = tag_spec
        cg = conn.create_customer_gateway(**kwargs).get("CustomerGateway") or {}
        cg_id = cg.get("CustomerGatewayId")
        if not cg_id:
            return {"created": False}
        if customer_gateway_name:
            boto3mod.cache_id(
                "ec2",
                customer_gateway_name,
                opts=__opts__,
                context=__context__,
                sub_resource="customer_gateway",
                resource_id=cg_id,
                region=region,
                key=key,
                keyid=keyid,
                profile=profile,
            )
        return {"created": True, "id": cg_id}
    except botocore.exceptions.ClientError as exc:
        return {"created": False, "error": boto3mod.get_error(exc)}


def delete_customer_gateway(
    customer_gateway_id=None,
    customer_gateway_name=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Delete a customer gateway by id or name.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_vpc.delete_customer_gateway 'cgw-b6a247df'
    """
    try:
        if customer_gateway_name and not customer_gateway_id:
            customer_gateway_id = _get_resource_id(
                "customer_gateway",
                customer_gateway_name,
                region=region,
                key=key,
                keyid=keyid,
                profile=profile,
            )
        if not customer_gateway_id:
            return {
                "deleted": False,
                "error": {"message": f"customer gateway {customer_gateway_name} does not exist."},
            }
        conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
        conn.delete_customer_gateway(CustomerGatewayId=customer_gateway_id)
        if customer_gateway_name:
            boto3mod.cache_id(
                "ec2",
                customer_gateway_name,
                opts=__opts__,
                context=__context__,
                sub_resource="customer_gateway",
                resource_id=customer_gateway_id,
                invalidate=True,
                region=region,
                key=key,
                keyid=keyid,
                profile=profile,
            )
        return {"deleted": True}
    except botocore.exceptions.ClientError as exc:
        return {"deleted": False, "error": boto3mod.get_error(exc)}


def customer_gateway_exists(
    customer_gateway_id=None,
    customer_gateway_name=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Return ``{"exists": True}`` if the given customer gateway exists.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_vpc.customer_gateway_exists cgw-b6a247df
    """
    return resource_exists(
        "customer_gateway",
        name=customer_gateway_name,
        resource_id=customer_gateway_id,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )


def create_network_acl(
    vpc_id=None,
    vpc_name=None,
    network_acl_name=None,
    subnet_id=None,
    subnet_name=None,
    tags=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Create a network ACL within a VPC, optionally associating it with a
    subnet.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_vpc.create_network_acl 'vpc-6b1fe402'
    """
    _id = vpc_name or vpc_id
    try:
        vpc_id = check_vpc(vpc_id, vpc_name, region, key, keyid, profile)
    except botocore.exceptions.ClientError as exc:
        return {"created": False, "error": boto3mod.get_error(exc)}
    if not vpc_id:
        return {"created": False, "error": {"message": f"VPC {_id} does not exist."}}
    if all((subnet_id, subnet_name)):
        raise SaltInvocationError("Only one of subnet_name or subnet_id may be provided.")
    try:
        if subnet_name:
            subnet_id = _get_resource_id(
                "subnet", subnet_name, region=region, key=key, keyid=keyid, profile=profile
            )
            if not subnet_id:
                return {
                    "created": False,
                    "error": {"message": f"Subnet {subnet_name} does not exist."},
                }
        conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
        kwargs = {"VpcId": vpc_id}
        tag_spec = _tag_specifications("network-acl", name=network_acl_name, tags=tags)
        if tag_spec:
            kwargs["TagSpecifications"] = tag_spec
        acl = conn.create_network_acl(**kwargs).get("NetworkAcl") or {}
        acl_id = acl.get("NetworkAclId")
        if not acl_id:
            return {"created": False}
        result = {"created": True, "id": acl_id}
        if network_acl_name:
            boto3mod.cache_id(
                "ec2",
                network_acl_name,
                opts=__opts__,
                context=__context__,
                sub_resource="network_acl",
                resource_id=acl_id,
                region=region,
                key=key,
                keyid=keyid,
                profile=profile,
            )
        if subnet_id:
            # Find existing association on the subnet to replace.
            existing = conn.describe_network_acls(
                Filters=[{"Name": "association.subnet-id", "Values": [subnet_id]}]
            ).get("NetworkAcls", [])
            assoc_id = None
            for cur in existing:
                for assoc in cur.get("Associations", []):
                    if assoc.get("SubnetId") == subnet_id:
                        assoc_id = assoc.get("NetworkAclAssociationId")
                        break
                if assoc_id:
                    break
            if assoc_id:
                resp = conn.replace_network_acl_association(
                    AssociationId=assoc_id, NetworkAclId=acl_id
                )
                result["association_id"] = resp.get("NewAssociationId")
        return result
    except botocore.exceptions.ClientError as exc:
        return {"created": False, "error": boto3mod.get_error(exc)}


def delete_network_acl(
    network_acl_id=None,
    network_acl_name=None,
    disassociate=False,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Delete a network ACL by id or name. When ``disassociate`` is ``True``
    any existing subnet association is replaced with the VPC's default ACL
    before deletion.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_vpc.delete_network_acl network_acl_id='acl-5fb85d36'
    """
    try:
        conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
        if network_acl_name and not network_acl_id:
            network_acl_id = _get_resource_id(
                "network_acl",
                network_acl_name,
                region=region,
                key=key,
                keyid=keyid,
                profile=profile,
            )
        if not network_acl_id:
            return {
                "deleted": False,
                "error": {"message": f"network acl {network_acl_name} does not exist."},
            }
        if disassociate:
            acls = conn.describe_network_acls(NetworkAclIds=[network_acl_id]).get("NetworkAcls", [])
            if acls:
                vpc_id = acls[0].get("VpcId")
                default_acl = None
                if vpc_id:
                    defaults = conn.describe_network_acls(
                        Filters=[
                            {"Name": "vpc-id", "Values": [vpc_id]},
                            {"Name": "default", "Values": ["true"]},
                        ]
                    ).get("NetworkAcls", [])
                    if defaults:
                        default_acl = defaults[0].get("NetworkAclId")
                for assoc in acls[0].get("Associations", []):
                    assoc_id = assoc.get("NetworkAclAssociationId")
                    if default_acl and assoc_id:
                        try:
                            conn.replace_network_acl_association(
                                AssociationId=assoc_id, NetworkAclId=default_acl
                            )
                        except botocore.exceptions.ClientError:
                            log.exception("Failed to disassociate network acl %s", network_acl_id)
        conn.delete_network_acl(NetworkAclId=network_acl_id)
        if network_acl_name:
            boto3mod.cache_id(
                "ec2",
                network_acl_name,
                opts=__opts__,
                context=__context__,
                sub_resource="network_acl",
                resource_id=network_acl_id,
                invalidate=True,
                region=region,
                key=key,
                keyid=keyid,
                profile=profile,
            )
        return {"deleted": True}
    except botocore.exceptions.ClientError as exc:
        return {"deleted": False, "error": boto3mod.get_error(exc)}


def network_acl_exists(
    network_acl_id=None,
    name=None,
    network_acl_name=None,
    tags=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Return ``{"exists": True}`` if the network ACL exists.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_vpc.network_acl_exists network_acl_id='acl-5fb85d36'
    """
    if name:
        log.warning(
            "boto3_vpc.network_acl_exists: name parameter is deprecated; "
            "use network_acl_name instead."
        )
        network_acl_name = name
    return resource_exists(
        "network_acl",
        name=network_acl_name,
        resource_id=network_acl_id,
        tags=tags,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )


def associate_network_acl_to_subnet(
    network_acl_id=None,
    subnet_id=None,
    network_acl_name=None,
    subnet_name=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Associate a network ACL with a subnet by replacing the subnet's current
    network ACL association.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_vpc.associate_network_acl_to_subnet \\
            network_acl_id='acl-5fb85d36' subnet_id='subnet-6a1fe403'
    """
    try:
        if network_acl_name:
            network_acl_id = _get_resource_id(
                "network_acl",
                network_acl_name,
                region=region,
                key=key,
                keyid=keyid,
                profile=profile,
            )
            if not network_acl_id:
                return {
                    "associated": False,
                    "error": {"message": f"Network ACL {network_acl_name} does not exist."},
                }
        if subnet_name:
            subnet_id = _get_resource_id(
                "subnet", subnet_name, region=region, key=key, keyid=keyid, profile=profile
            )
            if not subnet_id:
                return {
                    "associated": False,
                    "error": {"message": f"Subnet {subnet_name} does not exist."},
                }
        conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
        existing = conn.describe_network_acls(
            Filters=[{"Name": "association.subnet-id", "Values": [subnet_id]}]
        ).get("NetworkAcls", [])
        assoc_id = None
        for cur in existing:
            for assoc in cur.get("Associations", []):
                if assoc.get("SubnetId") == subnet_id:
                    assoc_id = assoc.get("NetworkAclAssociationId")
                    break
            if assoc_id:
                break
        if not assoc_id:
            return {
                "associated": False,
                "error": {
                    "message": f"No existing network acl association found for subnet {subnet_id}."
                },
            }
        resp = conn.replace_network_acl_association(
            AssociationId=assoc_id, NetworkAclId=network_acl_id
        )
        return {"associated": True, "id": resp.get("NewAssociationId")}
    except botocore.exceptions.ClientError as exc:
        return {"associated": False, "error": boto3mod.get_error(exc)}


def disassociate_network_acl(
    subnet_id=None,
    vpc_id=None,
    subnet_name=None,
    vpc_name=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Disassociate the network ACL from a subnet by replacing its association
    with the VPC's default network ACL.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_vpc.disassociate_network_acl 'subnet-6a1fe403'
    """
    if not exactly_one((subnet_name, subnet_id)):
        raise SaltInvocationError(
            "One (but not both) of subnet_id or subnet_name must be provided."
        )
    if all((vpc_name, vpc_id)):
        raise SaltInvocationError("Only one of vpc_id or vpc_name may be provided.")
    try:
        if subnet_name:
            subnet_id = _get_resource_id(
                "subnet", subnet_name, region=region, key=key, keyid=keyid, profile=profile
            )
            if not subnet_id:
                return {
                    "disassociated": False,
                    "error": {"message": f"Subnet {subnet_name} does not exist."},
                }
        if vpc_name or vpc_id:
            vpc_id = check_vpc(vpc_id, vpc_name, region, key, keyid, profile)
        conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
        # find existing association
        existing = conn.describe_network_acls(
            Filters=[{"Name": "association.subnet-id", "Values": [subnet_id]}]
        ).get("NetworkAcls", [])
        assoc_id = None
        owning_vpc = None
        for cur in existing:
            for assoc in cur.get("Associations", []):
                if assoc.get("SubnetId") == subnet_id:
                    assoc_id = assoc.get("NetworkAclAssociationId")
                    owning_vpc = cur.get("VpcId")
                    break
            if assoc_id:
                break
        if not assoc_id:
            return {
                "disassociated": False,
                "error": {
                    "message": f"No existing network acl association for subnet {subnet_id}."
                },
            }
        defaults = conn.describe_network_acls(
            Filters=[
                {"Name": "vpc-id", "Values": [vpc_id or owning_vpc]},
                {"Name": "default", "Values": ["true"]},
            ]
        ).get("NetworkAcls", [])
        if not defaults:
            return {
                "disassociated": False,
                "error": {"message": "Could not find default network acl for VPC."},
            }
        default_acl_id = defaults[0].get("NetworkAclId")
        resp = conn.replace_network_acl_association(
            AssociationId=assoc_id, NetworkAclId=default_acl_id
        )
        return {"disassociated": True, "association_id": resp.get("NewAssociationId")}
    except botocore.exceptions.ClientError as exc:
        return {"disassociated": False, "error": boto3mod.get_error(exc)}


def _network_acl_protocol(protocol):
    if protocol is None:
        return None
    if isinstance(protocol, int):
        return str(protocol)
    if isinstance(protocol, str):
        if protocol.isdigit():
            return protocol
        if protocol == "all":
            return "-1"
        try:
            return str(socket.getprotobyname(protocol))
        except OSError as exc:
            raise SaltInvocationError(str(exc)) from exc
    raise SaltInvocationError(f"Invalid protocol {protocol!r}")


def _do_network_acl_entry(replace, **kwargs):
    rkey = "replaced" if replace else "created"
    network_acl_id = kwargs.get("network_acl_id")
    network_acl_name = kwargs.get("network_acl_name")
    region = kwargs.get("region")
    key = kwargs.get("key")
    keyid = kwargs.get("keyid")
    profile = kwargs.get("profile")
    if not exactly_one((network_acl_name, network_acl_id)):
        raise SaltInvocationError(
            "One (but not both) of network_acl_id or network_acl_name must be provided."
        )
    for v in ("rule_number", "protocol", "rule_action", "cidr_block"):
        if kwargs.get(v) is None:
            raise SaltInvocationError(f"{v} is required.")
    if network_acl_name:
        network_acl_id = _get_resource_id(
            "network_acl",
            network_acl_name,
            region=region,
            key=key,
            keyid=keyid,
            profile=profile,
        )
    if not network_acl_id:
        return {
            rkey: False,
            "error": {
                "message": "Network ACL {} does not exist.".format(
                    network_acl_name or network_acl_id
                )
            },
        }
    try:
        conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
        entry = {
            "NetworkAclId": network_acl_id,
            "RuleNumber": int(kwargs["rule_number"]),
            "Protocol": _network_acl_protocol(kwargs["protocol"]),
            "RuleAction": kwargs["rule_action"],
            "Egress": bool(kwargs.get("egress")),
            "CidrBlock": kwargs["cidr_block"],
        }
        if kwargs.get("port_range_from") is not None or kwargs.get("port_range_to") is not None:
            entry["PortRange"] = {
                "From": int(kwargs.get("port_range_from") or 0),
                "To": int(kwargs.get("port_range_to") or 0),
            }
        if kwargs.get("icmp_type") is not None or kwargs.get("icmp_code") is not None:
            entry["IcmpTypeCode"] = {
                "Type": int(kwargs.get("icmp_type") or 0),
                "Code": int(kwargs.get("icmp_code") or 0),
            }
        if replace:
            conn.replace_network_acl_entry(**entry)
        else:
            conn.create_network_acl_entry(**entry)
        return {rkey: True}
    except botocore.exceptions.ClientError as exc:
        return {rkey: False, "error": boto3mod.get_error(exc)}


def create_network_acl_entry(
    network_acl_id=None,
    rule_number=None,
    protocol=None,
    rule_action=None,
    cidr_block=None,
    egress=None,
    network_acl_name=None,
    icmp_code=None,
    icmp_type=None,
    port_range_from=None,
    port_range_to=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Create a network ACL entry.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_vpc.create_network_acl_entry 'acl-5fb85d36' 32767 \\
            'all' 'deny' '0.0.0.0/0' egress=True
    """
    return _do_network_acl_entry(
        False,
        network_acl_id=network_acl_id,
        rule_number=rule_number,
        protocol=protocol,
        rule_action=rule_action,
        cidr_block=cidr_block,
        egress=egress,
        network_acl_name=network_acl_name,
        icmp_code=icmp_code,
        icmp_type=icmp_type,
        port_range_from=port_range_from,
        port_range_to=port_range_to,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )


def replace_network_acl_entry(
    network_acl_id=None,
    rule_number=None,
    protocol=None,
    rule_action=None,
    cidr_block=None,
    egress=None,
    network_acl_name=None,
    icmp_code=None,
    icmp_type=None,
    port_range_from=None,
    port_range_to=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Replace a network ACL entry.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_vpc.replace_network_acl_entry 'acl-5fb85d36' 32767 \\
            'all' 'deny' '0.0.0.0/0' egress=True
    """
    return _do_network_acl_entry(
        True,
        network_acl_id=network_acl_id,
        rule_number=rule_number,
        protocol=protocol,
        rule_action=rule_action,
        cidr_block=cidr_block,
        egress=egress,
        network_acl_name=network_acl_name,
        icmp_code=icmp_code,
        icmp_type=icmp_type,
        port_range_from=port_range_from,
        port_range_to=port_range_to,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )


def delete_network_acl_entry(
    network_acl_id=None,
    rule_number=None,
    egress=None,
    network_acl_name=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Delete a network ACL entry.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_vpc.delete_network_acl_entry 'acl-5fb85d36' 32767
    """
    if not exactly_one((network_acl_name, network_acl_id)):
        raise SaltInvocationError(
            "One (but not both) of network_acl_id or network_acl_name must be provided."
        )
    for v in ("rule_number", "egress"):
        if locals()[v] is None:
            raise SaltInvocationError(f"{v} is required.")
    if network_acl_name:
        network_acl_id = _get_resource_id(
            "network_acl",
            network_acl_name,
            region=region,
            key=key,
            keyid=keyid,
            profile=profile,
        )
    if not network_acl_id:
        return {
            "deleted": False,
            "error": {
                "message": "Network ACL {} does not exist.".format(
                    network_acl_name or network_acl_id
                )
            },
        }
    try:
        conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
        conn.delete_network_acl_entry(
            NetworkAclId=network_acl_id, RuleNumber=int(rule_number), Egress=bool(egress)
        )
        return {"deleted": True}
    except botocore.exceptions.ClientError as exc:
        return {"deleted": False, "error": boto3mod.get_error(exc)}


def create_route_table(
    vpc_id=None,
    vpc_name=None,
    route_table_name=None,
    tags=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Create a route table in the specified VPC.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_vpc.create_route_table vpc_name='myvpc' \\
            route_table_name='myroutetable'
    """
    try:
        vpc_id = check_vpc(vpc_id, vpc_name, region, key, keyid, profile)
        if not vpc_id:
            return {
                "created": False,
                "error": {"message": f"VPC {vpc_name or vpc_id} does not exist."},
            }
        conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
        kwargs = {"VpcId": vpc_id}
        tag_spec = _tag_specifications("route-table", name=route_table_name, tags=tags)
        if tag_spec:
            kwargs["TagSpecifications"] = tag_spec
        rt = conn.create_route_table(**kwargs).get("RouteTable") or {}
        rt_id = rt.get("RouteTableId")
        if not rt_id:
            return {"created": False}
        if route_table_name:
            boto3mod.cache_id(
                "ec2",
                route_table_name,
                opts=__opts__,
                context=__context__,
                sub_resource="route_table",
                resource_id=rt_id,
                region=region,
                key=key,
                keyid=keyid,
                profile=profile,
            )
        return {"created": True, "id": rt_id}
    except botocore.exceptions.ClientError as exc:
        return {"created": False, "error": boto3mod.get_error(exc)}


def delete_route_table(
    route_table_id=None,
    route_table_name=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Delete a route table by id or name.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_vpc.delete_route_table route_table_id='rtb-1f382e7d'
    """
    try:
        if route_table_name and not route_table_id:
            route_table_id = _get_resource_id(
                "route_table",
                route_table_name,
                region=region,
                key=key,
                keyid=keyid,
                profile=profile,
            )
        if not route_table_id:
            return {
                "deleted": False,
                "error": {"message": f"route table {route_table_name} does not exist."},
            }
        conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
        conn.delete_route_table(RouteTableId=route_table_id)
        if route_table_name:
            boto3mod.cache_id(
                "ec2",
                route_table_name,
                opts=__opts__,
                context=__context__,
                sub_resource="route_table",
                resource_id=route_table_id,
                invalidate=True,
                region=region,
                key=key,
                keyid=keyid,
                profile=profile,
            )
        return {"deleted": True}
    except botocore.exceptions.ClientError as exc:
        return {"deleted": False, "error": boto3mod.get_error(exc)}


def route_table_exists(
    route_table_id=None,
    name=None,
    route_table_name=None,
    tags=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Return ``{"exists": True}`` if the route table exists.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_vpc.route_table_exists route_table_id='rtb-1f382e7d'
    """
    if name:
        log.warning(
            "boto3_vpc.route_table_exists: name parameter is deprecated; "
            "use route_table_name instead."
        )
        route_table_name = name
    return resource_exists(
        "route_table",
        name=route_table_name,
        resource_id=route_table_id,
        tags=tags,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )


_ROUTE_KEYS = {
    "destination_cidr_block": "DestinationCidrBlock",
    "gateway_id": "GatewayId",
    "instance_id": "InstanceId",
    "interface_id": "NetworkInterfaceId",
    "nat_gateway_id": "NatGatewayId",
    "vpc_peering_connection_id": "VpcPeeringConnectionId",
}
_ASSOC_KEYS = {
    "id": "RouteTableAssociationId",
    "main": "Main",
    "route_table_id": "RouteTableId",
    "subnet_id": "SubnetId",
}


def _route_payload(rt):
    routes = []
    for r in rt.get("Routes", []) or []:
        routes.append({k: r.get(v) for k, v in _ROUTE_KEYS.items() if v in r})
    assocs = []
    for a in rt.get("Associations", []) or []:
        assocs.append({k: a.get(v) for k, v in _ASSOC_KEYS.items() if v in a})
    return {
        "id": rt.get("RouteTableId"),
        "vpc_id": rt.get("VpcId"),
        "tags": _tags_dict(rt.get("Tags")),
        "routes": routes,
        "associations": assocs,
    }


def describe_route_tables(
    route_table_id=None,
    route_table_name=None,
    vpc_id=None,
    tags=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Return a list of route tables matching the filter criteria.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_vpc.describe_route_tables vpc_id='vpc-a6a9efc3'
    """
    if not any((route_table_id, route_table_name, tags, vpc_id)):
        raise SaltInvocationError(
            "At least one of the following must be specified: "
            "route table id, route table name, vpc_id, or tags."
        )
    try:
        conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
        kwargs = {"Filters": []}
        if route_table_id:
            kwargs["RouteTableIds"] = [route_table_id]
        if vpc_id:
            kwargs["Filters"].append({"Name": "vpc-id", "Values": [vpc_id]})
        if route_table_name:
            kwargs["Filters"].append({"Name": "tag:Name", "Values": [route_table_name]})
        for tag_name, tag_value in (tags or {}).items():
            kwargs["Filters"].append({"Name": f"tag:{tag_name}", "Values": [tag_value]})
        if not kwargs["Filters"]:
            del kwargs["Filters"]
        tables = conn.describe_route_tables(**kwargs).get("RouteTables", [])
    except botocore.exceptions.ClientError as exc:
        return {"error": boto3mod.get_error(exc)}
    return [_route_payload(rt) for rt in tables]


def route_exists(
    destination_cidr_block,
    route_table_name=None,
    route_table_id=None,
    gateway_id=None,
    instance_id=None,
    interface_id=None,
    tags=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
    vpc_peering_connection_id=None,
    nat_gateway_id=None,
):
    """
    Return ``{"exists": True}`` if a matching route is present in the
    specified route table.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_vpc.route_exists destination_cidr_block='10.0.0.0/20' \\
            gateway_id='local' route_table_name='test'
    """
    if not any((route_table_name, route_table_id)):
        raise SaltInvocationError(
            "At least one of the following must be specified: "
            "route_table_name or route_table_id."
        )
    if not any((gateway_id, instance_id, interface_id, vpc_peering_connection_id, nat_gateway_id)):
        raise SaltInvocationError(
            "At least one of the following must be specified: gateway_id, instance_id, "
            "interface_id, nat_gateway_id or vpc_peering_connection_id."
        )
    tables = describe_route_tables(
        route_table_id=route_table_id,
        route_table_name=route_table_name,
        tags=tags,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )
    if isinstance(tables, dict) and "error" in tables:
        return tables
    if not tables:
        return {"exists": False}
    if len(tables) != 1:
        raise CommandExecutionError("Found more than one route table.")
    want = {
        "destination_cidr_block": destination_cidr_block,
        "gateway_id": gateway_id,
        "instance_id": instance_id,
        "interface_id": interface_id,
        "vpc_peering_connection_id": vpc_peering_connection_id,
        "nat_gateway_id": nat_gateway_id,
    }
    for route in tables[0]["routes"]:
        have = {k: route.get(k) for k in want}
        if have == want:
            return {"exists": True}
    return {"exists": False}


def associate_route_table(
    route_table_id=None,
    subnet_id=None,
    route_table_name=None,
    subnet_name=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Associate a route table with a subnet.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_vpc.associate_route_table 'rtb-1f382e7d' 'subnet-6a1fe403'
    """
    if all((subnet_id, subnet_name)):
        raise SaltInvocationError("Only one of subnet_name or subnet_id may be provided.")
    if all((route_table_id, route_table_name)):
        raise SaltInvocationError("Only one of route_table_name or route_table_id may be provided.")
    try:
        if subnet_name:
            subnet_id = _get_resource_id(
                "subnet", subnet_name, region=region, key=key, keyid=keyid, profile=profile
            )
            if not subnet_id:
                return {
                    "associated": False,
                    "error": {"message": f"Subnet {subnet_name} does not exist."},
                }
        if route_table_name:
            route_table_id = _get_resource_id(
                "route_table",
                route_table_name,
                region=region,
                key=key,
                keyid=keyid,
                profile=profile,
            )
            if not route_table_id:
                return {
                    "associated": False,
                    "error": {"message": f"Route table {route_table_name} does not exist."},
                }
        conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
        resp = conn.associate_route_table(RouteTableId=route_table_id, SubnetId=subnet_id)
        return {"association_id": resp.get("AssociationId")}
    except botocore.exceptions.ClientError as exc:
        return {"associated": False, "error": boto3mod.get_error(exc)}


def disassociate_route_table(association_id, region=None, key=None, keyid=None, profile=None):
    """
    Disassociate a route table.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_vpc.disassociate_route_table 'rtbassoc-d8ccddba'
    """
    try:
        conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
        conn.disassociate_route_table(AssociationId=association_id)
        return {"disassociated": True}
    except botocore.exceptions.ClientError as exc:
        return {"disassociated": False, "error": boto3mod.get_error(exc)}


def replace_route_table_association(
    association_id, route_table_id, region=None, key=None, keyid=None, profile=None
):
    """
    Replace a route table association with a new route table.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_vpc.replace_route_table_association 'rtbassoc-d8ccddba' 'rtb-1f382e7d'
    """
    try:
        conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
        resp = conn.replace_route_table_association(
            AssociationId=association_id, RouteTableId=route_table_id
        )
        return {"replaced": True, "association_id": resp.get("NewAssociationId")}
    except botocore.exceptions.ClientError as exc:
        return {"replaced": False, "error": boto3mod.get_error(exc)}


def create_route(
    route_table_id=None,
    destination_cidr_block=None,
    route_table_name=None,
    gateway_id=None,
    internet_gateway_name=None,
    instance_id=None,
    interface_id=None,
    vpc_peering_connection_id=None,
    vpc_peering_connection_name=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
    nat_gateway_id=None,
    nat_gateway_subnet_name=None,
    nat_gateway_subnet_id=None,
):
    """
    Create a route in a route table.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_vpc.create_route 'rtb-1f382e7d' '10.0.0.0/16' gateway_id='vgw-a1b2c3'
    """
    if not exactly_one((route_table_name, route_table_id)):
        raise SaltInvocationError(
            "One (but not both) of route_table_id or route_table_name must be provided."
        )
    if not exactly_one(
        (
            gateway_id,
            internet_gateway_name,
            instance_id,
            interface_id,
            vpc_peering_connection_id,
            nat_gateway_id,
            nat_gateway_subnet_id,
            nat_gateway_subnet_name,
            vpc_peering_connection_name,
        )
    ):
        raise SaltInvocationError(
            "Exactly one of gateway_id, internet_gateway_name, instance_id, interface_id, "
            "vpc_peering_connection_id, nat_gateway_id, nat_gateway_subnet_id, "
            "nat_gateway_subnet_name or vpc_peering_connection_name must be provided."
        )
    if destination_cidr_block is None:
        raise SaltInvocationError("destination_cidr_block is required.")
    try:
        if route_table_name:
            route_table_id = _get_resource_id(
                "route_table",
                route_table_name,
                region=region,
                key=key,
                keyid=keyid,
                profile=profile,
            )
            if not route_table_id:
                return {
                    "created": False,
                    "error": {"message": f"route table {route_table_name} does not exist."},
                }
        if internet_gateway_name:
            gateway_id = _get_resource_id(
                "internet_gateway",
                internet_gateway_name,
                region=region,
                key=key,
                keyid=keyid,
                profile=profile,
            )
            if not gateway_id:
                return {
                    "created": False,
                    "error": {
                        "message": f"internet gateway {internet_gateway_name} does not exist."
                    },
                }
        if vpc_peering_connection_name:
            vpc_peering_connection_id = _get_resource_id(
                "vpc_peering_connection",
                vpc_peering_connection_name,
                region=region,
                key=key,
                keyid=keyid,
                profile=profile,
            )
            if not vpc_peering_connection_id:
                return {
                    "created": False,
                    "error": {
                        "message": (
                            f"VPC peering connection {vpc_peering_connection_name} "
                            "does not exist."
                        )
                    },
                }
        if nat_gateway_subnet_name:
            gws = describe_nat_gateways(
                subnet_name=nat_gateway_subnet_name,
                region=region,
                key=key,
                keyid=keyid,
                profile=profile,
            )
            if not gws:
                return {
                    "created": False,
                    "error": {
                        "message": (f"nat gateway for {nat_gateway_subnet_name} does not exist.")
                    },
                }
            nat_gateway_id = gws[0]["NatGatewayId"]
        if nat_gateway_subnet_id:
            gws = describe_nat_gateways(
                subnet_id=nat_gateway_subnet_id,
                region=region,
                key=key,
                keyid=keyid,
                profile=profile,
            )
            if not gws:
                return {
                    "created": False,
                    "error": {
                        "message": (f"nat gateway for {nat_gateway_subnet_id} does not exist.")
                    },
                }
            nat_gateway_id = gws[0]["NatGatewayId"]

        conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
        kwargs = {
            "RouteTableId": route_table_id,
            "DestinationCidrBlock": destination_cidr_block,
        }
        if gateway_id:
            kwargs["GatewayId"] = gateway_id
        if instance_id:
            kwargs["InstanceId"] = instance_id
        if interface_id:
            kwargs["NetworkInterfaceId"] = interface_id
        if vpc_peering_connection_id:
            kwargs["VpcPeeringConnectionId"] = vpc_peering_connection_id
        if nat_gateway_id:
            kwargs["NatGatewayId"] = nat_gateway_id
        conn.create_route(**kwargs)
        return {"created": True}
    except botocore.exceptions.ClientError as exc:
        return {"created": False, "error": boto3mod.get_error(exc)}


def delete_route(
    route_table_id=None,
    destination_cidr_block=None,
    route_table_name=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Delete a route from a route table.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_vpc.delete_route 'rtb-1f382e7d' '10.0.0.0/16'
    """
    if not exactly_one((route_table_name, route_table_id)):
        raise SaltInvocationError(
            "One (but not both) of route_table_id or route_table_name must be provided."
        )
    if destination_cidr_block is None:
        raise SaltInvocationError("destination_cidr_block is required.")
    try:
        if route_table_name:
            route_table_id = _get_resource_id(
                "route_table",
                route_table_name,
                region=region,
                key=key,
                keyid=keyid,
                profile=profile,
            )
            if not route_table_id:
                return {
                    "deleted": False,
                    "error": {"message": f"route table {route_table_name} does not exist."},
                }
        conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
        conn.delete_route(RouteTableId=route_table_id, DestinationCidrBlock=destination_cidr_block)
        return {"deleted": True}
    except botocore.exceptions.ClientError as exc:
        return {"deleted": False, "error": boto3mod.get_error(exc)}


def replace_route(
    route_table_id=None,
    destination_cidr_block=None,
    route_table_name=None,
    gateway_id=None,
    instance_id=None,
    interface_id=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
    vpc_peering_connection_id=None,
    nat_gateway_id=None,
):
    """
    Replace an existing route in a route table.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_vpc.replace_route 'rtb-1f382e7d' '10.0.0.0/16' gateway_id='vgw-a1b2c3'
    """
    if not exactly_one((route_table_name, route_table_id)):
        raise SaltInvocationError(
            "One (but not both) of route_table_id or route_table_name must be provided."
        )
    if destination_cidr_block is None:
        raise SaltInvocationError("destination_cidr_block is required.")
    try:
        if route_table_name:
            route_table_id = _get_resource_id(
                "route_table",
                route_table_name,
                region=region,
                key=key,
                keyid=keyid,
                profile=profile,
            )
            if not route_table_id:
                return {
                    "replaced": False,
                    "error": {"message": f"route table {route_table_name} does not exist."},
                }
        conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
        kwargs = {
            "RouteTableId": route_table_id,
            "DestinationCidrBlock": destination_cidr_block,
        }
        if gateway_id:
            kwargs["GatewayId"] = gateway_id
        if instance_id:
            kwargs["InstanceId"] = instance_id
        if interface_id:
            kwargs["NetworkInterfaceId"] = interface_id
        if vpc_peering_connection_id:
            kwargs["VpcPeeringConnectionId"] = vpc_peering_connection_id
        if nat_gateway_id:
            kwargs["NatGatewayId"] = nat_gateway_id
        conn.replace_route(**kwargs)
        return {"replaced": True}
    except botocore.exceptions.ClientError as exc:
        return {"replaced": False, "error": boto3mod.get_error(exc)}


_VPC_PEERING_ACTIVE_STATES = ("active", "pending-acceptance", "provisioning")


def _get_peering_connection_ids(name, conn):
    filters = [
        {"Name": "tag:Name", "Values": [name]},
        {"Name": "status-code", "Values": list(_VPC_PEERING_ACTIVE_STATES)},
    ]
    peerings = conn.describe_vpc_peering_connections(Filters=filters).get(
        "VpcPeeringConnections", []
    )
    return [p["VpcPeeringConnectionId"] for p in peerings]


def _vpc_peering_conn_id_for_name(name, conn):
    ids = _get_peering_connection_ids(name, conn)
    if not ids:
        return None
    if len(ids) > 1:
        raise CommandExecutionError(
            f"Found multiple VPC peering connections named {name}; use an ID instead."
        )
    return ids[0]


def request_vpc_peering_connection(
    requester_vpc_id=None,
    requester_vpc_name=None,
    peer_vpc_id=None,
    peer_vpc_name=None,
    name=None,
    peer_owner_id=None,
    peer_region=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
    dry_run=False,
):
    """
    Request a VPC peering connection between two VPCs.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_vpc.request_vpc_peering_connection vpc-4a3e622e vpc-be82e9da \\
            name=my_vpc_connection
    """
    if not exactly_one((requester_vpc_id, requester_vpc_name)):
        raise SaltInvocationError(
            "Exactly one of requester_vpc_id or requester_vpc_name is required."
        )
    if not exactly_one((peer_vpc_id, peer_vpc_name)):
        raise SaltInvocationError("Exactly one of peer_vpc_id or peer_vpc_name is required.")
    try:
        conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
        if name and _vpc_peering_conn_id_for_name(name, conn):
            raise SaltInvocationError(f"A VPC peering connection named {name} already exists.")
        if requester_vpc_name:
            requester_vpc_id = _get_id(
                vpc_name=requester_vpc_name,
                region=region,
                key=key,
                keyid=keyid,
                profile=profile,
            )
            if not requester_vpc_id:
                return {"error": f"Could not resolve VPC name {requester_vpc_name} to an ID"}
        if peer_vpc_name:
            peer_vpc_id = _get_id(
                vpc_name=peer_vpc_name,
                region=region,
                key=key,
                keyid=keyid,
                profile=profile,
            )
            if not peer_vpc_id:
                return {"error": f"Could not resolve VPC name {peer_vpc_name} to an ID"}
        params = {"VpcId": requester_vpc_id, "PeerVpcId": peer_vpc_id, "DryRun": dry_run}
        if peer_owner_id:
            params["PeerOwnerId"] = peer_owner_id
        if peer_region:
            params["PeerRegion"] = peer_region
        resp = conn.create_vpc_peering_connection(**params)
        peering = resp.get("VpcPeeringConnection", {})
        conn_id = peering.get("VpcPeeringConnectionId", "ERROR")
        msg = f"VPC peering {conn_id} requested."
        if name:
            conn.create_tags(Resources=[conn_id], Tags=[{"Key": "Name", "Value": name}])
            msg += f" With name {name}."
        return {"msg": msg}
    except botocore.exceptions.ClientError as exc:
        return {"error": boto3mod.get_error(exc)}


def describe_vpc_peering_connection(name, region=None, key=None, keyid=None, profile=None):
    """
    Return any VPC peering connection ids for the given VPC peering
    connection name that are ``active``, ``pending-acceptance`` or
    ``provisioning``.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_vpc.describe_vpc_peering_connection salt-vpc
    """
    conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
    try:
        return {"VPC-Peerings": _get_peering_connection_ids(name, conn)}
    except botocore.exceptions.ClientError as exc:
        return {"error": boto3mod.get_error(exc)}


def accept_vpc_peering_connection(
    conn_id="",
    name="",
    region=None,
    key=None,
    keyid=None,
    profile=None,
    dry_run=False,
):
    """
    Accept a pending VPC peering connection request.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_vpc.accept_vpc_peering_connection name=salt-vpc
    """
    if not exactly_one((conn_id, name)):
        raise SaltInvocationError("One (but not both) of conn_id or name must be provided.")
    conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
    if name:
        conn_id = _vpc_peering_conn_id_for_name(name, conn)
        if not conn_id:
            raise SaltInvocationError(f"No ID found for VPC peering connection named {name}.")
    try:
        conn.accept_vpc_peering_connection(DryRun=dry_run, VpcPeeringConnectionId=conn_id)
        return {"msg": "VPC peering connection accepted."}
    except botocore.exceptions.ClientError as exc:
        return {"error": boto3mod.get_error(exc)}


def delete_vpc_peering_connection(
    conn_id=None,
    conn_name=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
    dry_run=False,
):
    """
    Delete a VPC peering connection by id or name.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_vpc.delete_vpc_peering_connection conn_name=salt-vpc
    """
    if not exactly_one((conn_id, conn_name)):
        raise SaltInvocationError("Exactly one of conn_id or conn_name must be provided.")
    conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
    if conn_name:
        conn_id = _vpc_peering_conn_id_for_name(conn_name, conn)
        if not conn_id:
            raise SaltInvocationError(
                f"Couldn't resolve VPC peering connection {conn_name} to an ID."
            )
    try:
        conn.delete_vpc_peering_connection(DryRun=dry_run, VpcPeeringConnectionId=conn_id)
        return {"msg": "VPC peering connection deleted."}
    except botocore.exceptions.ClientError as exc:
        return {"error": boto3mod.get_error(exc)}


def is_peering_connection_pending(
    conn_id=None,
    conn_name=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Return ``True`` if the VPC peering connection is in the
    ``pending-acceptance`` state.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_vpc.is_peering_connection_pending conn_name=salt-vpc
    """
    if not exactly_one((conn_id, conn_name)):
        raise SaltInvocationError("Exactly one of conn_id or conn_name must be provided.")
    conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
    try:
        if conn_id:
            vpcs = conn.describe_vpc_peering_connections(VpcPeeringConnectionIds=[conn_id]).get(
                "VpcPeeringConnections", []
            )
        else:
            filters = [
                {"Name": "tag:Name", "Values": [conn_name]},
                {"Name": "status-code", "Values": list(_VPC_PEERING_ACTIVE_STATES)},
            ]
            vpcs = conn.describe_vpc_peering_connections(Filters=filters).get(
                "VpcPeeringConnections", []
            )
    except botocore.exceptions.ClientError as exc:
        log.error("Failed to describe VPC peering connections: %s", exc)
        return False
    if not vpcs:
        return False
    if len(vpcs) > 1:
        raise CommandExecutionError(
            f"Found more than one VPC peering connection for {conn_id or conn_name}."
        )
    return vpcs[0]["Status"]["Code"] == "pending-acceptance"


def peering_connection_pending_from_vpc(
    conn_id=None,
    conn_name=None,
    vpc_id=None,
    vpc_name=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Return ``True`` if a VPC peering connection is pending from the given
    requester VPC.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_vpc.peering_connection_pending_from_vpc conn_name=salt-vpc \\
            vpc_name=myvpc
    """
    if not exactly_one((conn_id, conn_name)):
        raise SaltInvocationError("Exactly one of conn_id or conn_name must be provided.")
    if not exactly_one((vpc_id, vpc_name)):
        raise SaltInvocationError("Exactly one of vpc_id or vpc_name must be provided.")
    if vpc_name:
        vpc_id = check_vpc(vpc_name=vpc_name, region=region, key=key, keyid=keyid, profile=profile)
        if not vpc_id:
            return False
    conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
    filters = [
        {"Name": "requester-vpc-info.vpc-id", "Values": [vpc_id]},
        {"Name": "status-code", "Values": list(_VPC_PEERING_ACTIVE_STATES)},
    ]
    if conn_id:
        filters.append({"Name": "vpc-peering-connection-id", "Values": [conn_id]})
    else:
        filters.append({"Name": "tag:Name", "Values": [conn_name]})
    try:
        vpcs = conn.describe_vpc_peering_connections(Filters=filters).get(
            "VpcPeeringConnections", []
        )
    except botocore.exceptions.ClientError as exc:
        log.error("Failed to describe VPC peering connections: %s", exc)
        return False
    if not vpcs:
        return False
    if len(vpcs) > 1:
        raise CommandExecutionError(
            f"Found more than one VPC peering connection for {conn_id or conn_name}."
        )
    return vpcs[0]["Status"]["Code"] == "pending-acceptance"
