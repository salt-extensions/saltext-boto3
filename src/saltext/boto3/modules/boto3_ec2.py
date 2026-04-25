"""
Connection module for Amazon EC2 using boto3.
=============================================

    Renamed from ``boto_ec2`` to ``boto3_ec2`` and rewritten to use the
    boto3 ``ec2`` client APIs directly via
    :py:mod:`saltext.boto3.utils.boto3mod`.  The legacy boto2 code path
    (object-style access, retry loops) has been removed.

:depends:
  - boto3 >= 1.28.0
  - botocore >= 1.31.0

:configuration: This module accepts explicit EC2 credentials but can
    also utilize IAM roles assigned to the instance through Instance Profiles.
    Dynamic credentials are then automatically obtained from AWS API and no
    further configuration is necessary. More Information available at:

    .. code-block:: text

        http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html

    If IAM roles are not used you need to specify them either in the minion's
    config file or as a profile. For example, to specify them in the minion's
    config file:

.. code-block:: yaml

    ec2.keyid: GKTADJGHEIQSXMKKRBJ08H
    ec2.key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs

A region may also be specified in the configuration:

.. code-block:: yaml

    ec2.region: us-east-1

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
import os
import time

import salt.utils.data
import salt.utils.json
from salt.exceptions import CommandExecutionError
from salt.exceptions import SaltInvocationError

from saltext.boto3.utils import boto3mod

try:
    from botocore.exceptions import ClientError

    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

log = logging.getLogger(__name__)

__virtualname__ = "boto3_ec2"

# Attributes valid for describe_instance_attribute / modify_instance_attribute.
_ATTRIBUTE_LIST = [
    "instanceType",
    "kernel",
    "ramdisk",
    "userData",
    "disableApiTermination",
    "instanceInitiatedShutdownBehavior",
    "rootDeviceName",
    "blockDeviceMapping",
    "productCodes",
    "sourceDestCheck",
    "groupSet",
    "ebsOptimized",
    "sriovNetSupport",
]

_EIP_KEYS = [
    ("AllocationId", "allocation_id"),
    ("AssociationId", "association_id"),
    ("Domain", "domain"),
    ("InstanceId", "instance_id"),
    ("NetworkInterfaceId", "network_interface_id"),
    ("NetworkInterfaceOwnerId", "network_interface_owner_id"),
    ("PublicIp", "public_ip"),
    ("PrivateIpAddress", "private_ip_address"),
]


def __virtual__():
    if HAS_BOTO3:
        return __virtualname__
    return (False, "The boto3_ec2 module could not be loaded: boto3 is not available.")


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


def _paginate(fn, key, **kwargs):
    """Yield items from a paginated ec2 API response."""
    next_token = ""
    while next_token is not None:
        call_kwargs = dict(kwargs)
        if next_token:
            call_kwargs["NextToken"] = next_token
        resp = fn(**call_kwargs)
        yield from resp.get(key, [])
        next_token = resp.get("NextToken")
        if not next_token:
            return


def _filters_to_aws(filters):
    """Translate a {"name": "value"} (or list) dict to the AWS list form."""
    out = []
    for name, value in (filters or {}).items():
        if not isinstance(value, (list, tuple)):
            value = [value]
        out.append({"Name": name, "Values": [str(v) for v in value]})
    return out


def _eip_info(addr):
    """Translate a describe_addresses entry into the legacy snake_case dict."""
    return {snake: addr.get(aws) for aws, snake in _EIP_KEYS}


def _get_all_eip_addresses(
    addresses=None, allocation_ids=None, region=None, key=None, keyid=None, profile=None
):
    conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
    try:
        kwargs = {}
        if addresses:
            kwargs["PublicIps"] = (
                list(addresses) if isinstance(addresses, (list, tuple)) else [addresses]
            )
        if allocation_ids:
            kwargs["AllocationIds"] = (
                list(allocation_ids)
                if isinstance(allocation_ids, (list, tuple))
                else [allocation_ids]
            )
        return conn.describe_addresses(**kwargs).get("Addresses", [])
    except ClientError as e:
        log.error(e)
        return []


def get_all_eip_addresses(
    addresses=None, allocation_ids=None, region=None, key=None, keyid=None, profile=None
):
    """
    Get public addresses of some, or all EIPs associated with the current account.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.get_all_eip_addresses

    """
    return [
        addr.get("PublicIp")
        for addr in _get_all_eip_addresses(addresses, allocation_ids, region, key, keyid, profile)
    ]


def get_unassociated_eip_address(
    domain="standard", region=None, key=None, keyid=None, profile=None
):
    """
    Return the first unassociated EIP (public IP string), or None.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.get_unassociated_eip_address

    """
    for address in get_all_eip_addresses(region=region, key=key, keyid=keyid, profile=profile):
        info = get_eip_address_info(
            addresses=address, region=region, key=key, keyid=keyid, profile=profile
        )[0]
        if info["instance_id"] or info["network_interface_id"]:
            continue
        if info["domain"] == domain:
            return address
    log.debug("No unassociated Elastic IP found!")
    return None


def get_eip_address_info(
    addresses=None, allocation_ids=None, region=None, key=None, keyid=None, profile=None
):
    """
    Get 'interesting' info about some, or all EIPs associated with the account.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.get_eip_address_info

    """
    if isinstance(addresses, str):
        addresses = [addresses]
    if isinstance(allocation_ids, str):
        allocation_ids = [allocation_ids]
    return [
        _eip_info(a)
        for a in _get_all_eip_addresses(
            addresses=addresses,
            allocation_ids=allocation_ids,
            region=region,
            key=key,
            keyid=keyid,
            profile=profile,
        )
    ]


def allocate_eip_address(domain=None, region=None, key=None, keyid=None, profile=None):
    """
    Allocate a new Elastic IP address and return dict of details, or False.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.allocate_eip_address

    """
    if domain and domain != "vpc":
        raise SaltInvocationError("The only permitted value for the 'domain' param is 'vpc'.")
    conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
    try:
        kwargs = {}
        if domain:
            kwargs["Domain"] = domain
        resp = conn.allocate_address(**kwargs)
    except ClientError as e:
        log.error(e)
        return False
    return _eip_info(resp)


def release_eip_address(
    public_ip=None, allocation_id=None, region=None, key=None, keyid=None, profile=None
):
    """Free an Elastic IP address. Returns True on success.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.release_eip_address

    """
    if not salt.utils.data.exactly_one((public_ip, allocation_id)):
        raise SaltInvocationError("Exactly one of 'public_ip' OR 'allocation_id' must be provided")
    conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
    try:
        kwargs = {}
        if public_ip:
            kwargs["PublicIp"] = public_ip
        if allocation_id:
            kwargs["AllocationId"] = allocation_id
        conn.release_address(**kwargs)
        return True
    except ClientError as e:
        log.error(e)
        return False


def associate_eip_address(
    instance_id=None,
    instance_name=None,
    public_ip=None,
    allocation_id=None,
    network_interface_id=None,
    network_interface_name=None,
    private_ip_address=None,
    allow_reassociation=False,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Associate an Elastic IP address with a running instance or network interface.
    Returns True on success.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.associate_eip_address

    """
    if not salt.utils.data.exactly_one(
        (instance_id, instance_name, network_interface_id, network_interface_name)
    ):
        raise SaltInvocationError(
            "Exactly one of 'instance_id', 'instance_name', "
            "'network_interface_id', 'network_interface_name' must be provided"
        )
    conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
    if instance_name:
        try:
            instance_id = get_id(
                name=instance_name, region=region, key=key, keyid=keyid, profile=profile
            )
        except ClientError as e:
            log.error(e)
            return False
        if not instance_id:
            log.error("Given instance_name '%s' cannot be mapped to an instance_id", instance_name)
            return False
    if network_interface_name:
        r = get_network_interface_id(
            network_interface_name, region=region, key=key, keyid=keyid, profile=profile
        )
        network_interface_id = r.get("result")
        if not network_interface_id:
            log.error(
                "Given network_interface_name '%s' cannot be mapped to a network_interface_id",
                network_interface_name,
            )
            return False
    kwargs = {"AllowReassociation": bool(allow_reassociation)}
    if instance_id:
        kwargs["InstanceId"] = instance_id
    if public_ip:
        kwargs["PublicIp"] = public_ip
    if allocation_id:
        kwargs["AllocationId"] = allocation_id
    if network_interface_id:
        kwargs["NetworkInterfaceId"] = network_interface_id
    if private_ip_address:
        kwargs["PrivateIpAddress"] = private_ip_address
    try:
        conn.associate_address(**kwargs)
        return True
    except ClientError as e:
        log.error(e)
        return False


def disassociate_eip_address(
    public_ip=None, association_id=None, region=None, key=None, keyid=None, profile=None
):
    """Disassociate an Elastic IP address. Returns True on success.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.disassociate_eip_address

    """
    conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
    kwargs = {}
    if public_ip:
        kwargs["PublicIp"] = public_ip
    if association_id:
        kwargs["AssociationId"] = association_id
    try:
        conn.disassociate_address(**kwargs)
        return True
    except ClientError as e:
        log.error(e)
        return False


def assign_private_ip_addresses(
    network_interface_name=None,
    network_interface_id=None,
    private_ip_addresses=None,
    secondary_private_ip_address_count=None,
    allow_reassignment=False,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """Assign secondary private IP addresses to an ENI. Returns True on success.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.assign_private_ip_addresses

    """
    if not salt.utils.data.exactly_one((network_interface_name, network_interface_id)):
        raise SaltInvocationError(
            "Exactly one of 'network_interface_name', 'network_interface_id' must be provided"
        )
    conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
    if network_interface_name:
        r = get_network_interface_id(
            network_interface_name, region=region, key=key, keyid=keyid, profile=profile
        )
        network_interface_id = r.get("result")
        if not network_interface_id:
            return False
    kwargs = {
        "NetworkInterfaceId": network_interface_id,
        "AllowReassignment": bool(allow_reassignment),
    }
    if private_ip_addresses:
        kwargs["PrivateIpAddresses"] = list(private_ip_addresses)
    if secondary_private_ip_address_count:
        kwargs["SecondaryPrivateIpAddressCount"] = secondary_private_ip_address_count
    try:
        conn.assign_private_ip_addresses(**kwargs)
        return True
    except ClientError as e:
        log.error(e)
        return False


def unassign_private_ip_addresses(
    network_interface_name=None,
    network_interface_id=None,
    private_ip_addresses=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """Unassign secondary private IP addresses from an ENI. Returns True on success.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.unassign_private_ip_addresses

    """
    if not salt.utils.data.exactly_one((network_interface_name, network_interface_id)):
        raise SaltInvocationError(
            "Exactly one of 'network_interface_name', 'network_interface_id' must be provided"
        )
    conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
    if network_interface_name:
        r = get_network_interface_id(
            network_interface_name, region=region, key=key, keyid=keyid, profile=profile
        )
        network_interface_id = r.get("result")
        if not network_interface_id:
            return False
    try:
        conn.unassign_private_ip_addresses(
            NetworkInterfaceId=network_interface_id,
            PrivateIpAddresses=list(private_ip_addresses or []),
        )
        return True
    except ClientError as e:
        log.error(e)
        return False


def get_zones(region=None, key=None, keyid=None, profile=None):
    """Get the list of AZ names for the configured region.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.get_zones

    """
    conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
    resp = conn.describe_availability_zones()
    return [z["ZoneName"] for z in resp.get("AvailabilityZones", [])]


def _describe_instances(conn, instance_ids=None, filters=None):
    kwargs = {}
    if instance_ids:
        kwargs["InstanceIds"] = list(instance_ids)
    if filters:
        kwargs["Filters"] = filters
    instances = []
    for reservation in _paginate(conn.describe_instances, "Reservations", **kwargs):
        instances.extend(reservation.get("Instances", []))
    return instances


def find_instances(
    instance_id=None,
    name=None,
    tags=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
    return_objs=False,
    in_states=None,
    filters=None,
):
    """
    Given instance properties, find and return matching instance ids (default) or
    the raw boto3 instance dicts (when ``return_objs`` is True).

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.find_instances

    """
    conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
    try:
        filter_dict = {}
        instance_ids = None
        if instance_id:
            instance_ids = [instance_id]
        if name:
            filter_dict["tag:Name"] = name
        if tags:
            for tname, tvalue in tags.items():
                filter_dict[f"tag:{tname}"] = tvalue
        if filters:
            filter_dict.update(filters)
        aws_filters = _filters_to_aws(filter_dict) if filter_dict else None
        instances = _describe_instances(conn, instance_ids, aws_filters)
        if in_states:
            instances = [i for i in instances if i.get("State", {}).get("Name") in in_states]
        if not instances:
            return []
        if return_objs:
            return instances
        return [i["InstanceId"] for i in instances]
    except ClientError as e:
        log.error(e)
        return []


def create_image(
    ami_name,
    instance_id=None,
    instance_name=None,
    tags=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
    description=None,
    no_reboot=False,
    dry_run=False,
    filters=None,
):
    """Create an AMI from a single matched instance. Returns AMI id or False.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.create_image

    """
    instances = find_instances(
        instance_id=instance_id,
        name=instance_name,
        tags=tags,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
        return_objs=True,
        filters=filters,
    )
    if not instances:
        log.error("Source instance not found")
        return False
    if len(instances) > 1:
        log.error("Multiple instances matched; refusing to create image.")
        return False
    conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
    try:
        kwargs = {
            "InstanceId": instances[0]["InstanceId"],
            "Name": ami_name,
            "NoReboot": bool(no_reboot),
            "DryRun": bool(dry_run),
        }
        if description:
            kwargs["Description"] = description
        resp = conn.create_image(**kwargs)
        return resp.get("ImageId")
    except ClientError as e:
        log.error(e)
        return False


def find_images(
    ami_name=None,
    executable_by=None,
    owners=None,
    image_ids=None,
    tags=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
    return_objs=False,
):
    """
    Return matching AMI ids, or objects when ``return_objs`` is True.
    Returns False when no images are found.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.find_images

    """
    retries = 30
    conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
    while retries:
        try:
            kwargs = {}
            filter_dict = {}
            if image_ids:
                kwargs["ImageIds"] = (
                    list(image_ids) if isinstance(image_ids, (list, tuple)) else [image_ids]
                )
            if executable_by:
                kwargs["ExecutableUsers"] = (
                    list(executable_by)
                    if isinstance(executable_by, (list, tuple))
                    else [executable_by]
                )
            if owners:
                kwargs["Owners"] = list(owners) if isinstance(owners, (list, tuple)) else [owners]
            if ami_name:
                filter_dict["name"] = ami_name
            if tags:
                for tname, tvalue in tags.items():
                    filter_dict[f"tag:{tname}"] = tvalue
            if filter_dict:
                kwargs["Filters"] = _filters_to_aws(filter_dict)
            images = conn.describe_images(**kwargs).get("Images", [])
            if not images:
                return False
            if return_objs:
                return images
            return [img["ImageId"] for img in images]
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code in ("Throttling", "RequestLimitExceeded"):
                log.debug("Throttled by AWS API, will retry in 5 seconds...")
                time.sleep(5)
                retries -= 1
                continue
            log.error("Failed to look up images: %s", e)
            return False
    return False


def terminate(
    instance_id=None,
    name=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
    filters=None,
):
    """Terminate the instance described by instance_id or Name tag.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.terminate

    """
    instances = find_instances(
        instance_id=instance_id,
        name=name,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
        return_objs=True,
        filters=filters,
    )
    if instances in (False, None, []):
        return instances
    if len(instances) != 1:
        log.warning("Refusing to terminate multiple instances at once")
        return False
    conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.terminate_instances(InstanceIds=[instances[0]["InstanceId"]])
        return True
    except ClientError as e:
        log.error(e)
        return False


def get_id(
    name=None,
    tags=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
    in_states=None,
    filters=None,
):
    """Return a single instance id matching the given properties, or None.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.get_id

    """
    instance_ids = find_instances(
        name=name,
        tags=tags,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
        in_states=in_states,
        filters=filters,
    )
    if not instance_ids:
        log.warning("Could not find instance.")
        return None
    if len(instance_ids) > 1:
        raise CommandExecutionError("Found more than one instance matching the criteria.")
    return instance_ids[0]


def get_tags(instance_id=None, keyid=None, key=None, profile=None, region=None):
    """Return a list of {name: value} tag dicts for an instance.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.get_tags

    """
    conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
    tags = []
    try:
        resp = conn.describe_tags(Filters=_filters_to_aws({"resource-id": instance_id}))
        for tag in resp.get("Tags", []):
            tags.append({tag["Key"]: tag["Value"]})
    except ClientError as e:
        log.error(e)
        return []
    if not tags:
        log.info("No tags found for instance_id %s", instance_id)
    return tags


def exists(
    instance_id=None,
    name=None,
    tags=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
    in_states=None,
    filters=None,
):
    """Return True if any instance matching the given properties exists.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.exists

    """
    instances = find_instances(
        instance_id=instance_id,
        name=name,
        tags=tags,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
        in_states=in_states,
        filters=filters,
    )
    return bool(instances)


def _to_blockdev_map(thing):
    """
    Convert a string, a json payload, or a dict into a list of boto3
    BlockDeviceMapping entries.
    """
    if not thing:
        return None
    if isinstance(thing, str):
        thing = salt.utils.json.loads(thing)
    if isinstance(thing, list):
        return thing
    if not isinstance(thing, dict):
        log.error("Can't convert %r to a BlockDeviceMapping list", thing)
        return None
    out = []
    for device_name, attrs in thing.items():
        entry = {"DeviceName": device_name}
        if attrs.get("ephemeral_name"):
            entry["VirtualName"] = attrs["ephemeral_name"]
        if attrs.get("no_device"):
            entry["NoDevice"] = ""
        ebs = {}
        if attrs.get("volume_id"):
            ebs["VolumeId"] = attrs["volume_id"]
        if attrs.get("snapshot_id"):
            ebs["SnapshotId"] = attrs["snapshot_id"]
        if attrs.get("delete_on_termination") is not None:
            ebs["DeleteOnTermination"] = bool(attrs["delete_on_termination"])
        if attrs.get("size") is not None:
            ebs["VolumeSize"] = attrs["size"]
        if attrs.get("volume_type"):
            ebs["VolumeType"] = attrs["volume_type"]
        if attrs.get("iops") is not None:
            ebs["Iops"] = attrs["iops"]
        if attrs.get("encrypted") is not None:
            ebs["Encrypted"] = bool(attrs["encrypted"])
        if ebs:
            entry["Ebs"] = ebs
        out.append(entry)
    return out


def run(
    image_id,
    name=None,
    tags=None,
    key_name=None,
    security_groups=None,
    user_data=None,
    instance_type="m1.small",
    placement=None,
    kernel_id=None,
    ramdisk_id=None,
    monitoring_enabled=None,
    vpc_id=None,
    vpc_name=None,
    subnet_id=None,
    subnet_name=None,
    private_ip_address=None,
    block_device_map=None,
    disable_api_termination=None,
    instance_initiated_shutdown_behavior=None,
    placement_group=None,
    client_token=None,
    security_group_ids=None,
    security_group_names=None,
    additional_info=None,
    tenancy=None,
    instance_profile_arn=None,
    instance_profile_name=None,
    ebs_optimized=None,
    network_interface_id=None,
    network_interface_name=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
    network_interfaces=None,
):  # pylint: disable=unused-argument
    """
    Create and start an EC2 instance. Returns ``{"instance_id": ...}`` on
    success, False otherwise.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.run

    """
    if all((subnet_id, subnet_name)):
        raise SaltInvocationError("Only one of subnet_name or subnet_id may be provided.")
    if subnet_name:
        r = __salt__["boto3_vpc.get_resource_id"](
            "subnet", subnet_name, region=region, key=key, keyid=keyid, profile=profile
        )
        if "id" not in r:
            log.warning("Couldn't resolve subnet name %s.", subnet_name)
            return False
        subnet_id = r["id"]

    if all((security_group_ids, security_group_names)):
        raise SaltInvocationError(
            "Only one of security_group_ids or security_group_names may be provided."
        )
    if security_group_names:
        security_group_ids = []
        for sgn in security_group_names:
            r = __salt__["boto3_secgroup.get_group_id"](
                sgn, vpc_name=vpc_name, region=region, key=key, keyid=keyid, profile=profile
            )
            if not r:
                log.warning("Couldn't resolve security group name %s", sgn)
                return False
            security_group_ids.append(r)

    nif_sources = sum(
        1 for v in (network_interface_id, network_interface_name, network_interfaces) if v
    )
    if nif_sources > 1:
        raise SaltInvocationError(
            "Only one of network_interface_id, network_interface_name or "
            "network_interfaces may be provided."
        )

    if network_interface_name:
        result = get_network_interface_id(
            network_interface_name, region=region, key=key, keyid=keyid, profile=profile
        )
        network_interface_id = result.get("result")
        if not network_interface_id:
            log.warning(
                "Given network_interface_name '%s' cannot be mapped to a network_interface_id",
                network_interface_name,
            )

    kwargs = {
        "ImageId": image_id,
        "InstanceType": instance_type,
        "MinCount": 1,
        "MaxCount": 1,
    }
    if key_name:
        kwargs["KeyName"] = key_name
    if security_groups:
        kwargs["SecurityGroups"] = list(security_groups)
    if user_data is not None:
        kwargs["UserData"] = user_data
    if placement:
        kwargs.setdefault("Placement", {})["AvailabilityZone"] = placement
    if placement_group:
        kwargs.setdefault("Placement", {})["GroupName"] = placement_group
    if tenancy:
        kwargs.setdefault("Placement", {})["Tenancy"] = tenancy
    if kernel_id:
        kwargs["KernelId"] = kernel_id
    if ramdisk_id:
        kwargs["RamdiskId"] = ramdisk_id
    if monitoring_enabled is not None:
        kwargs["Monitoring"] = {"Enabled": bool(monitoring_enabled)}
    if private_ip_address:
        kwargs["PrivateIpAddress"] = private_ip_address
    bdm = _to_blockdev_map(block_device_map)
    if bdm:
        kwargs["BlockDeviceMappings"] = bdm
    if disable_api_termination is not None:
        kwargs["DisableApiTermination"] = bool(disable_api_termination)
    if instance_initiated_shutdown_behavior:
        kwargs["InstanceInitiatedShutdownBehavior"] = instance_initiated_shutdown_behavior
    if client_token:
        kwargs["ClientToken"] = client_token
    if additional_info:
        kwargs["AdditionalInfo"] = additional_info
    if instance_profile_arn or instance_profile_name:
        profile_spec = {}
        if instance_profile_arn:
            profile_spec["Arn"] = instance_profile_arn
        if instance_profile_name:
            profile_spec["Name"] = instance_profile_name
        kwargs["IamInstanceProfile"] = profile_spec
    if ebs_optimized is not None:
        kwargs["EbsOptimized"] = bool(ebs_optimized)

    if network_interfaces:
        kwargs["NetworkInterfaces"] = list(network_interfaces)
    elif network_interface_id:
        kwargs["NetworkInterfaces"] = [
            {"NetworkInterfaceId": network_interface_id, "DeviceIndex": 0}
        ]
    else:
        iface = {"DeviceIndex": 0}
        if subnet_id:
            iface["SubnetId"] = subnet_id
        if security_group_ids:
            iface["Groups"] = list(security_group_ids)
        if len(iface) > 1:
            kwargs["NetworkInterfaces"] = [iface]

    conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
    try:
        resp = conn.run_instances(**kwargs)
    except ClientError as e:
        log.error(e)
        return False
    instances = resp.get("Instances") or []
    if not instances:
        log.warning("Instance could not be reserved")
        return False
    instance_id = instances[0]["InstanceId"]

    # Poll until running.
    status = "pending"
    while status == "pending":
        time.sleep(5)
        try:
            desc = conn.describe_instances(InstanceIds=[instance_id])
        except ClientError as e:
            log.error(e)
            return False
        insts = [i for r in desc.get("Reservations", []) for i in r.get("Instances", [])]
        if not insts:
            return False
        status = insts[0].get("State", {}).get("Name", "pending")
    if status == "running":
        tag_list = []
        if name:
            tag_list.append({"Key": "Name", "Value": name})
        if tags:
            tag_list.extend({"Key": k, "Value": v} for k, v in tags.items())
        if tag_list:
            try:
                conn.create_tags(Resources=[instance_id], Tags=tag_list)
            except ClientError as e:
                log.error(e)
        return {"instance_id": instance_id}
    log.warning('Instance could not be started -- status is "%s"', status)
    return None


def get_key(key_name, region=None, key=None, keyid=None, profile=None):
    """Return ``(name, fingerprint)`` if the key exists, else False.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.get_key

    """
    conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
    try:
        resp = conn.describe_key_pairs(KeyNames=[key_name])
    except ClientError as e:
        log.debug(e)
        return False
    pairs = resp.get("KeyPairs") or []
    if not pairs:
        return False
    pair = pairs[0]
    return pair["KeyName"], pair["KeyFingerprint"]


def create_key(key_name, save_path, region=None, key=None, keyid=None, profile=None):
    """Create a new key pair, save the private material to ``save_path`` and return it.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.create_key

    """
    conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
    try:
        resp = conn.create_key_pair(KeyName=key_name)
    except ClientError as e:
        log.debug(e)
        return False
    material = resp.get("KeyMaterial", "")
    try:
        # Mirror boto2 key.save(): write to <save_path>/<key_name>.pem

        path = os.path.join(save_path, f"{key_name}.pem")
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(material)
        os.chmod(path, 0o600)
    except OSError as e:
        log.error("Failed to save private key to %s: %s", save_path, e)
        return False
    return material


def import_key(key_name, public_key_material, region=None, key=None, keyid=None, profile=None):
    """Import a key pair by public material. Returns the fingerprint or False.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.import_key

    """
    conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
    try:
        if isinstance(public_key_material, str):
            public_key_material = public_key_material.encode("utf-8")
        resp = conn.import_key_pair(KeyName=key_name, PublicKeyMaterial=public_key_material)
        return resp.get("KeyFingerprint")
    except ClientError as e:
        log.debug(e)
        return False


def delete_key(key_name, region=None, key=None, keyid=None, profile=None):
    """Delete a key pair. Returns True on success.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.delete_key

    """
    conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.delete_key_pair(KeyName=key_name)
        return True
    except ClientError as e:
        log.debug(e)
        return False


def get_keys(keynames=None, filters=None, region=None, key=None, keyid=None, profile=None):
    """Return a list of key pair names matching ``keynames`` and ``filters``.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.get_keys

    """
    conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
    try:
        kwargs = {}
        if keynames:
            kwargs["KeyNames"] = (
                list(keynames) if isinstance(keynames, (list, tuple)) else [keynames]
            )
        if filters:
            kwargs["Filters"] = _filters_to_aws(filters)
        resp = conn.describe_key_pairs(**kwargs)
        return [k["KeyName"] for k in resp.get("KeyPairs", [])]
    except ClientError as e:
        log.debug(e)
        return False


def _resolve_instance_id(instance_name, instance_id, region, key, keyid, profile, filters):
    if not any((instance_name, instance_id)):
        raise SaltInvocationError(
            "At least one of the following must be specified: instance_name or instance_id."
        )
    if instance_name and instance_id:
        raise SaltInvocationError(
            "Both instance_name and instance_id can not be specified in the same command."
        )
    if instance_name:
        instances = find_instances(
            name=instance_name,
            region=region,
            key=key,
            keyid=keyid,
            profile=profile,
            filters=filters,
        )
        if len(instances) > 1:
            raise CommandExecutionError("Found more than one EC2 instance matching the criteria.")
        if not instances:
            return None
        return instances[0]
    return instance_id


def _attribute_param(attribute):
    # AWS API uses snake case variants for the Attribute query param.
    mapping = {
        "instanceType": "instanceType",
        "kernel": "kernel",
        "ramdisk": "ramdisk",
        "userData": "userData",
        "disableApiTermination": "disableApiTermination",
        "instanceInitiatedShutdownBehavior": "instanceInitiatedShutdownBehavior",
        "rootDeviceName": "rootDeviceName",
        "blockDeviceMapping": "blockDeviceMapping",
        "productCodes": "productCodes",
        "sourceDestCheck": "sourceDestCheck",
        "groupSet": "groupSet",
        "ebsOptimized": "ebsOptimized",
        "sriovNetSupport": "sriovNetSupport",
    }
    return mapping[attribute]


def get_attribute(
    attribute,
    instance_name=None,
    instance_id=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
    filters=None,
):
    """Return ``{attribute: value}`` for an EC2 instance, or False.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.get_attribute

    """
    if attribute not in _ATTRIBUTE_LIST:
        raise SaltInvocationError(f"Attribute must be one of: {_ATTRIBUTE_LIST}.")
    instance_id = _resolve_instance_id(
        instance_name, instance_id, region, key, keyid, profile, filters
    )
    if not instance_id:
        return False
    conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
    try:
        resp = conn.describe_instance_attribute(
            InstanceId=instance_id, Attribute=_attribute_param(attribute)
        )
    except ClientError as e:
        log.error(e)
        return False
    # Response keys are CamelCase (e.g. SourceDestCheck, GroupSet, BlockDeviceMappings, ...).
    aws_attr_map = {
        "instanceType": "InstanceType",
        "kernel": "KernelId",
        "ramdisk": "RamdiskId",
        "userData": "UserData",
        "disableApiTermination": "DisableApiTermination",
        "instanceInitiatedShutdownBehavior": "InstanceInitiatedShutdownBehavior",
        "rootDeviceName": "RootDeviceName",
        "blockDeviceMapping": "BlockDeviceMappings",
        "productCodes": "ProductCodes",
        "sourceDestCheck": "SourceDestCheck",
        "groupSet": "Groups",
        "ebsOptimized": "EbsOptimized",
        "sriovNetSupport": "SriovNetSupport",
    }
    aws_key = aws_attr_map[attribute]
    raw = resp.get(aws_key)
    if isinstance(raw, dict) and "Value" in raw:
        value = raw["Value"]
    else:
        value = raw
    if value is None:
        return False
    return {attribute: value}


def set_attribute(
    attribute,
    attribute_value,
    instance_name=None,
    instance_id=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
    filters=None,
):
    """Set an EC2 instance attribute. Returns True on success, False on failure.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.set_attribute

    """
    if attribute not in _ATTRIBUTE_LIST:
        raise SaltInvocationError(f"Attribute must be one of: {_ATTRIBUTE_LIST}.")
    instance_id = _resolve_instance_id(
        instance_name, instance_id, region, key, keyid, profile, filters
    )
    if not instance_id:
        return False
    conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
    kwargs = {"InstanceId": instance_id}
    # modify_instance_attribute uses per-attribute kwargs.
    modify_map = {
        "instanceType": ("InstanceType", {"Value": attribute_value}),
        "kernel": ("Kernel", {"Value": attribute_value}),
        "ramdisk": ("Ramdisk", {"Value": attribute_value}),
        "userData": ("UserData", {"Value": attribute_value}),
        "disableApiTermination": ("DisableApiTermination", {"Value": bool(attribute_value)}),
        "instanceInitiatedShutdownBehavior": (
            "InstanceInitiatedShutdownBehavior",
            {"Value": attribute_value},
        ),
        "sourceDestCheck": ("SourceDestCheck", {"Value": bool(attribute_value)}),
        "groupSet": ("Groups", attribute_value),
        "ebsOptimized": ("EbsOptimized", {"Value": bool(attribute_value)}),
        "sriovNetSupport": ("SriovNetSupport", {"Value": attribute_value}),
        "blockDeviceMapping": ("BlockDeviceMappings", attribute_value),
    }
    if attribute not in modify_map:
        raise SaltInvocationError(f"Attribute {attribute} is not settable via this API.")
    param, value = modify_map[attribute]
    kwargs[param] = value
    try:
        conn.modify_instance_attribute(**kwargs)
        return True
    except ClientError as e:
        log.error(e)
        return False


def _describe_network_interfaces(conn, name=None, network_interface_id=None):
    """Return {"result": eni_dict} or {"error": {...}}."""
    r = {}
    if not (name or network_interface_id):
        raise SaltInvocationError("Either name or network_interface_id must be provided.")
    try:
        if network_interface_id:
            resp = conn.describe_network_interfaces(NetworkInterfaceIds=[network_interface_id])
        else:
            resp = conn.describe_network_interfaces(Filters=_filters_to_aws({"tag:Name": name}))
    except ClientError as e:
        return {"error": boto3mod.get_error(e)}
    enis = resp.get("NetworkInterfaces", [])
    if not enis:
        return {"error": {"message": "No ENIs found."}}
    if len(enis) > 1:
        return {"error": {"message": "Name specified is tagged on multiple ENIs."}}
    r["result"] = enis[0]
    return r


def _describe_network_interface(eni):
    """Translate an ENI describe response into the legacy snake_case dict."""
    r = {}
    # Top-level mapping
    mapping = {
        "Status": "status",
        "Description": "description",
        "AvailabilityZone": "availability_zone",
        "RequesterId": "requesterId",
        "RequesterManaged": "requester_managed",
        "MacAddress": "mac_address",
        "PrivateIpAddress": "private_ip_address",
        "VpcId": "vpc_id",
        "NetworkInterfaceId": "id",
        "SourceDestCheck": "source_dest_check",
        "OwnerId": "owner_id",
        "SubnetId": "subnet_id",
    }
    for aws_key, snake in mapping.items():
        if aws_key in eni:
            r[snake] = eni[aws_key]
    # Tags -> dict-like (list of {Key,Value})
    r["tags"] = {t["Key"]: t["Value"] for t in eni.get("TagSet", []) or eni.get("Tags", [])}
    r["groups"] = [
        {"name": g.get("GroupName"), "id": g.get("GroupId")} for g in eni.get("Groups", [])
    ]
    r["private_ip_addresses"] = [
        {"private_ip_address": a.get("PrivateIpAddress"), "primary": a.get("Primary", False)}
        for a in eni.get("PrivateIpAddresses", [])
    ]
    association = eni.get("Association") or {}
    if association:
        r["associationId"] = association.get("AssociationId")
        r["publicDnsName"] = association.get("PublicDnsName")
        r["ipOwnerId"] = association.get("IpOwnerId")
        r["publicIp"] = association.get("PublicIp")
        r["allocationId"] = association.get("AllocationId")
    r["attachment"] = {}
    attachment = eni.get("Attachment") or {}
    attach_map = {
        "Status": "status",
        "AttachTime": "attach_time",
        "DeviceIndex": "device_index",
        "DeleteOnTermination": "delete_on_termination",
        "InstanceId": "instance_id",
        "InstanceOwnerId": "instance_owner_id",
        "AttachmentId": "id",
    }
    for aws_key, snake in attach_map.items():
        if aws_key in attachment:
            r["attachment"][snake] = attachment[aws_key]
    return r


def get_network_interface_id(name, region=None, key=None, keyid=None, profile=None):
    """Return ``{"result": eni_id}`` or ``{"error": {...}}``.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.get_network_interface_id

    """
    conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
    r = {}
    try:
        resp = conn.describe_network_interfaces(Filters=_filters_to_aws({"tag:Name": name}))
    except ClientError as e:
        return {"error": boto3mod.get_error(e)}
    enis = resp.get("NetworkInterfaces", [])
    if not enis:
        r["error"] = {"message": "No ENIs found."}
    elif len(enis) > 1:
        r["error"] = {"message": "Name specified is tagged on multiple ENIs."}
    else:
        r["result"] = enis[0]["NetworkInterfaceId"]
    return r


def get_network_interface(
    name=None,
    network_interface_id=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """Return ``{"result": {...}}`` or ``{"error": {...}}``.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.get_network_interface

    """
    conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
    r = {}
    result = _describe_network_interfaces(conn, name, network_interface_id)
    if "error" in result:
        if result["error"].get("message") == "No ENIs found.":
            r["result"] = None
            return r
        return result
    r["result"] = _describe_network_interface(result["result"])
    return r


def create_network_interface(
    name,
    subnet_id=None,
    subnet_name=None,
    private_ip_address=None,
    description=None,
    groups=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """Create an ENI tagged with ``Name=<name>``.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.create_network_interface

    """
    if not salt.utils.data.exactly_one((subnet_id, subnet_name)):
        raise SaltInvocationError(
            "One (but not both) of subnet_id or subnet_name must be provided."
        )
    if subnet_name:
        resource = __salt__["boto3_vpc.get_resource_id"](
            "subnet", subnet_name, region=region, key=key, keyid=keyid, profile=profile
        )
        if "id" not in resource:
            log.warning("Couldn't resolve subnet name %s.", subnet_name)
            return False
        subnet_id = resource["id"]

    conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
    r = {}
    existing = _describe_network_interfaces(conn, name)
    if "result" in existing:
        return {"error": {"message": "An ENI with this Name tag already exists."}}
    vpc = __salt__["boto3_vpc.get_subnet_association"](
        [subnet_id], region=region, key=key, keyid=keyid, profile=profile
    )
    vpc_id = vpc.get("vpc_id")
    if not vpc_id:
        return {"error": {"message": f"subnet_id {subnet_id} does not map to a valid vpc id."}}
    group_ids = __salt__["boto3_secgroup.convert_to_group_ids"](
        groups, vpc_id=vpc_id, region=region, key=key, keyid=keyid, profile=profile
    )
    kwargs = {"SubnetId": subnet_id}
    if private_ip_address:
        kwargs["PrivateIpAddress"] = private_ip_address
    if description:
        kwargs["Description"] = description
    if group_ids:
        kwargs["Groups"] = list(group_ids)
    try:
        resp = conn.create_network_interface(**kwargs)
        eni = resp["NetworkInterface"]
        conn.create_tags(
            Resources=[eni["NetworkInterfaceId"]],
            Tags=[{"Key": "Name", "Value": name}],
        )
    except ClientError as e:
        return {"error": boto3mod.get_error(e)}
    # Re-describe so that tags are populated.
    try:
        refreshed = conn.describe_network_interfaces(
            NetworkInterfaceIds=[eni["NetworkInterfaceId"]]
        ).get("NetworkInterfaces", [eni])
        eni = refreshed[0]
    except ClientError:
        pass
    r["result"] = _describe_network_interface(eni)
    return r


def delete_network_interface(
    name=None,
    network_interface_id=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """Delete an ENI.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.delete_network_interface

    """
    if not (name or network_interface_id):
        raise SaltInvocationError("Either name or network_interface_id must be provided.")
    conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
    r = {}
    result = _describe_network_interfaces(conn, name, network_interface_id)
    if "error" in result:
        return result
    eni = result["result"]
    network_interface_id = eni.get("NetworkInterfaceId")
    if not network_interface_id:
        return {"error": {"message": "ID not found for this network interface."}}
    try:
        conn.delete_network_interface(NetworkInterfaceId=network_interface_id)
        r["result"] = True
    except ClientError as e:
        r["error"] = boto3mod.get_error(e)
    return r


def attach_network_interface(
    device_index,
    name=None,
    network_interface_id=None,
    instance_name=None,
    instance_id=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """Attach an ENI to an instance.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.attach_network_interface

    """
    if not salt.utils.data.exactly_one((name, network_interface_id)):
        raise SaltInvocationError(
            "Exactly one (but not both) of 'name' or 'network_interface_id' must be provided."
        )
    if not salt.utils.data.exactly_one((instance_name, instance_id)):
        raise SaltInvocationError(
            "Exactly one (but not both) of 'instance_name' or 'instance_id' must be provided."
        )
    conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
    r = {}
    result = _describe_network_interfaces(conn, name, network_interface_id)
    if "error" in result:
        return result
    eni = result["result"]
    network_interface_id = eni.get("NetworkInterfaceId")
    if not network_interface_id:
        return {"error": {"message": "ID not found for this network interface."}}
    if instance_name:
        try:
            instance_id = get_id(
                name=instance_name, region=region, key=key, keyid=keyid, profile=profile
            )
        except ClientError as e:
            log.error(e)
            return False
    try:
        resp = conn.attach_network_interface(
            NetworkInterfaceId=network_interface_id,
            InstanceId=instance_id,
            DeviceIndex=device_index,
        )
        r["result"] = resp.get("AttachmentId")
    except ClientError as e:
        r["error"] = boto3mod.get_error(e)
    return r


def detach_network_interface(
    name=None,
    network_interface_id=None,
    attachment_id=None,
    force=False,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """Detach an ENI.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.detach_network_interface

    """
    if not (name or network_interface_id or attachment_id):
        raise SaltInvocationError(
            "Either name or network_interface_id or attachment_id must be provided."
        )
    conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
    r = {}
    if not attachment_id:
        result = _describe_network_interfaces(conn, name, network_interface_id)
        if "error" in result:
            return result
        eni = result["result"]
        attachment_id = (eni.get("Attachment") or {}).get("AttachmentId")
        if not attachment_id:
            return {"error": {"message": "Attachment id not found for this ENI."}}
    try:
        conn.detach_network_interface(AttachmentId=attachment_id, Force=bool(force))
        r["result"] = True
    except ClientError as e:
        r["error"] = boto3mod.get_error(e)
    return r


def modify_network_interface_attribute(
    name=None,
    network_interface_id=None,
    attr=None,
    value=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """Modify an ENI attribute: description, source_dest_check, groups, delete_on_termination.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.modify_network_interface_attribute

    """
    if not (name or network_interface_id):
        raise SaltInvocationError("Either name or network_interface_id must be provided.")
    if attr is None and value is None:
        raise SaltInvocationError("attr and value must be provided.")
    conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
    r = {}
    result = _describe_network_interfaces(conn, name, network_interface_id)
    if "error" in result:
        return result
    eni = result["result"]
    network_interface_id = eni["NetworkInterfaceId"]
    info = _describe_network_interface(eni)
    kwargs = {"NetworkInterfaceId": network_interface_id}
    if attr in ("description",):
        kwargs["Description"] = {"Value": value}
    elif attr in ("source_dest_check", "sourceDestCheck"):
        kwargs["SourceDestCheck"] = {"Value": bool(value)}
    elif attr in ("groups", "groupSet"):
        vpc_id = info.get("vpc_id")
        if vpc_id:
            value = __salt__["boto3_secgroup.convert_to_group_ids"](
                value, vpc_id=vpc_id, region=region, key=key, keyid=keyid, profile=profile
            )
            if not value:
                return {
                    "error": {"message": "Security groups do not map to valid security group ids"}
                }
        kwargs["Groups"] = list(value)
    elif attr in ("delete_on_termination", "deleteOnTermination"):
        attachment_id = (eni.get("Attachment") or {}).get("AttachmentId")
        if not attachment_id:
            return {
                "error": {
                    "message": (
                        "No attachment id found for this ENI. The ENI must be attached "
                        "before delete_on_termination can be modified"
                    )
                }
            }
        kwargs["Attachment"] = {
            "AttachmentId": attachment_id,
            "DeleteOnTermination": bool(value),
        }
    else:
        return {"error": {"message": f"Unsupported ENI attribute: {attr}"}}
    try:
        conn.modify_network_interface_attribute(**kwargs)
        r["result"] = True
    except ClientError as e:
        r["error"] = boto3mod.get_error(e)
    return r


def get_all_volumes(
    volume_ids=None,
    filters=None,
    return_objs=False,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """Return a list of volume IDs or describe-volume dicts.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.get_all_volumes

    """
    conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
    try:
        kwargs = {}
        if volume_ids:
            kwargs["VolumeIds"] = (
                list(volume_ids) if isinstance(volume_ids, (list, tuple)) else [volume_ids]
            )
        if filters:
            kwargs["Filters"] = _filters_to_aws(filters)
        vols = list(_paginate(conn.describe_volumes, "Volumes", **kwargs))
        if return_objs:
            return vols
        return [v["VolumeId"] for v in vols]
    except ClientError as e:
        log.error(e)
        return []


def set_volumes_tags(
    tag_maps,
    authoritative=False,
    dry_run=False,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Apply tags to EBS volumes. See legacy ``boto_ec2.set_volumes_tags`` for the schema.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.set_volumes_tags

    """
    ret = {"success": True, "comment": "", "changes": {}}
    running_states = ("pending", "rebooting", "running", "stopping", "stopped")
    tag_sets = {}
    for tm in tag_maps:
        filters = dict(tm.get("filters", {}))
        tags = dict(tm.get("tags", {}))
        args = {
            "return_objs": True,
            "region": region,
            "key": key,
            "keyid": keyid,
            "profile": profile,
        }
        new_filters = {}
        in_states = tm.get("in_states", running_states)
        try:
            for k, v in filters.items():
                if k == "volume_ids":
                    args["volume_ids"] = v
                elif k == "instance_name":
                    instance_id = get_id(
                        name=v,
                        in_states=in_states,
                        region=region,
                        key=key,
                        keyid=keyid,
                        profile=profile,
                    )
                    if not instance_id:
                        raise CommandExecutionError(f"Couldn't resolve instance Name {v} to an ID.")
                    new_filters["attachment.instance_id"] = instance_id
                else:
                    new_filters[k] = v
        except CommandExecutionError as e:
            log.warning(e)
            continue
        args["filters"] = new_filters
        volumes = get_all_volumes(**args)
        for vol in volumes:
            vid = vol["VolumeId"]
            tag_sets.setdefault(vid.replace("-", "_"), {"vol": vol, "tags": tags.copy()})[
                "tags"
            ].update(tags.copy())

    changes = {"old": {}, "new": {}}
    for entry in tag_sets.values():
        vol, tags = entry["vol"], entry["tags"]
        current_tags = {t["Key"]: t["Value"] for t in vol.get("Tags", [])}
        vol_id = vol["VolumeId"]
        curr = set(current_tags)
        req = set(tags)
        add = list(req - curr)
        update = [r for r in (req & curr) if current_tags[r] != tags[r]]
        remove = list(curr - req)
        if add or update or (authoritative and remove):
            changes["old"][vol_id] = current_tags
            changes["new"][vol_id] = tags
        if not dry_run:
            if not create_tags(vol_id, tags, region=region, key=key, keyid=keyid, profile=profile):
                ret["success"] = False
                ret["comment"] = f"Failed to set tags on vol.id {vol_id}: {tags}"
                return ret
            if authoritative and remove:
                if not delete_tags(
                    vol_id, remove, region=region, key=key, keyid=keyid, profile=profile
                ):
                    ret["success"] = False
                    ret["comment"] = f"Failed to remove tags on vol.id {vol_id}: {remove}"
                    return ret
    if changes["old"] or changes["new"]:
        ret["changes"].update(changes)
    return ret


def get_all_tags(filters=None, region=None, key=None, keyid=None, profile=None):
    """Describe all tags matching the filter criteria.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.get_all_tags

    """
    conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
    try:
        kwargs = {}
        if filters:
            kwargs["Filters"] = _filters_to_aws(filters)
        tags = {}
        for tag in _paginate(conn.describe_tags, "Tags", **kwargs):
            tags.setdefault(tag["ResourceId"], {})[tag["Key"]] = tag["Value"]
        return tags
    except ClientError as e:
        log.error(e)
        return {}


def create_tags(resource_ids, tags, region=None, key=None, keyid=None, profile=None):
    """Create metadata tags on the given resources.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.create_tags

    """
    if not isinstance(resource_ids, list):
        resource_ids = [resource_ids]
    conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.create_tags(
            Resources=resource_ids,
            Tags=[{"Key": k, "Value": v} for k, v in tags.items()],
        )
        return True
    except ClientError as e:
        log.error(e)
        return False


def delete_tags(resource_ids, tags, region=None, key=None, keyid=None, profile=None):
    """Delete metadata tags from the given resources.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.delete_tags

    """
    if not isinstance(resource_ids, list):
        resource_ids = [resource_ids]
    conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
    aws_tags = []
    if isinstance(tags, dict):
        for k, v in tags.items():
            entry = {"Key": k}
            if v is not None:
                entry["Value"] = v
            aws_tags.append(entry)
    else:
        aws_tags = [{"Key": k} for k in tags]
    try:
        conn.delete_tags(Resources=resource_ids, Tags=aws_tags)
        return True
    except ClientError as e:
        log.error(e)
        return False


def detach_volume(
    volume_id,
    instance_id=None,
    device=None,
    force=False,
    wait_for_detachement=False,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """Detach an EBS volume. Returns True on success.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.detach_volume

    """
    conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
    kwargs = {"VolumeId": volume_id, "Force": bool(force)}
    if instance_id:
        kwargs["InstanceId"] = instance_id
    if device:
        kwargs["Device"] = device
    try:
        conn.detach_volume(**kwargs)
        if wait_for_detachement and not _wait_for_volume_available(conn, volume_id):
            log.error('Timed out waiting for the volume status "available".')
            return False
        return True
    except ClientError as e:
        log.error(e)
        return False


def delete_volume(
    volume_id,
    instance_id=None,
    device=None,
    force=False,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):  # pylint: disable=unused-argument
    """Delete an EBS volume. Set ``force=True`` to force-detach first.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.delete_volume

    """
    conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.delete_volume(VolumeId=volume_id)
        return True
    except ClientError as e:
        if not force:
            log.error(e)
            return False
    try:
        conn.detach_volume(VolumeId=volume_id, Force=True)
        conn.delete_volume(VolumeId=volume_id)
        return True
    except ClientError as e:
        log.error(e)
        return False


def _wait_for_volume_available(conn, volume_id, retries=5, interval=5):
    for _ in range(retries + 1):
        time.sleep(interval)
        try:
            vols = conn.describe_volumes(VolumeIds=[volume_id]).get("Volumes", [])
        except ClientError:
            return False
        if len(vols) != 1:
            return False
        if vols[0].get("State") == "available":
            return True
    return False


def attach_volume(volume_id, instance_id, device, region=None, key=None, keyid=None, profile=None):
    """Attach an EBS volume. Returns True on success.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.attach_volume

    """
    conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.attach_volume(VolumeId=volume_id, InstanceId=instance_id, Device=device)
        return True
    except ClientError as e:
        log.error(e)
        return False


def create_volume(
    zone_name,
    size=None,
    snapshot_id=None,
    volume_type=None,
    iops=None,
    encrypted=False,
    kms_key_id=None,
    wait_for_creation=False,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """Create an EBS volume. Returns ``{"result": vol_id}`` or ``{"error": ...}``.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.create_volume

    """
    if size is None and snapshot_id is None:
        raise SaltInvocationError("Size must be provided if not created from snapshot.")
    conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
    kwargs = {"AvailabilityZone": zone_name, "Encrypted": bool(encrypted)}
    if size is not None:
        kwargs["Size"] = size
    if snapshot_id:
        kwargs["SnapshotId"] = snapshot_id
    if volume_type:
        kwargs["VolumeType"] = volume_type
    if iops is not None:
        kwargs["Iops"] = iops
    if kms_key_id:
        kwargs["KmsKeyId"] = kms_key_id
    ret = {}
    try:
        resp = conn.create_volume(**kwargs)
        vol_id = resp["VolumeId"]
        if wait_for_creation and not _wait_for_volume_available(conn, vol_id):
            ret["error"] = 'Timed out waiting for the volume status "available".'
        else:
            ret["result"] = vol_id
    except ClientError as e:
        ret["error"] = boto3mod.get_error(e)
    return ret


def describe_instance_metadata_options(
    instance_id, region=None, key=None, keyid=None, profile=None
):
    """
    Return the current Instance Metadata Service (IMDS) options for an instance.

    instance_id
        The ID of the EC2 instance.

    CLI Example:

    .. code-block:: bash

        salt '*' boto3_ec2.describe_instance_metadata_options i-0123456789abcdef0
    """
    conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
    ret = {}
    try:
        resp = conn.describe_instances(InstanceIds=[instance_id])
        reservations = resp.get("Reservations", [])
        if not reservations or not reservations[0].get("Instances"):
            ret["error"] = f"Instance {instance_id} not found."
            return ret
        inst = reservations[0]["Instances"][0]
        ret["result"] = inst.get("MetadataOptions", {})
    except ClientError as e:
        ret["error"] = boto3mod.get_error(e)
    return ret


def modify_instance_metadata_options(
    instance_id,
    http_tokens=None,
    http_put_response_hop_limit=None,
    http_endpoint=None,
    http_protocol_ipv6=None,
    instance_metadata_tags=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Modify the Instance Metadata Service (IMDS) options for an instance.

    instance_id
        The ID of the EC2 instance.
    http_tokens
        ``optional`` or ``required``. ``required`` enforces IMDSv2.
    http_put_response_hop_limit
        Integer 1-64. Desired HTTP PUT response hop limit for metadata requests.
    http_endpoint
        ``enabled`` or ``disabled``.
    http_protocol_ipv6
        ``enabled`` or ``disabled``.
    instance_metadata_tags
        ``enabled`` or ``disabled``.

    Returns ``{"result": <MetadataOptions>}`` on success, or ``{"error": ...}``.

    CLI Example:

    .. code-block:: bash

        salt '*' boto3_ec2.modify_instance_metadata_options i-01234 http_tokens=required
    """
    conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
    kwargs = {"InstanceId": instance_id}
    if http_tokens is not None:
        kwargs["HttpTokens"] = http_tokens
    if http_put_response_hop_limit is not None:
        kwargs["HttpPutResponseHopLimit"] = int(http_put_response_hop_limit)
    if http_endpoint is not None:
        kwargs["HttpEndpoint"] = http_endpoint
    if http_protocol_ipv6 is not None:
        kwargs["HttpProtocolIpv6"] = http_protocol_ipv6
    if instance_metadata_tags is not None:
        kwargs["InstanceMetadataTags"] = instance_metadata_tags
    ret = {}
    try:
        resp = conn.modify_instance_metadata_options(**kwargs)
        ret["result"] = resp.get("InstanceMetadataOptions", {})
    except ClientError as e:
        ret["error"] = boto3mod.get_error(e)
    return ret


def require_imdsv2(instance_id, region=None, key=None, keyid=None, profile=None):
    """
    Convenience wrapper to enforce IMDSv2 on an instance by setting
    ``HttpTokens=required`` and ``HttpEndpoint=enabled``.

    instance_id
        The ID of the EC2 instance.

    CLI Example:

    .. code-block:: bash

        salt '*' boto3_ec2.require_imdsv2 i-0123456789abcdef0
    """
    return modify_instance_metadata_options(
        instance_id,
        http_tokens="required",
        http_endpoint="enabled",
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )
