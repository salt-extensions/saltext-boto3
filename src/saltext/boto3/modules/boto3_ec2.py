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

    addresses (list, optional):
        A list of public IP addresses to filter by.

    allocation_ids (list, optional):
        A list of allocation IDs to filter by.

    region (str, optional):
        The AWS region where the EIPs are located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.get_all_eip_addresses region='us-west-2'
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

    domain (str, optional):
        The domain of the Elastic IP address to filter by. The only permitted value is 'vpc'.

    region (str, optional):
        The AWS region where the EIPs are located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.get_unassociated_eip_address domain='vpc' region='us-west-2'
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

    addresses (list, optional):
        A list of public IP addresses to filter by.

    allocation_ids (list, optional):
        A list of allocation IDs to filter by.

    region (str, optional):
        The AWS region where the EIPs are located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.get_eip_address_info region='us-west-2'
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

    domain (str, optional):
        The domain in which to allocate the Elastic IP address. The only permitted value is 'vpc'.

    region (str, optional):
        The AWS region where the EIP is to be allocated.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.allocate_eip_address region='us-west-2'
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

    public_ip (str, optional):
        The public IP address of the Elastic IP to release.

    allocation_id (str, optional):
        The allocation ID of the Elastic IP to release.

    region (str, optional):
        The AWS region where the EIP is located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.release_eip_address public_ip='1.2.3.4' region='us-west-2'
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

    instance_id (str, optional):
        The ID of the instance to associate the Elastic IP with.

    instance_name (str, optional):
        The name of the instance to associate the Elastic IP with.

    public_ip (str, optional):
        The public IP address of the Elastic IP to associate.

    allocation_id (str, optional):
        The allocation ID of the Elastic IP to associate.

    network_interface_id (str, optional):
        The ID of the network interface to associate the Elastic IP with.

    network_interface_name (str, optional):
        The name of the network interface to associate the Elastic IP with.

    private_ip_address (str, optional):
        The private IP address to associate the Elastic IP with.

    allow_reassociation (bool, optional):
        Whether to allow reassociation of the Elastic IP. Defaults to False.

    region (str, optional):
        The AWS region where the instance or network interface is located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.associate_eip_address instance_id='i-1234567890abcdef0' public_ip='1.2.3.4' region='us-west-2'
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

    public_ip (str, optional):
        The public IP address of the Elastic IP to disassociate.

    association_id (str, optional):
        The association ID of the Elastic IP to disassociate.

    region (str, optional):
        The AWS region where the EIP is located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.disassociate_eip_address public_ip='1.2.3.4' region='us-west-2'
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

    network_interface_name (str, optional):
        The name of the network interface to assign private IP addresses to.

    network_interface_id (str, optional):
        The ID of the network interface to assign private IP addresses to.

    private_ip_addresses (list, optional):
        A list of private IP addresses to assign to the network interface.

    secondary_private_ip_address_count (int, optional):
        The number of secondary private IP addresses to assign to the network interface.

    allow_reassignment (bool, optional):
        Whether to allow reassignment of the private IP addresses. Defaults to False.

    region (str, optional):
        The AWS region where the network interface is located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.assign_private_ip_addresses network_interface_id='eni-12345678' private_ip_addresses='["10.0.0.1"]' region='us-west-2'
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

    network_interface_name (str, optional):
        The name of the network interface to unassign private IP addresses from.

    network_interface_id (str, optional):
        The ID of the network interface to unassign private IP addresses from.

    private_ip_addresses (list, optional):
        A list of private IP addresses to unassign from the network interface.

    region (str, optional):
        The AWS region where the network interface is located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.unassign_private_ip_addresses region='us-west-2'
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

    region (str, optional):
        The AWS region where the availability zones are located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.get_zones region='us-west-2'
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

    instance_id (str, optional):
        The ID of the instance to find.

    name (str, optional):
        The name tag of the instance to find.

    tags (dict, optional):
        A dictionary of tags to filter instances by.

    region (str, optional):
        The AWS region where the instances are located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    return_objs (bool, optional):
        Whether to return the raw boto3 instance dicts instead of instance IDs.

    in_states (list, optional):
        A list of instance states to filter by.

    filters (dict, optional):
        Additional filters to apply when searching for instances.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.find_instances region='us-west-2'
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
    """
    Create an AMI from a single matched instance. Returns AMI id or False.

    ami_name (str):
        The name of the AMI to create.

    instance_id (str, optional):
        The ID of the instance to create the AMI from.

    instance_name (str, optional):
        The name tag of the instance to create the AMI from.

    tags (dict, optional):
        A dictionary of tags to apply to the AMI.

    region (str, optional):
        The AWS region where the instance is located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    description (str, optional):
        A description for the AMI.

    no_reboot (bool, optional):
        Whether to avoid rebooting the instance when creating the AMI. Defaults to False.

    dry_run (bool, optional):
        Whether to perform a dry run of the AMI creation. Defaults to False.

    filters (dict, optional):
        Additional filters to apply when searching for the source instance.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.create_image ami_name='my-ami' instance_id='i-1234567890abcdef0' region='us-west-2'
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

    ami_name (str, optional):
        The name of the AMI to find.

    executable_by (list, optional):
        A list of AWS account IDs or 'self' to filter AMIs by who can execute them.

    owners (list, optional):
        A list of AWS account IDs or 'self' to filter AMIs by their owners.

    image_ids (list, optional):
        A list of AMI IDs to filter by.

    tags (dict, optional):
        A dictionary of tags to filter AMIs by.

    region (str, optional):
        The AWS region where the AMIs are located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    return_objs (bool, optional):
        Whether to return the raw boto3 AMI dicts instead of AMI IDs.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.find_images region='us-west-2'
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

    instance_id (str, optional):
        The ID of the instance to terminate.

    name (str, optional):
        The name tag of the instance to terminate.

    region (str, optional):
        The AWS region where the instance is located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    filters (dict, optional):
        Additional filters to apply when searching for the instance to terminate.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.terminate region='us-west-2'
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

    name (str, optional):
        The name tag of the instance to find.

    tags (dict, optional):
        A dictionary of tags to filter instances by.

    region (str, optional):
        The AWS region where the instance is located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    in_states (list, optional):
        A list of instance states to filter by.

    filters (dict, optional):
        Additional filters to apply when searching for the instance.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.get_id region='us-west-2'
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

    instance_id (str, optional):
        The ID of the instance to retrieve tags for.

    keyid (str, optional):
        The AWS access key ID.

    key (str, optional):
        The AWS secret access key.

    profile (str, optional):
        The profile to use for AWS credentials.

    region (str, optional):
        The AWS region where the instance is located.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.get_tags region='us-west-2'
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

    instance_id (str, optional):
        The ID of the instance to check for existence.

    name (str, optional):
        The name tag of the instance to check for existence.

    tags (dict, optional):
        A dictionary of tags to filter instances by.

    region (str, optional):
        The AWS region where the instance is located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    in_states (list, optional):
        A list of instance states to filter by.

    filters (dict, optional):
        Additional filters to apply when searching for the instance.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.exists region='us-west-2'
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

    instance_id (str, optional):
        The ID of the instance to create or start.

    name (str, optional):
        The name tag of the instance to create or start.

    tags (dict, optional):
        A dictionary of tags to assign to the instance.

    key_name (str, optional):
        The name of the key pair to use for the instance.

    security_groups (list, optional):
        A list of security group names to associate with the instance.

    user_data (str, optional):
        The user data to provide when launching the instance.

    instance_type (str, optional):
        The type of instance to create (e.g., "t2.micro").

    placement (str, optional):
        The placement constraint for the instance (e.g., availability zone).

    kernel_id (str, optional):
        The ID of the kernel to use for the instance.

    ramdisk_id (str, optional):
        The ID of the RAM disk to use for the instance.

    monitoring_enabled (bool, optional):
        Whether detailed monitoring is enabled for the instance.

    vpc_id (str, optional):
        The ID of the VPC in which to launch the instance.

    vpc_name (str, optional):
        The name of the VPC in which to launch the instance.

    subnet_id (str, optional):
        The ID of the subnet in which to launch the instance.

    subnet_name (str, optional):
        The name of the subnet in which to launch the instance.

    private_ip_address (str, optional):
        The private IP address to assign to the instance.

    block_device_map (list, optional):
        A list of block device mappings for the instance.

    disable_api_termination (bool, optional):
        Whether to disable API termination for the instance.

    instance_initiated_shutdown_behavior (str, optional):
        The shutdown behavior for the instance (e.g., "stop" or "terminate").

    placement_group (str, optional):
        The name of the placement group in which to launch the instance.

    client_token (str, optional):
        A unique, case-sensitive token to ensure idempotency of the request.

    security_group_ids (list, optional):
        A list of security group IDs to associate with the instance.

    security_group_names (list, optional):
        A list of security group names to associate with the instance.

    additional_info (str, optional):
        Additional information to provide when launching the instance.

    tenancy (str, optional):
        The tenancy of the instance (e.g., "default" or "dedicated").

    instance_profile_arn (str, optional):
        The ARN of the instance profile to associate with the instance.

    instance_profile_name (str, optional):
        The name of the instance profile to associate with the instance.

    ebs_optimized (bool, optional):
        Whether the instance is optimized for EBS I/O.

    network_interface_id (str, optional):
        The ID of the network interface to associate with the instance.

    network_interface_name (str, optional):
        The name of the network interface to associate with the instance.

    region (str, optional):
        The AWS region where the instance should be created.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    network_interfaces (list, optional):
        A list of network interfaces to associate with the instance.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.run region='us-west-2'
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

    key_name (str):
        The name of the key pair to retrieve.

    region (str, optional):
        The AWS region where the key pair is located.
    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.get_key my-key-name region='us-west-2'
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

    key_name (str):
        The name of the key pair to create.

    save_path (str):
        The directory path where the private key material will be saved.

    region (str, optional):
        The AWS region where the key pair will be created.
    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.create_key my-key-name /path/to/save region='us-west-2'
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

    key_name (str):
        The name of the key pair to import.

    public_key_material (str):
        The public key material to import.

    region (str, optional):
        The AWS region where the key pair will be imported.
    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.import_key my-key-name /path/to/public/key.pub region='us-west-2'
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

    key_name (str):
        The name of the key pair to delete.

    region (str, optional):
        The AWS region where the key pair is located.
    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.delete_key my-key-name region='us-west-2'
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

    keynames (list, optional):
        A list of key pair names to filter the results.

    filters (dict, optional):
        A dictionary of filters to apply when retrieving key pairs.

    region (str, optional):
        The AWS region where the key pairs are located.
    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

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

    attribute (str):
        The attribute of the EC2 instance to retrieve.

    instance_name (str, optional):
        The name of the EC2 instance.

    instance_id (str, optional):
        The ID of the EC2 instance.

    region (str, optional):
        The AWS region where the EC2 instance is located.
    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    filters (dict, optional):
        A dictionary of filters to apply when retrieving the EC2 instance attribute.

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

    attribute (str):
        The attribute of the EC2 instance to set.

    attribute_value (any):
        The value to set for the specified attribute.

    instance_name (str, optional):
        The name of the EC2 instance.

    instance_id (str, optional):
        The ID of the EC2 instance.

    region (str, optional):
        The AWS region where the EC2 instance is located.
    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    filters (dict, optional):
        A dictionary of filters to apply when setting the EC2 instance attribute.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.set_attribute attribute=instanceType attribute_value=t2.micro instance_name=my-instance
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

    name (str):
        The name tag of the network interface to retrieve.

    region (str, optional):
        The AWS region where the network interface is located.
    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.get_network_interface_id name=my-eni
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

    name (str, optional):
        The name tag of the network interface to retrieve.

    network_interface_id (str, optional):
        The ID of the network interface to retrieve.

    region (str, optional):
        The AWS region where the network interface is located.
    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.get_network_interface name=my-eni
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

    name (str):
        The name tag of the network interface to create.

    subnet_id (str, optional):
        The ID of the subnet in which to create the network interface.

    subnet_name (str, optional):
        The name tag of the subnet in which to create the network interface.

    private_ip_address (str, optional):
        The private IP address to assign to the network interface.

    description (str, optional):
        A description for the network interface.

    groups (list, optional):
        A list of security group names or IDs to associate with the network interface.

    region (str, optional):
        The AWS region where the network interface should be created.
    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.create_network_interface name=my-eni subnet_id=subnet-12345678
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

    name (str, optional):
        The name tag of the network interface to delete.

    network_interface_id (str, optional):
        The ID of the network interface to delete.

    region (str, optional):
        The AWS region where the network interface is located.
    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.delete_network_interface name=my-eni
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

    device_index (int):
        The device index for the network interface attachment.

    name (str, optional):
        The name tag of the network interface to attach.

    network_interface_id (str, optional):
        The ID of the network interface to attach.

    instance_name (str, optional):
        The name tag of the instance to attach the network interface to.

    instance_id (str, optional):
        The ID of the instance to attach the network interface to.

    region (str, optional):
        The AWS region where the network interface and instance are located.
    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.attach_network_interface device_index=1 name=my-eni instance_name=my-instance
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

    name (str, optional):
        The name tag of the network interface to detach.

    network_interface_id (str, optional):
        The ID of the network interface to detach.

    attachment_id (str, optional):
        The ID of the network interface attachment to detach.

    force (bool, optional):
        Whether to force the detachment.

    region (str, optional):
        The AWS region where the network interface is located.
    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.detach_network_interface name=my-eni
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

    name (str, optional):
        The name tag of the network interface to modify.

    network_interface_id (str, optional):
        The ID of the network interface to modify.

    attr (str, optional):
        The attribute of the network interface to modify. Valid values are: description, source_dest_check, groups, delete_on_termination.

    value (varies, optional):
        The new value for the specified attribute.

    region (str, optional):
        The AWS region where the network interface is located.
    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.modify_network_interface_attribute name=my-eni attr=description value="New description"
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

    name (str, optional):
        The name tag of the volume to retrieve.

    volume_ids (list, optional):
        A list of volume IDs to retrieve.

    filters (dict, optional):
        A dictionary of filters to apply when retrieving volumes.

    return_objs (bool, optional):
        Whether to return the full volume objects or just the volume IDs.

    region (str, optional):
        The AWS region where the volumes are located.
    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.get_all_volumes name=my-volume
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

    tag_maps (list):
        A list of dictionaries containing the filters and tags to apply to the volumes.
        Each dictionary should have the following structure:

        .. code-block:: python

            {
                "filters": {"volume_ids": ["vol-12345678"], "instance_name": "my-instance"},
                "tags": {"Key1": "Value1", "Key2": "Value2"},
                "in_states": ["running", "stopped"],  # Optional
            }
            # Repeat for each volume you want to tag

    authoritative (bool, optional):
        Whether to replace existing tags with the new tags (True) or merge them (False).

    dry_run (bool, optional):
        If True, checks whether you have the required permissions for the action, without actually making the request.

    region (str, optional):
        The AWS region where the volumes are located.
    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.set_volumes_tags tag_maps='[{"filters": {"volume_ids": ["vol-12345678"]}, "tags": {"Key1": "Value1", "Key2": "Value2"}}]'
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

    filters (dict, optional):
        A dictionary of filters to apply when retrieving tags. The keys should be the filter names and the values should be the filter values.
        For example:

        .. code-block:: python

            {"resource_id": ["vol-12345678"], "key": "Key1"}

    region (str, optional):
        The AWS region where the resources are located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.get_all_tags filters='{"resource_id": ["vol-12345678"], "key": "Key1"}'
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

    resource_ids (list):
        A list of resource IDs to tag.

    tags (dict):
        A dictionary of tags to apply to the resources. The keys are the tag names and the values are the tag values.

    region (str, optional):
        The AWS region where the resources are located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.create_tags resource_ids='["vol-12345678"]' tags='{"Key1": "Value1", "Key2": "Value2"}'
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

    resource_ids (list):
        A list of resource IDs from which to delete tags.

    tags (dict or list):
        A dictionary of tags to delete from the resources. The keys are the tag names and the values are the tag values.
        If a list is provided, it should be a list of tag names to delete.

    region (str, optional):
        The AWS region where the resources are located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.delete_tags resource_ids='["vol-12345678"]' tags='{"Key1": "Value1", "Key2": "Value2"}' region='us-west-2'
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

    volume_id (str):
        The ID of the EBS volume to detach.

    instance_id (str, optional):
        The ID of the instance from which to detach the volume.

    device (str, optional):
        The device name to detach.

    force (bool, optional):
        Whether to force the detachment.

    wait_for_detachement (bool, optional):
        Whether to wait for the volume to be fully detached before returning.

    region (str, optional):
        The AWS region where the volume is located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.detach_volume volume_id='vol-12345678' instance_id='i-12345678' device='/dev/sdh' force=True wait_for_detachement=True
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

    volume_id (str):
        The ID of the EBS volume to delete.

    instance_id (str, optional):
        The ID of the instance from which to detach the volume before deletion.

    device (str, optional):
        The device name to detach before deletion.

    force (bool, optional):
        Whether to force the detachment before deletion.

    region (str, optional):
        The AWS region where the volume is located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.delete_volume volume_id='vol-12345678' instance_id='i-12345678' device='/dev/sdh' force=True
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

    volume_id (str):
        The ID of the EBS volume to attach.

    instance_id (str):
        The ID of the instance to which to attach the volume.

    device (str):
        The device name to attach the volume as.

    region (str, optional):
        The AWS region where the volume and instance are located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.attach_volume volume_id='vol-12345678' instance_id='i-12345678' device='/dev/sdh'
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

    zone_name (str):
        The availability zone in which to create the volume.

    size (int, optional):
        The size of the volume in GiB.

    snapshot_id (str, optional):
        The ID of the snapshot from which to create the volume.

    volume_type (str, optional):
        The type of the volume (e.g., 'gp2', 'io1').

    iops (int, optional):
        The number of IOPS for the volume (required for 'io1' type).

    encrypted (bool, optional):
        Whether the volume should be encrypted.

    kms_key_id (str, optional):
        The ID of the KMS key to use for encryption.

    wait_for_creation (bool, optional):
        Whether to wait for the volume to become available before returning.

    region (str, optional):
        The AWS region where the volume should be created.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ec2.create_volume zone_name='us-west-2a' size=10 volume_type='gp2' encrypted=True
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

    region (str, optional):
        The AWS region where the instance is located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt '*' boto3_ec2.describe_instance_metadata_options i-0123456789abcdef0 region='us-west-2'
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

    instance_id (str):
        The ID of the EC2 instance.

    http_tokens (str, optional):
        ``optional`` or ``required``. ``required`` enforces IMDSv2.

    http_put_response_hop_limit (int, optional):
        Integer 1-64. Desired HTTP PUT response hop limit for metadata requests.

    http_endpoint (str, optional):
        ``enabled`` or ``disabled``.

    http_protocol_ipv6 (str, optional):
        ``enabled`` or ``disabled``.

    instance_metadata_tags (str, optional):
        ``enabled`` or ``disabled``.

    region (str, optional):
        The AWS region where the instance is located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

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

    instance_id (str):
        The ID of the EC2 instance.

    region (str, optional):
        The AWS region where the instance is located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.
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
