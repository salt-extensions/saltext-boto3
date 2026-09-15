"""
Connection module for Amazon EFS using boto3.
=============================================

    Renamed from ``boto_efs`` to ``boto3_efs`` and rewritten to use the
    boto3 ``efs`` client APIs directly via
    :py:mod:`saltext.boto3.utils.boto3mod`.  The legacy boto2 code path
    (object-style access, retry loops) has been removed.

:depends:
  - boto3 >= 1.28.0
  - botocore >= 1.31.0

:configuration: This module accepts explicit EFS credentials but can
    also utilize IAM roles assigned to the instance through Instance Profiles.
    Dynamic credentials are then automatically obtained from AWS API and no
    further configuration is necessary. More Information available at:

    .. code-block:: text

        http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html

    If IAM roles are not used you need to specify them either in the minion's
    config file or as a profile. For example, to specify them in the minion's
    config file:

.. code-block:: yaml

    efs.keyid: GKTADJGHEIQSXMKKRBJ08H
    efs.key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs

A region may also be specified in the configuration:

.. code-block:: yaml

    efs.region: us-east-1

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

from saltext.boto3.utils import boto3mod

try:
    from botocore.exceptions import ClientError

    logging.getLogger("boto3").setLevel(logging.CRITICAL)
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

log = logging.getLogger(__name__)

__virtualname__ = "boto3_efs"


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
    Only load if boto3 is available.
    """
    if HAS_BOTO3:
        return __virtualname__
    return (False, "The boto3_efs module could not be loaded: boto3 is not available.")


def create_file_system(
    name,
    performance_mode="generalPurpose",
    keyid=None,
    key=None,
    profile=None,
    region=None,
    creation_token=None,
    **kwargs,
):  # pylint: disable=unused-argument
    """
    Creates a new, empty file system.

    name (str):
        (string) - The name for the new file system

    performance_mode (str):
        (string) - The PerformanceMode of the file system. Can be either
        generalPurpose or maxIO

    creation_token (str):
        (string) - A unique name to be used as reference when creating an EFS.
        This will ensure idempotency. Set to name if not specified otherwise

    keyid (str, optional):
        The AWS access key ID.

    key (str, optional):
        The AWS secret access key.

    profile (str, optional):
        The profile to use for AWS credentials.

    region (str, optional):
        The AWS region where the EFS file system is to be created.

    creation_token (str, optional):
        The creation token for the EFS file system. If not specified, it will default to the name.

    ``**kwargs`` (optional):
        Additional parameters that are passed to the boto3 client method.

    CLI Example:

    .. code-block:: bash

        salt 'my-minion' boto3_efs.create_file_system efs-name generalPurpose
    """
    if creation_token is None:
        creation_token = name

    tags = [{"Key": "Name", "Value": name}]

    client = _get_conn("efs", region=region, key=key, keyid=keyid, profile=profile)

    try:
        response = client.create_file_system(
            CreationToken=creation_token, PerformanceMode=performance_mode
        )
    except ClientError as exc:
        log.error("Failed to create EFS file system %s.\n%s", name, exc)
        return False

    if "FileSystemId" in response:
        try:
            client.create_tags(FileSystemId=response["FileSystemId"], Tags=tags)
        except ClientError as exc:
            log.error("Failed to tag EFS file system %s.\n%s", response["FileSystemId"], exc)

    if "Name" in response:
        response["Name"] = name

    return response


def create_mount_target(
    filesystemid,
    subnetid,
    ipaddress=None,
    securitygroups=None,
    keyid=None,
    key=None,
    profile=None,
    region=None,
    **kwargs,
):  # pylint: disable=unused-argument
    """
    Creates a mount target for a file system.

    filesystemid (str):
        ID of the file system for which to create the mount target.

    subnetid (str):
        ID of the subnet to add the mount target in.

    ipaddress (str, optional):
        Valid IPv4 address within the address range of the specified subnet.

    securitygroups (list[str], optional):
        Up to five VPC security group IDs.

    keyid (str, optional):
        The AWS access key ID.

    key (str, optional):
        The AWS secret access key.

    profile (str, optional):
        The profile to use for AWS credentials.

    region (str, optional):
        The AWS region where the EFS file system is located.

    ``**kwargs`` (optional):
        Additional parameters that are passed to the boto3 client method.

    CLI Example:

    .. code-block:: bash

        salt 'my-minion' boto3_efs.create_mount_target filesystemid subnetid
    """
    client = _get_conn("efs", region=region, key=key, keyid=keyid, profile=profile)

    params = {"FileSystemId": filesystemid, "SubnetId": subnetid}
    if ipaddress is not None:
        params["IpAddress"] = ipaddress
    if securitygroups is not None:
        params["SecurityGroups"] = securitygroups

    try:
        return client.create_mount_target(**params)
    except ClientError as exc:
        log.error("Failed to create mount target for EFS %s.\n%s", filesystemid, exc)
        return False


def create_tags(
    filesystemid, tags, keyid=None, key=None, profile=None, region=None, **kwargs
):  # pylint: disable=unused-argument
    """
    Creates or overwrites tags associated with a file system.

    filesystemid (str):
        ID of the file system for whose tags will be modified.

    tags (dict):
        The tags to add to the file system

    keyid (str, optional):
        The AWS access key ID.

    key (str, optional):
        The AWS secret access key.

    profile (str, optional):
        The profile to use for AWS credentials.

    region (str, optional):
        The AWS region where the EFS file system is located.

    ``**kwargs`` (optional):
        Additional parameters that are passed to the boto3 client method.

    CLI Example:

    .. code-block:: bash

        salt 'my-minion' boto3_efs.create_tags
    """
    client = _get_conn("efs", region=region, key=key, keyid=keyid, profile=profile)

    new_tags = [{"Key": k, "Value": v} for k, v in tags.items()]

    try:
        client.create_tags(FileSystemId=filesystemid, Tags=new_tags)
    except ClientError as exc:
        log.error("Failed to create tags on EFS %s.\n%s", filesystemid, exc)
        return False
    return True


def delete_file_system(
    filesystemid, keyid=None, key=None, profile=None, region=None, **kwargs
):  # pylint: disable=unused-argument
    """
    Deletes a file system, permanently severing access to its contents.

    filesystemid (str):
        ID of the file system to delete.

    keyid (str, optional):
        The AWS access key ID.

    key (str, optional):
        The AWS secret access key.

    profile (str, optional):
        The profile to use for AWS credentials.

    region (str, optional):
        The AWS region where the EFS file system is located.

    ``**kwargs`` (optional):
        Additional parameters that are passed to the boto3 client method.

    CLI Example:

    .. code-block:: bash

        salt 'my-minion' boto3_efs.delete_file_system filesystemid
    """
    client = _get_conn("efs", region=region, key=key, keyid=keyid, profile=profile)

    try:
        client.delete_file_system(FileSystemId=filesystemid)
    except ClientError as exc:
        log.error("Failed to delete EFS file system %s.\n%s", filesystemid, exc)
        return False
    return True


def delete_mount_target(
    mounttargetid, keyid=None, key=None, profile=None, region=None, **kwargs
):  # pylint: disable=unused-argument
    """
    Deletes the specified mount target.

    mounttargetid (str):
        ID of the mount target to delete.

    keyid (str, optional):
        The AWS access key ID.

    key (str, optional):
        The AWS secret access key.

    profile (str, optional):
        The profile to use for AWS credentials.

    region (str, optional):
        The AWS region where the EFS mount target is located.

    ``**kwargs`` (optional):
        Additional parameters that are passed to the boto3 client method.

    CLI Example:

    .. code-block:: bash

        salt 'my-minion' boto3_efs.delete_mount_target mounttargetid
    """
    client = _get_conn("efs", region=region, key=key, keyid=keyid, profile=profile)

    try:
        client.delete_mount_target(MountTargetId=mounttargetid)
    except ClientError as exc:
        log.error("Failed to delete mount target %s.\n%s", mounttargetid, exc)
        return False
    return True


def delete_tags(
    filesystemid, tags, keyid=None, key=None, profile=None, region=None, **kwargs
):  # pylint: disable=unused-argument
    """
    Deletes the specified tags from a file system.

    filesystemid (str):
        ID of the file system for whose tags will be removed.

    tags (list[str]):
        The tag keys to delete from the file system.

    keyid (str, optional):
        The AWS access key ID.

    key (str, optional):
        The AWS secret access key.

    profile (str, optional):
        The profile to use for AWS credentials.

    region (str, optional):
        The AWS region where the EFS file system is located.

    ``**kwargs`` (optional):
        Additional parameters that are passed to the boto3 client method.

    CLI Example:

    .. code-block:: bash

        salt 'my-minion' boto3_efs.delete_tags
    """
    client = _get_conn("efs", region=region, key=key, keyid=keyid, profile=profile)

    try:
        client.delete_tags(FileSystemId=filesystemid, Tags=tags)
    except ClientError as exc:
        log.error("Failed to delete tags on EFS %s.\n%s", filesystemid, exc)
        return False
    return True


def get_file_systems(
    filesystemid=None,
    keyid=None,
    key=None,
    profile=None,
    region=None,
    creation_token=None,
    **kwargs,
):  # pylint: disable=unused-argument
    """
    Get all EFS properties or a specific instance property
    if filesystemid is specified.

    filesystemid (str, optional):
        ID of the file system to retrieve properties.

    creation_token (str, optional):
        A unique token that identifies an EFS.

    keyid (str, optional):
        The AWS access key ID.

    key (str, optional):
        The AWS secret access key.

    profile (str, optional):
        The profile to use for AWS credentials.

    region (str, optional):
        The AWS region where the EFS file system is located.

    ``**kwargs`` (optional):
        Additional parameters that are passed to the boto3 client method.

    CLI Example:

    .. code-block:: bash

        salt 'my-minion' boto3_efs.get_file_systems efs-id
    """
    client = _get_conn("efs", region=region, key=key, keyid=keyid, profile=profile)

    try:
        if filesystemid and creation_token:
            response = client.describe_file_systems(
                FileSystemId=filesystemid, CreationToken=creation_token
            )
            return response["FileSystems"]
        if filesystemid:
            response = client.describe_file_systems(FileSystemId=filesystemid)
            return response["FileSystems"]
        if creation_token:
            response = client.describe_file_systems(CreationToken=creation_token)
            return response["FileSystems"]

        response = client.describe_file_systems()
        result = list(response["FileSystems"])
        while "NextMarker" in response:
            response = client.describe_file_systems(Marker=response["NextMarker"])
            result.extend(response["FileSystems"])
        return result
    except ClientError as exc:
        log.error("Failed to describe EFS file systems.\n%s", exc)
        return []


def get_mount_targets(
    filesystemid=None,
    mounttargetid=None,
    keyid=None,
    key=None,
    profile=None,
    region=None,
    **kwargs,
):  # pylint: disable=unused-argument
    """
    Get all the EFS mount point properties for a specific filesystemid or
    the properties for a specific mounttargetid.

    filesystemid (str, optional):
        ID of the file system whose mount targets to list.

    mounttargetid (str, optional):
        ID of the mount target to have its properties returned.

    keyid (str, optional):
        The AWS access key ID.

    key (str, optional):
        The AWS secret access key.

    profile (str, optional):
        The profile to use for AWS credentials.

    region (str, optional):
        The AWS region where the EFS file system is located.

    ``**kwargs`` (optional):
        Additional parameters that are passed to the boto3 client method.

    CLI Example:

    .. code-block:: bash

        salt 'my-minion' boto3_efs.get_mount_targets
    """
    client = _get_conn("efs", region=region, key=key, keyid=keyid, profile=profile)

    try:
        if filesystemid:
            response = client.describe_mount_targets(FileSystemId=filesystemid)
            result = list(response["MountTargets"])
            while "NextMarker" in response:
                response = client.describe_mount_targets(
                    FileSystemId=filesystemid, Marker=response["NextMarker"]
                )
                result.extend(response["MountTargets"])
            return result
        if mounttargetid:
            response = client.describe_mount_targets(MountTargetId=mounttargetid)
            return response["MountTargets"]
    except ClientError as exc:
        log.error("Failed to describe mount targets.\n%s", exc)
        return []

    return None


def get_tags(
    filesystemid, keyid=None, key=None, profile=None, region=None, **kwargs
):  # pylint: disable=unused-argument
    """
    Return the tags associated with an EFS instance.

    filesystemid (str):
        ID of the file system whose tags to list.

    keyid (str, optional):
        The AWS access key ID.

    key (str, optional):
        The AWS secret access key.

    profile (str, optional):
        The profile to use for AWS credentials.

    region (str, optional):
        The AWS region where the EFS file system is located.

    ``**kwargs`` (optional):
        Additional parameters that are passed to the boto3 client method.

    CLI Example:

    .. code-block:: bash

        salt 'my-minion' boto3_efs.get_tags efs-id
    """
    client = _get_conn("efs", region=region, key=key, keyid=keyid, profile=profile)
    try:
        response = client.describe_tags(FileSystemId=filesystemid)
        result = list(response["Tags"])

        while "NextMarker" in response:
            response = client.describe_tags(
                FileSystemId=filesystemid, Marker=response["NextMarker"]
            )
            result.extend(response["Tags"])
    except ClientError as exc:
        log.error("Failed to describe tags on EFS %s.\n%s", filesystemid, exc)
        return []

    return result


def set_security_groups(
    mounttargetid,
    securitygroup,
    keyid=None,
    key=None,
    profile=None,
    region=None,
    **kwargs,
):  # pylint: disable=unused-argument
    """
    Modifies the set of security groups in effect for a mount target.

    mounttargetid (str):
        ID of the mount target whose security groups will be modified

    securitygroup (list[str]):
        List of no more than 5 VPC security group IDs.

    keyid (str, optional):
        The AWS access key ID.

    key (str, optional):
        The AWS secret access key.

    profile (str, optional):
        The profile to use for AWS credentials.

    region (str, optional):
        The AWS region where the EFS file system is located.

    ``**kwargs`` (optional):
        Additional parameters that are passed to the boto3 client method.

    CLI Example:

    .. code-block:: bash

        salt 'my-minion' boto3_efs.set_security_groups my-mount-target-id my-sec-group
    """
    client = _get_conn("efs", region=region, key=key, keyid=keyid, profile=profile)
    try:
        client.modify_mount_target_security_groups(
            MountTargetId=mounttargetid, SecurityGroups=securitygroup
        )
    except ClientError as exc:
        log.error(
            "Failed to set security groups on mount target %s.\n%s",
            mounttargetid,
            exc,
        )
        return False
    return True
