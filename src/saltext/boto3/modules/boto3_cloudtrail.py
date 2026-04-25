"""
Connection module for Amazon CloudTrail using boto3.
====================================================

    Renamed from ``boto_cloudtrail`` to ``boto3_cloudtrail`` and rewritten to use the
    boto3 ``cloudtrail`` client APIs directly via
    :py:mod:`saltext.boto3.utils.boto3mod`.  The legacy boto2 code path
    (object-style access, retry loops) has been removed.

:depends:
  - boto3 >= 1.28.0
  - botocore >= 1.31.0

:configuration: This module accepts explicit CloudTrail credentials but can
    also utilize IAM roles assigned to the instance through Instance Profiles.
    Dynamic credentials are then automatically obtained from AWS API and no
    further configuration is necessary. More Information available at:

    .. code-block:: text

        http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html

    If IAM roles are not used you need to specify them either in the minion's
    config file or as a profile. For example, to specify them in the minion's
    config file:

.. code-block:: yaml

    cloudtrail.keyid: GKTADJGHEIQSXMKKRBJ08H
    cloudtrail.key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs

A region may also be specified in the configuration:

.. code-block:: yaml

    cloudtrail.region: us-east-1

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

__virtualname__ = "boto3_cloudtrail"


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
    return (
        False,
        "The boto3_cloudtrail module could not be loaded: boto3 is not available.",
    )


def exists(Name, region=None, key=None, keyid=None, profile=None):
    """
    Given a trail name, check whether the given trail exists.

    Returns ``{"exists": True}`` or ``{"exists": False}``.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_cloudtrail.exists mytrail
    """
    try:
        conn = _get_conn("cloudtrail", region=region, key=key, keyid=keyid, profile=profile)
        conn.get_trail_status(Name=Name)
        return {"exists": True}
    except ClientError as e:
        err = boto3mod.get_error(e)
        if e.response.get("Error", {}).get("Code") == "TrailNotFoundException":
            return {"exists": False}
        return {"error": err}


def create(
    Name,
    S3BucketName,
    S3KeyPrefix=None,
    SnsTopicName=None,
    IncludeGlobalServiceEvents=None,
    IsMultiRegionTrail=None,
    EnableLogFileValidation=None,
    CloudWatchLogsLogGroupArn=None,
    CloudWatchLogsRoleArn=None,
    KmsKeyId=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Given a valid config, create a trail.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_cloudtrail.create my_trail my_bucket
    """
    try:
        conn = _get_conn("cloudtrail", region=region, key=key, keyid=keyid, profile=profile)
        kwargs = {}
        optional = {
            "S3KeyPrefix": S3KeyPrefix,
            "SnsTopicName": SnsTopicName,
            "IncludeGlobalServiceEvents": IncludeGlobalServiceEvents,
            "IsMultiRegionTrail": IsMultiRegionTrail,
            "EnableLogFileValidation": EnableLogFileValidation,
            "CloudWatchLogsLogGroupArn": CloudWatchLogsLogGroupArn,
            "CloudWatchLogsRoleArn": CloudWatchLogsRoleArn,
            "KmsKeyId": KmsKeyId,
        }
        for arg, value in optional.items():
            if value is not None:
                kwargs[arg] = value
        trail = conn.create_trail(Name=Name, S3BucketName=S3BucketName, **kwargs)
        if trail:
            log.info("The newly created trail name is %s", trail["Name"])
            return {"created": True, "name": trail["Name"]}
        log.warning("Trail was not created")
        return {"created": False}
    except ClientError as e:
        return {"created": False, "error": boto3mod.get_error(e)}


def delete(Name, region=None, key=None, keyid=None, profile=None):
    """
    Given a trail name, delete it.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_cloudtrail.delete mytrail
    """
    try:
        conn = _get_conn("cloudtrail", region=region, key=key, keyid=keyid, profile=profile)
        conn.delete_trail(Name=Name)
        return {"deleted": True}
    except ClientError as e:
        return {"deleted": False, "error": boto3mod.get_error(e)}


def describe(Name, region=None, key=None, keyid=None, profile=None):
    """
    Given a trail name describe its properties.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_cloudtrail.describe mytrail
    """
    try:
        conn = _get_conn("cloudtrail", region=region, key=key, keyid=keyid, profile=profile)
        trails = conn.describe_trails(trailNameList=[Name])
        if trails and trails.get("trailList"):
            keys = (
                "Name",
                "S3BucketName",
                "S3KeyPrefix",
                "SnsTopicName",
                "IncludeGlobalServiceEvents",
                "IsMultiRegionTrail",
                "HomeRegion",
                "TrailARN",
                "LogFileValidationEnabled",
                "CloudWatchLogsLogGroupArn",
                "CloudWatchLogsRoleArn",
                "KmsKeyId",
            )
            trail = trails["trailList"].pop()
            return {"trail": {k: trail.get(k) for k in keys}}
        return {"trail": None}
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "TrailNotFoundException":
            return {"trail": None}
        return {"error": boto3mod.get_error(e)}


def status(Name, region=None, key=None, keyid=None, profile=None):
    """
    Given a trail name return its status.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_cloudtrail.status mytrail
    """
    try:
        conn = _get_conn("cloudtrail", region=region, key=key, keyid=keyid, profile=profile)
        trail = conn.get_trail_status(Name=Name)
        if trail:
            keys = (
                "IsLogging",
                "LatestDeliveryError",
                "LatestNotificationError",
                "LatestDeliveryTime",
                "LatestNotificationTime",
                "StartLoggingTime",
                "StopLoggingTime",
                "LatestCloudWatchLogsDeliveryError",
                "LatestCloudWatchLogsDeliveryTime",
                "LatestDigestDeliveryTime",
                "LatestDigestDeliveryError",
                "LatestDeliveryAttemptTime",
                "LatestNotificationAttemptTime",
                "LatestNotificationAttemptSucceeded",
                "LatestDeliveryAttemptSucceeded",
                "TimeLoggingStarted",
                "TimeLoggingStopped",
            )
            return {"trail": {k: trail.get(k) for k in keys}}
        return {"trail": None}
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "TrailNotFoundException":
            return {"trail": None}
        return {"error": boto3mod.get_error(e)}


def list_trails(region=None, key=None, keyid=None, profile=None):
    """
    List all trails.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_cloudtrail.list_trails
    """
    try:
        conn = _get_conn("cloudtrail", region=region, key=key, keyid=keyid, profile=profile)
        trails = conn.describe_trails()
        if not bool(trails.get("trailList")):
            log.warning("No trails found")
        return {"trails": trails.get("trailList", [])}
    except ClientError as e:
        return {"error": boto3mod.get_error(e)}


def update(
    Name,
    S3BucketName,
    S3KeyPrefix=None,
    SnsTopicName=None,
    IncludeGlobalServiceEvents=None,
    IsMultiRegionTrail=None,
    EnableLogFileValidation=None,
    CloudWatchLogsLogGroupArn=None,
    CloudWatchLogsRoleArn=None,
    KmsKeyId=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Given a valid config, update a trail.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_cloudtrail.update my_trail my_bucket
    """
    try:
        conn = _get_conn("cloudtrail", region=region, key=key, keyid=keyid, profile=profile)
        kwargs = {}
        optional = {
            "S3KeyPrefix": S3KeyPrefix,
            "SnsTopicName": SnsTopicName,
            "IncludeGlobalServiceEvents": IncludeGlobalServiceEvents,
            "IsMultiRegionTrail": IsMultiRegionTrail,
            "EnableLogFileValidation": EnableLogFileValidation,
            "CloudWatchLogsLogGroupArn": CloudWatchLogsLogGroupArn,
            "CloudWatchLogsRoleArn": CloudWatchLogsRoleArn,
            "KmsKeyId": KmsKeyId,
        }
        for arg, value in optional.items():
            if value is not None:
                kwargs[arg] = value
        trail = conn.update_trail(Name=Name, S3BucketName=S3BucketName, **kwargs)
        if trail:
            log.info("The updated trail name is %s", trail["Name"])
            return {"updated": True, "name": trail["Name"]}
        log.warning("Trail was not updated")
        return {"updated": False}
    except ClientError as e:
        return {"updated": False, "error": boto3mod.get_error(e)}


def start_logging(Name, region=None, key=None, keyid=None, profile=None):
    """
    Start logging for a trail.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_cloudtrail.start_logging my_trail
    """
    try:
        conn = _get_conn("cloudtrail", region=region, key=key, keyid=keyid, profile=profile)
        conn.start_logging(Name=Name)
        return {"started": True}
    except ClientError as e:
        return {"started": False, "error": boto3mod.get_error(e)}


def stop_logging(Name, region=None, key=None, keyid=None, profile=None):
    """
    Stop logging for a trail.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_cloudtrail.stop_logging my_trail
    """
    try:
        conn = _get_conn("cloudtrail", region=region, key=key, keyid=keyid, profile=profile)
        conn.stop_logging(Name=Name)
        return {"stopped": True}
    except ClientError as e:
        return {"stopped": False, "error": boto3mod.get_error(e)}


def _get_trail_arn(name, region=None, key=None, keyid=None, profile=None):
    if name.startswith("arn:aws:cloudtrail:"):
        return name

    sts = _get_conn("sts", region=region, key=key, keyid=keyid, profile=profile)
    account_id = sts.get_caller_identity()["Account"]
    if profile and "region" in profile:
        region = profile["region"]
    if region is None:
        region = "us-east-1"
    return f"arn:aws:cloudtrail:{region}:{account_id}:trail/{name}"


def add_tags(Name, region=None, key=None, keyid=None, profile=None, **kwargs):
    """
    Add tags to a trail.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_cloudtrail.add_tags my_trail tag_a=tag_value tag_b=tag_value
    """
    try:
        conn = _get_conn("cloudtrail", region=region, key=key, keyid=keyid, profile=profile)
        tagslist = []
        for k, v in kwargs.items():
            if str(k).startswith("__"):
                continue
            tagslist.append({"Key": str(k), "Value": str(v)})
        conn.add_tags(
            ResourceId=_get_trail_arn(Name, region=region, key=key, keyid=keyid, profile=profile),
            TagsList=tagslist,
        )
        return {"tagged": True}
    except ClientError as e:
        return {"tagged": False, "error": boto3mod.get_error(e)}


def remove_tags(Name, region=None, key=None, keyid=None, profile=None, **kwargs):
    """
    Remove tags from a trail.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_cloudtrail.remove_tags my_trail tag_a=tag_value tag_b=tag_value
    """
    try:
        conn = _get_conn("cloudtrail", region=region, key=key, keyid=keyid, profile=profile)
        tagslist = []
        for k, v in kwargs.items():
            if str(k).startswith("__"):
                continue
            tagslist.append({"Key": str(k), "Value": str(v)})
        conn.remove_tags(
            ResourceId=_get_trail_arn(Name, region=region, key=key, keyid=keyid, profile=profile),
            TagsList=tagslist,
        )
        return {"tagged": True}
    except ClientError as e:
        return {"tagged": False, "error": boto3mod.get_error(e)}


def list_tags(Name, region=None, key=None, keyid=None, profile=None):
    """
    List tags of a trail.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_cloudtrail.list_tags my_trail
    """
    try:
        conn = _get_conn("cloudtrail", region=region, key=key, keyid=keyid, profile=profile)
        rid = _get_trail_arn(Name, region=region, key=key, keyid=keyid, profile=profile)
        ret = conn.list_tags(ResourceIdList=[rid])
        tlist = ret.get("ResourceTagList", []).pop().get("TagsList")
        tagdict = {}
        for tag in tlist:
            tagdict[tag.get("Key")] = tag.get("Value")
        return {"tags": tagdict}
    except ClientError as e:
        return {"error": boto3mod.get_error(e)}
