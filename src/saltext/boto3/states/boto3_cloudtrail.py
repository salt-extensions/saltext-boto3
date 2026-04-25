"""
Manage CloudTrail Objects using boto3.
======================================

    Renamed from ``boto_cloudtrail`` to ``boto3_cloudtrail`` and updated to call the
    refactored ``boto3_cloudtrail`` execution module.

Create and destroy CloudTrail objects.

:depends:
  - boto3 >= 1.28.0
  - botocore >= 1.31.0

This module uses boto3, which can be installed via package, or pip.

This module accepts explicit CloudTrail credentials but can also utilize
IAM roles assigned to the instance through Instance Profiles. Dynamic
credentials are then automatically obtained from AWS API and no further
configuration is necessary. More Information available at:

.. code-block:: text

    http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html

If IAM roles are not used you need to specify them either in the minion's config file
or as a profile. For example, to specify them in the minion's config file:

.. code-block:: yaml

    cloudtrail.keyid: GKTADJGHEIQSXMKKRBJ08H
    cloudtrail.key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs

It's also possible to specify key, keyid and region via a profile, either
as a passed in dict, or as a string to pull from pillars or minion config:

.. code-block:: yaml

    myprofile:
        keyid: GKTADJGHEIQSXMKKRBJ08H
        key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs
        region: us-east-1

.. code-block:: yaml

    Manage my_trail CloudTrail trail:
        boto3_cloudtrail.present:
          - name: my_trail
          - S3BucketName: my_bucket
          - S3KeyPrefix: my_prefix
          - SnsTopicName: my_sns_topic
          - IncludeGlobalServiceEvents: True
          - IsMultiRegionTrail: False
          - EnableLogFileValidation: False
          - CloudWatchLogsLogGroupArn: my_log_group_arn
          - CloudWatchLogsRoleArn: my_log_role_arn
          - KmsKeyId: my_kms_key_id
          - LoggingEnabled: True
          - tags:
              testing_key: testing_value

.. versionadded:: 1.0.0
"""

import logging
import os

import salt.utils.data

log = logging.getLogger(__name__)

__virtualname__ = "boto3_cloudtrail"


def __virtual__():
    """
    Only load if the boto3_cloudtrail execution module is available.
    """
    if "boto3_cloudtrail.exists" in __salt__:
        return __virtualname__
    return (
        False,
        "The boto3_cloudtrail state module could not be loaded: "
        "boto3_cloudtrail exec module unavailable.",
    )


def present(
    name,
    Name,
    S3BucketName,
    S3KeyPrefix=None,
    SnsTopicName=None,
    IncludeGlobalServiceEvents=True,
    IsMultiRegionTrail=None,
    EnableLogFileValidation=False,
    CloudWatchLogsLogGroupArn=None,
    CloudWatchLogsRoleArn=None,
    KmsKeyId=None,
    LoggingEnabled=True,
    Tags=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):  # pylint: disable=unused-argument
    """
    Ensure trail exists.

    name
        The name of the state definition.

    Name
        Name of the trail.

    S3BucketName
        Specifies the name of the Amazon S3 bucket designated for publishing log files.

    S3KeyPrefix
        Specifies the Amazon S3 key prefix that comes after the name of the bucket.

    SnsTopicName
        Specifies the name of the Amazon SNS topic defined for notification of log file delivery.

    IncludeGlobalServiceEvents
        Specifies whether the trail is publishing events from global services.

    EnableLogFileValidation
        Specifies whether log file integrity validation is enabled.

    CloudWatchLogsLogGroupArn
        Specifies a log group ARN to which CloudTrail logs will be delivered.

    CloudWatchLogsRoleArn
        Specifies the role for the CloudWatch Logs endpoint to assume.

    KmsKeyId
        Specifies the KMS key ID to use to encrypt the logs delivered by CloudTrail.

    LoggingEnabled
        Whether logging should be enabled for the trail.

    Tags
        A dictionary of tags that should be set on the trail.

    region
        Region to connect to.

    key
        Secret key to be used.

    keyid
        Access key to be used.

    profile
        A dict with region, key and keyid, or a pillar key (string) that
        contains a dict with region, key and keyid.

    Example:

    .. code-block:: yaml

        ensure-present:
          boto3_cloudtrail.present:
            - name: example

    """
    ret = {"name": Name, "result": True, "comment": "", "changes": {}}

    r = __salt__["boto3_cloudtrail.exists"](
        Name=Name, region=region, key=key, keyid=keyid, profile=profile
    )

    if "error" in r:
        ret["result"] = False
        ret["comment"] = "Failed to create trail: {}.".format(r["error"]["message"])
        return ret

    if not r.get("exists"):
        if __opts__["test"]:
            ret["comment"] = f"CloudTrail {Name} is set to be created."
            ret["result"] = None
            return ret
        r = __salt__["boto3_cloudtrail.create"](
            Name=Name,
            S3BucketName=S3BucketName,
            S3KeyPrefix=S3KeyPrefix,
            SnsTopicName=SnsTopicName,
            IncludeGlobalServiceEvents=IncludeGlobalServiceEvents,
            IsMultiRegionTrail=IsMultiRegionTrail,
            EnableLogFileValidation=EnableLogFileValidation,
            CloudWatchLogsLogGroupArn=CloudWatchLogsLogGroupArn,
            CloudWatchLogsRoleArn=CloudWatchLogsRoleArn,
            KmsKeyId=KmsKeyId,
            region=region,
            key=key,
            keyid=keyid,
            profile=profile,
        )
        if not r.get("created"):
            ret["result"] = False
            ret["comment"] = "Failed to create trail: {}.".format(r["error"]["message"])
            return ret
        _describe = __salt__["boto3_cloudtrail.describe"](
            Name, region=region, key=key, keyid=keyid, profile=profile
        )
        ret["changes"]["old"] = {"trail": None}
        ret["changes"]["new"] = _describe
        ret["comment"] = f"CloudTrail {Name} created."

        if LoggingEnabled:
            r = __salt__["boto3_cloudtrail.start_logging"](
                Name=Name, region=region, key=key, keyid=keyid, profile=profile
            )
            if "error" in r:
                ret["result"] = False
                ret["comment"] = "Failed to create trail: {}.".format(r["error"]["message"])
                ret["changes"] = {}
                return ret
            ret["changes"]["new"]["trail"]["LoggingEnabled"] = True
        else:
            ret["changes"]["new"]["trail"]["LoggingEnabled"] = False

        if bool(Tags):
            r = __salt__["boto3_cloudtrail.add_tags"](
                Name=Name,
                region=region,
                key=key,
                keyid=keyid,
                profile=profile,
                **Tags,
            )
            if not r.get("tagged"):
                ret["result"] = False
                ret["comment"] = "Failed to create trail: {}.".format(r["error"]["message"])
                ret["changes"] = {}
                return ret
            ret["changes"]["new"]["trail"]["Tags"] = Tags
        return ret

    ret["comment"] = os.linesep.join([ret["comment"], f"CloudTrail {Name} is present."])
    ret["changes"] = {}
    _describe = __salt__["boto3_cloudtrail.describe"](
        Name=Name, region=region, key=key, keyid=keyid, profile=profile
    )
    if "error" in _describe:
        ret["result"] = False
        ret["comment"] = "Failed to update trail: {}.".format(_describe["error"]["message"])
        ret["changes"] = {}
        return ret
    _describe = _describe.get("trail")

    r = __salt__["boto3_cloudtrail.status"](
        Name=Name, region=region, key=key, keyid=keyid, profile=profile
    )
    _describe["LoggingEnabled"] = r.get("trail", {}).get("IsLogging", False)

    need_update = False
    bucket_vars = {
        "S3BucketName": "S3BucketName",
        "S3KeyPrefix": "S3KeyPrefix",
        "SnsTopicName": "SnsTopicName",
        "IncludeGlobalServiceEvents": "IncludeGlobalServiceEvents",
        "IsMultiRegionTrail": "IsMultiRegionTrail",
        "EnableLogFileValidation": "LogFileValidationEnabled",
        "CloudWatchLogsLogGroupArn": "CloudWatchLogsLogGroupArn",
        "CloudWatchLogsRoleArn": "CloudWatchLogsRoleArn",
        "KmsKeyId": "KmsKeyId",
        "LoggingEnabled": "LoggingEnabled",
    }

    for invar, outvar in bucket_vars.items():
        if _describe[outvar] != locals()[invar]:
            need_update = True
            ret["changes"].setdefault("new", {})[invar] = locals()[invar]
            ret["changes"].setdefault("old", {})[invar] = _describe[outvar]

    r = __salt__["boto3_cloudtrail.list_tags"](
        Name=Name, region=region, key=key, keyid=keyid, profile=profile
    )
    _describe["Tags"] = r.get("tags", {})
    tagchange = salt.utils.data.compare_dicts(_describe["Tags"], Tags)
    if bool(tagchange):
        need_update = True
        ret["changes"].setdefault("new", {})["Tags"] = Tags
        ret["changes"].setdefault("old", {})["Tags"] = _describe["Tags"]

    if need_update:
        if __opts__["test"]:
            ret["comment"] = f"CloudTrail {Name} set to be modified."
            ret["result"] = None
            return ret

        ret["comment"] = os.linesep.join([ret["comment"], "CloudTrail to be modified"])
        r = __salt__["boto3_cloudtrail.update"](
            Name=Name,
            S3BucketName=S3BucketName,
            S3KeyPrefix=S3KeyPrefix,
            SnsTopicName=SnsTopicName,
            IncludeGlobalServiceEvents=IncludeGlobalServiceEvents,
            IsMultiRegionTrail=IsMultiRegionTrail,
            EnableLogFileValidation=EnableLogFileValidation,
            CloudWatchLogsLogGroupArn=CloudWatchLogsLogGroupArn,
            CloudWatchLogsRoleArn=CloudWatchLogsRoleArn,
            KmsKeyId=KmsKeyId,
            region=region,
            key=key,
            keyid=keyid,
            profile=profile,
        )
        if not r.get("updated"):
            ret["result"] = False
            ret["comment"] = "Failed to update trail: {}.".format(r["error"]["message"])
            ret["changes"] = {}
            return ret

        if LoggingEnabled:
            r = __salt__["boto3_cloudtrail.start_logging"](
                Name=Name, region=region, key=key, keyid=keyid, profile=profile
            )
            if not r.get("started"):
                ret["result"] = False
                ret["comment"] = "Failed to update trail: {}.".format(r["error"]["message"])
                ret["changes"] = {}
                return ret
        else:
            r = __salt__["boto3_cloudtrail.stop_logging"](
                Name=Name, region=region, key=key, keyid=keyid, profile=profile
            )
            if not r.get("stopped"):
                ret["result"] = False
                ret["comment"] = "Failed to update trail: {}.".format(r["error"]["message"])
                ret["changes"] = {}
                return ret

        if bool(tagchange):
            adds = {}
            removes = {}
            for k, diff in tagchange.items():
                if diff.get("new", "") != "":
                    adds[k] = Tags[k]
                elif diff.get("old", "") != "":
                    removes[k] = _describe["Tags"][k]
            if bool(adds):
                __salt__["boto3_cloudtrail.add_tags"](
                    Name=Name,
                    region=region,
                    key=key,
                    keyid=keyid,
                    profile=profile,
                    **adds,
                )
            if bool(removes):
                __salt__["boto3_cloudtrail.remove_tags"](
                    Name=Name,
                    region=region,
                    key=key,
                    keyid=keyid,
                    profile=profile,
                    **removes,
                )

    return ret


def absent(
    name, Name, region=None, key=None, keyid=None, profile=None
):  # pylint: disable=unused-argument
    """
    Ensure trail with passed properties is absent.

    name
        The name of the state definition.

    Name
        Name of the trail.

    region
        Region to connect to.

    key
        Secret key to be used.

    keyid
        Access key to be used.

    profile
        A dict with region, key and keyid, or a pillar key (string) that
        contains a dict with region, key and keyid.

    Example:

    .. code-block:: yaml

        ensure-absent:
          boto3_cloudtrail.absent:
            - name: example

    """
    ret = {"name": Name, "result": True, "comment": "", "changes": {}}

    r = __salt__["boto3_cloudtrail.exists"](
        Name, region=region, key=key, keyid=keyid, profile=profile
    )
    if "error" in r:
        ret["result"] = False
        ret["comment"] = "Failed to delete trail: {}.".format(r["error"]["message"])
        return ret

    if r and not r["exists"]:
        ret["comment"] = f"CloudTrail {Name} does not exist."
        return ret

    if __opts__["test"]:
        ret["comment"] = f"CloudTrail {Name} is set to be removed."
        ret["result"] = None
        return ret
    r = __salt__["boto3_cloudtrail.delete"](
        Name, region=region, key=key, keyid=keyid, profile=profile
    )
    if not r["deleted"]:
        ret["result"] = False
        ret["comment"] = "Failed to delete trail: {}.".format(r["error"]["message"])
        return ret
    ret["changes"]["old"] = {"trail": Name}
    ret["changes"]["new"] = {"trail": None}
    ret["comment"] = f"CloudTrail {Name} deleted."
    return ret
