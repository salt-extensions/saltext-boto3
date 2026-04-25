"""
Connection module for Amazon CloudWatch using boto3.
====================================================

    Renamed from ``boto_cloudwatch`` to ``boto3_cloudwatch`` and rewritten to use the
    boto3 ``cloudwatch`` client APIs directly via
    :py:mod:`saltext.boto3.utils.boto3mod`.  The legacy boto2 code path
    (object-style access, retry loops) has been removed.

:depends:
  - boto3 >= 1.28.0
  - botocore >= 1.31.0

:configuration: This module accepts explicit CloudWatch credentials but can
    also utilize IAM roles assigned to the instance through Instance Profiles.
    Dynamic credentials are then automatically obtained from AWS API and no
    further configuration is necessary. More Information available at:

    .. code-block:: text

        http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html

    If IAM roles are not used you need to specify them either in the minion's
    config file or as a profile. For example, to specify them in the minion's
    config file:

.. code-block:: yaml

    cloudwatch.keyid: GKTADJGHEIQSXMKKRBJ08H
    cloudwatch.key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs

A region may also be specified in the configuration:

.. code-block:: yaml

    cloudwatch.region: us-east-1

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

import salt.utils.json
import salt.utils.yaml
from salt.utils import odict

from saltext.boto3.utils import boto3mod

try:
    from botocore.exceptions import ClientError

    logging.getLogger("boto3").setLevel(logging.CRITICAL)
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

log = logging.getLogger(__name__)

__virtualname__ = "boto3_cloudwatch"


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
        "The boto3_cloudwatch module could not be loaded: boto3 is not available.",
    )


def _alarm_to_dict(alarm):
    """
    Convert a boto3 MetricAlarm response dict into a normalized dict.
    """
    fields = (
        "AlarmName",
        "MetricName",
        "Namespace",
        "Statistic",
        "ComparisonOperator",
        "Threshold",
        "Period",
        "EvaluationPeriods",
        "Unit",
        "AlarmDescription",
        "Dimensions",
        "AlarmActions",
        "InsufficientDataActions",
        "OKActions",
    )
    d = odict.OrderedDict()
    for f in fields:
        if f in alarm:
            d[f] = alarm[f]
    return d


def get_alarm(Name, region=None, key=None, keyid=None, profile=None):
    """
    Get alarm details. Also can be used to check to see if an alarm exists.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_cloudwatch.get_alarm myalarm region=us-east-1
    """
    try:
        conn = _get_conn("cloudwatch", region=region, key=key, keyid=keyid, profile=profile)
        resp = conn.describe_alarms(AlarmNames=[Name], AlarmTypes=["MetricAlarm"])
        alarms = resp.get("MetricAlarms", [])
        if not alarms:
            return None
        if len(alarms) > 1:
            log.error("multiple alarms matched name '%s'", Name)
        return _alarm_to_dict(alarms[0])
    except ClientError as e:
        return {"error": boto3mod.get_error(e)}


def get_all_alarms(prefix=None, region=None, key=None, keyid=None, profile=None):
    """
    Get all alarm details.  Produces results that can be used to create an sls file.

    If prefix parameter is given, alarm names in the output will be prepended
    with the prefix; alarms that have the prefix will be skipped.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_cloudwatch.get_all_alarms region=us-east-1 --out=txt
    """
    conn = _get_conn("cloudwatch", region=region, key=key, keyid=keyid, profile=profile)
    alarms = []
    paginator = conn.get_paginator("describe_alarms")
    for page in paginator.paginate(AlarmTypes=["MetricAlarm"]):
        alarms.extend(page.get("MetricAlarms", []))
    results = odict.OrderedDict()
    for alarm in alarms:
        alarm = _alarm_to_dict(alarm)
        name = alarm["AlarmName"]
        if prefix:
            if name.startswith(prefix):
                continue
            name = prefix + alarm["AlarmName"]
        del alarm["AlarmName"]
        alarm_sls = [{"name": name}, {"attributes": dict(alarm)}]
        results["manage alarm " + name] = {"boto3_cloudwatch_alarm.present": alarm_sls}
    return salt.utils.yaml.safe_dump(dict(results), default_flow_style=False)


def create_or_update_alarm(
    Name,
    MetricName=None,
    Namespace=None,
    Statistic=None,
    ComparisonOperator=None,
    Threshold=None,
    Period=None,
    EvaluationPeriods=None,
    Unit=None,
    AlarmDescription="",
    Dimensions=None,
    AlarmActions=None,
    InsufficientDataActions=None,
    OKActions=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):  # pylint: disable=unused-argument
    """
    Create or update a cloudwatch alarm.

    Dimensions must be a list of ``{"Name": ..., "Value": ...}`` dicts. If the
    value of Dimensions is a string, it will be json decoded first. Legacy
    ``{Name: [Value]}`` or ``{Name: Value}`` dicts are also accepted and
    converted.

    ``AlarmActions``, ``InsufficientDataActions`` and ``OKActions`` must be
    lists of strings. If passed as a string, they will be split on ``,``.
    Each entry may either be an ARN, or the convenience notation
    ``scaling_policy:<as_name>:<scaling_policy_name>`` referencing an ASG
    scaling policy.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_cloudwatch.create_or_update_alarm Name=myalarm ...
    """
    if Threshold is not None:
        Threshold = float(Threshold)
    if Period is not None:
        Period = int(Period)
    if EvaluationPeriods is not None:
        EvaluationPeriods = int(EvaluationPeriods)
    if isinstance(Dimensions, str):
        Dimensions = salt.utils.json.loads(Dimensions)
    if isinstance(Dimensions, dict):
        converted = []
        for k, v in Dimensions.items():
            if isinstance(v, (list, tuple)):
                for item in v:
                    converted.append({"Name": k, "Value": str(item)})
            else:
                converted.append({"Name": k, "Value": str(v)})
        Dimensions = converted
    if isinstance(AlarmActions, str):
        AlarmActions = AlarmActions.split(",")
    if isinstance(InsufficientDataActions, str):
        InsufficientDataActions = InsufficientDataActions.split(",")
    if isinstance(OKActions, str):
        OKActions = OKActions.split(",")

    if AlarmActions:
        AlarmActions = convert_to_arn(
            AlarmActions, region=region, key=key, keyid=keyid, profile=profile
        )
    if InsufficientDataActions:
        InsufficientDataActions = convert_to_arn(
            InsufficientDataActions, region=region, key=key, keyid=keyid, profile=profile
        )
    if OKActions:
        OKActions = convert_to_arn(OKActions, region=region, key=key, keyid=keyid, profile=profile)

    conn = _get_conn("cloudwatch", region=region, key=key, keyid=keyid, profile=profile)
    kwargs = {"AlarmName": Name}
    for arg in (
        "MetricName",
        "Namespace",
        "Statistic",
        "ComparisonOperator",
        "Threshold",
        "Period",
        "EvaluationPeriods",
        "Unit",
        "AlarmDescription",
        "Dimensions",
        "AlarmActions",
        "InsufficientDataActions",
        "OKActions",
    ):
        val = locals()[arg]
        if val is not None:
            kwargs[arg] = val
    try:
        conn.put_metric_alarm(**kwargs)
        log.info("Created/updated alarm %s", Name)
        return True
    except ClientError as e:
        log.error("Failed to create/update alarm %s: %s", Name, boto3mod.get_error(e))
        return False


def convert_to_arn(arns, region=None, key=None, keyid=None, profile=None):
    """
    Convert a list of strings into actual arns. Converts convenience names such
    as ``scaling_policy:<as_name>:<scaling_policy_name>``.

    CLI Example:

    .. code-block:: bash

        salt '*' boto3_cloudwatch.convert_to_arn 'scaling_policy:my-asg:ScaleDown'
    """
    results = []
    for arn in arns:
        if arn.startswith("scaling_policy:"):
            _, as_group, scaling_policy_name = arn.split(":")
            policy_arn = __salt__["boto3_asg.get_scaling_policy_arn"](
                as_group, scaling_policy_name, region, key, keyid, profile
            )
            if policy_arn:
                results.append(policy_arn)
            else:
                log.error("Could not convert: %s", arn)
        else:
            results.append(arn)
    return results


def delete_alarm(Name, region=None, key=None, keyid=None, profile=None):
    """
    Delete a cloudwatch alarm.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_cloudwatch.delete_alarm myalarm region=us-east-1
    """
    try:
        conn = _get_conn("cloudwatch", region=region, key=key, keyid=keyid, profile=profile)
        conn.delete_alarms(AlarmNames=[Name])
        log.info("Deleted alarm %s", Name)
        return True
    except ClientError as e:
        log.error("Failed to delete alarm %s: %s", Name, boto3mod.get_error(e))
        return False
