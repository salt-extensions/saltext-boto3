"""
Connection module for Amazon CloudWatch Events using boto3.
===========================================================

    Renamed from ``boto_cloudwatch_event`` to ``boto3_cloudwatch_event`` and rewritten to use the
    boto3 ``events`` client APIs directly via
    :py:mod:`saltext.boto3.utils.boto3mod`.  The legacy boto2 code path
    (object-style access, retry loops) has been removed.

:depends:
  - boto3 >= 1.28.0
  - botocore >= 1.31.0

:configuration: This module accepts explicit CloudWatch Events credentials but can
    also utilize IAM roles assigned to the instance through Instance Profiles.
    Dynamic credentials are then automatically obtained from AWS API and no
    further configuration is necessary. More Information available at:

    .. code-block:: text

        http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html

    If IAM roles are not used you need to specify them either in the minion's
    config file or as a profile. For example, to specify them in the minion's
    config file:

    .. code-block:: yaml

        cloudwatch_event.keyid: GKTADJGHEIQSXMKKRBJ08H
        cloudwatch_event.key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs

    A region may also be specified in the configuration:

    .. code-block:: yaml

        cloudwatch_event.region: us-east-1

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

from saltext.boto3.utils import boto3mod

try:
    from botocore.exceptions import ClientError

    logging.getLogger("boto3").setLevel(logging.CRITICAL)
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

log = logging.getLogger(__name__)

__virtualname__ = "boto3_cloudwatch_event"


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
        "The boto3_cloudwatch_event module could not be loaded: boto3 is not available.",
    )


def exists(Name, region=None, key=None, keyid=None, profile=None):
    """
    Given a rule name, check to see if the given rule exists.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_cloudwatch_event.exists myevent region=us-east-1
    """
    try:
        conn = _get_conn("events", region=region, key=key, keyid=keyid, profile=profile)
        events = conn.list_rules(NamePrefix=Name)
        if not events:
            return {"exists": False}
        for rule in events.get("Rules", []):
            if rule.get("Name") == Name:
                return {"exists": True}
        return {"exists": False}
    except ClientError as e:
        return {"error": boto3mod.get_error(e)}


def create_or_update(
    Name,
    ScheduleExpression=None,
    EventPattern=None,
    Description=None,
    RoleArn=None,
    State=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):  # pylint: disable=unused-argument
    """
    Given a valid config, create an event rule.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_cloudwatch_event.create_or_update my_rule
    """
    try:
        conn = _get_conn("events", region=region, key=key, keyid=keyid, profile=profile)
        kwargs = {}
        for arg in (
            "ScheduleExpression",
            "EventPattern",
            "State",
            "Description",
            "RoleArn",
        ):
            if locals()[arg] is not None:
                kwargs[arg] = locals()[arg]
        rule = conn.put_rule(Name=Name, **kwargs)
        if rule:
            log.info("The newly created event rule is %s", rule.get("RuleArn"))
            return {"created": True, "arn": rule.get("RuleArn")}
        log.warning("Event rule was not created")
        return {"created": False}
    except ClientError as e:
        return {"created": False, "error": boto3mod.get_error(e)}


def delete(Name, region=None, key=None, keyid=None, profile=None):
    """
    Given a rule name, delete it.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_cloudwatch_event.delete myrule
    """
    try:
        conn = _get_conn("events", region=region, key=key, keyid=keyid, profile=profile)
        conn.delete_rule(Name=Name)
        return {"deleted": True}
    except ClientError as e:
        return {"deleted": False, "error": boto3mod.get_error(e)}


def describe(Name, region=None, key=None, keyid=None, profile=None):
    """
    Given a rule name describe its properties.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_cloudwatch_event.describe myrule
    """
    try:
        conn = _get_conn("events", region=region, key=key, keyid=keyid, profile=profile)
        rule = conn.describe_rule(Name=Name)
        if rule:
            keys = (
                "Name",
                "Arn",
                "EventPattern",
                "ScheduleExpression",
                "State",
                "Description",
                "RoleArn",
            )
            return {"rule": {k: rule.get(k) for k in keys}}
        return {"rule": None}
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
            return {"error": f"Rule {Name} not found"}
        return {"error": boto3mod.get_error(e)}


def list_rules(region=None, key=None, keyid=None, profile=None):
    """
    List, with details, all CloudWatch Event rules visible in the current scope.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_cloudwatch_event.list_rules region=us-east-1
    """
    try:
        conn = _get_conn("events", region=region, key=key, keyid=keyid, profile=profile)
        ret = []
        NextToken = ""
        while NextToken is not None:
            args = {"NextToken": NextToken} if NextToken else {}
            r = conn.list_rules(**args)
            ret += r.get("Rules", [])
            NextToken = r.get("NextToken")
        return ret
    except ClientError as e:
        return {"error": boto3mod.get_error(e)}


def list_targets(Rule, region=None, key=None, keyid=None, profile=None):
    """
    Given a rule name list the targets of that rule.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_cloudwatch_event.list_targets myrule
    """
    try:
        conn = _get_conn("events", region=region, key=key, keyid=keyid, profile=profile)
        targets = conn.list_targets_by_rule(Rule=Rule)
        ret = []
        if targets and "Targets" in targets:
            keys = ("Id", "Arn", "Input", "InputPath")
            for target in targets.get("Targets"):
                ret.append({k: target.get(k) for k in keys if k in target})
            return {"targets": ret}
        return {"targets": None}
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
            return {"error": f"Rule {Rule} not found"}
        return {"error": boto3mod.get_error(e)}


def put_targets(Rule, Targets, region=None, key=None, keyid=None, profile=None):
    """
    Add the given targets to the given rule.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_cloudwatch_event.put_targets myrule [{'Id':'target1','Arn':'arn:...'}]
    """
    try:
        conn = _get_conn("events", region=region, key=key, keyid=keyid, profile=profile)
        if isinstance(Targets, str):
            Targets = salt.utils.json.loads(Targets)
        failures = conn.put_targets(Rule=Rule, Targets=Targets)
        if failures and failures.get("FailedEntryCount", 0) > 0:
            return {"failures": failures.get("FailedEntries")}
        return {"failures": None}
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
            return {"error": f"Rule {Rule} not found"}
        return {"error": boto3mod.get_error(e)}


def remove_targets(Rule, Ids, region=None, key=None, keyid=None, profile=None):
    """
    Given a rule name remove the named targets from the target list.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_cloudwatch_event.remove_targets myrule ['Target1']
    """
    try:
        conn = _get_conn("events", region=region, key=key, keyid=keyid, profile=profile)
        if isinstance(Ids, str):
            Ids = salt.utils.json.loads(Ids)
        failures = conn.remove_targets(Rule=Rule, Ids=Ids)
        if failures and failures.get("FailedEntryCount", 0) > 0:
            return {"failures": failures.get("FailedEntries", 1)}
        return {"failures": None}
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
            return {"error": f"Rule {Rule} not found"}
        return {"error": boto3mod.get_error(e)}
