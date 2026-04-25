"""
Manage CloudWatch Event Rules using boto3.
==========================================

    Renamed from ``boto_cloudwatch_event`` to ``boto3_cloudwatch_event`` and updated to call the
    refactored ``boto3_cloudwatch_event`` execution module.

Create and destroy CloudWatch event rules. Be aware that this interacts with
Amazon's services, and so may incur charges.

:depends:
  - boto3 >= 1.28.0
  - botocore >= 1.31.0

This module uses boto3, which can be installed via package, or pip.

This module accepts explicit CloudWatch credentials but can also utilize
IAM roles assigned to the instance through Instance Profiles. Dynamic
credentials are then automatically obtained from AWS API and no further
configuration is necessary. More Information available at:

.. code-block:: text

    http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html

If IAM roles are not used you need to specify them either in the minion's config file
or as a profile. For example, to specify them in the minion's config file:

.. code-block:: yaml

    cloudwatch.keyid: GKTADJGHEIQSXMKKRBJ08H
    cloudwatch.key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs

It's also possible to specify key, keyid and region via a profile, either
as a passed in dict, or as a string to pull from pillars or minion config:

.. code-block:: yaml

    myprofile:
        keyid: GKTADJGHEIQSXMKKRBJ08H
        key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs
        region: us-east-1

.. code-block:: yaml

    Ensure event rule exists:
        boto3_cloudwatch_event.present:
            - Name: mytrail
            - ScheduleExpression: 'rate(120 minutes)'
            - State: 'DISABLED'
            - Targets:
              - Id: "target1"
                Arn: "arn:aws:lambda:us-west-1:124456715622:function:my_function"
                Input: '{"arbitrary": "json"}'

.. versionadded:: 1.0.0
"""

import logging
import os

import salt.utils.json

log = logging.getLogger(__name__)

__virtualname__ = "boto3_cloudwatch_event"


def __virtual__():
    """
    Only load if the boto3_cloudwatch_event execution module is available.
    """
    if "boto3_cloudwatch_event.exists" in __salt__:
        return __virtualname__
    return (
        False,
        "The boto3_cloudwatch_event state module could not be loaded: "
        "boto3_cloudwatch_event exec module unavailable.",
    )


def present(
    name,
    Name=None,
    ScheduleExpression=None,
    EventPattern=None,
    Description=None,
    RoleArn=None,
    State=None,
    Targets=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Ensure the CloudWatch event rule exists.

    name
        The name of the state definition.

    Name
        Name of the event rule. Defaults to the value of ``name`` if not provided.

    ScheduleExpression
        The scheduling expression. For example, ``cron(0 20 * * ? *)`` or
        ``rate(5 minutes)``.

    EventPattern
        The event pattern.

    Description
        A description of the rule.

    State
        Indicates whether the rule is ENABLED or DISABLED.

    RoleArn
        The ARN of the IAM role associated with the rule.

    Targets
        A list of resources to be invoked when the rule is triggered.

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
          boto3_cloudwatch_event.present:
            - name: example

    """
    ret = {"name": Name, "result": True, "comment": "", "changes": {}}

    Name = Name if Name else name

    if isinstance(Targets, str):
        Targets = salt.utils.json.loads(Targets)
    if Targets is None:
        Targets = []

    r = __salt__["boto3_cloudwatch_event.exists"](
        Name=Name, region=region, key=key, keyid=keyid, profile=profile
    )

    if "error" in r:
        ret["result"] = False
        ret["comment"] = "Failed to create event rule: {}.".format(r["error"]["message"])
        return ret

    if not r.get("exists"):
        if __opts__["test"]:
            ret["comment"] = f"CloudWatch event rule {Name} is set to be created."
            ret["result"] = None
            return ret
        r = __salt__["boto3_cloudwatch_event.create_or_update"](
            Name=Name,
            ScheduleExpression=ScheduleExpression,
            EventPattern=EventPattern,
            Description=Description,
            RoleArn=RoleArn,
            State=State,
            region=region,
            key=key,
            keyid=keyid,
            profile=profile,
        )
        if not r.get("created"):
            ret["result"] = False
            ret["comment"] = "Failed to create event rule: {}.".format(r["error"]["message"])
            return ret
        _describe = __salt__["boto3_cloudwatch_event.describe"](
            Name, region=region, key=key, keyid=keyid, profile=profile
        )
        if "error" in _describe:
            ret["result"] = False
            ret["comment"] = "Failed to create event rule: {}.".format(
                _describe["error"]["message"]
            )
            ret["changes"] = {}
            return ret
        ret["changes"]["old"] = {"rule": None}
        ret["changes"]["new"] = _describe
        ret["comment"] = f"CloudWatch event rule {Name} created."

        if bool(Targets):
            r = __salt__["boto3_cloudwatch_event.put_targets"](
                Rule=Name,
                Targets=Targets,
                region=region,
                key=key,
                keyid=keyid,
                profile=profile,
            )
            if "error" in r:
                ret["result"] = False
                ret["comment"] = "Failed to create event rule: {}.".format(r["error"]["message"])
                ret["changes"] = {}
                return ret
            ret["changes"]["new"]["rule"]["Targets"] = Targets
        return ret

    ret["comment"] = os.linesep.join([ret["comment"], f"CloudWatch event rule {Name} is present."])
    ret["changes"] = {}
    _describe = __salt__["boto3_cloudwatch_event.describe"](
        Name=Name, region=region, key=key, keyid=keyid, profile=profile
    )
    if "error" in _describe:
        ret["result"] = False
        ret["comment"] = "Failed to update event rule: {}.".format(_describe["error"]["message"])
        ret["changes"] = {}
        return ret
    _describe = _describe.get("rule")

    r = __salt__["boto3_cloudwatch_event.list_targets"](
        Rule=Name, region=region, key=key, keyid=keyid, profile=profile
    )
    if "error" in r:
        ret["result"] = False
        ret["comment"] = "Failed to update event rule: {}.".format(r["error"]["message"])
        ret["changes"] = {}
        return ret
    _describe["Targets"] = r.get("targets") or []

    need_update = False
    rule_vars = {
        "ScheduleExpression": "ScheduleExpression",
        "EventPattern": "EventPattern",
        "Description": "Description",
        "RoleArn": "RoleArn",
        "State": "State",
        "Targets": "Targets",
    }
    for invar, outvar in rule_vars.items():
        if _describe[outvar] != locals()[invar]:
            need_update = True
            ret["changes"].setdefault("new", {})[invar] = locals()[invar]
            ret["changes"].setdefault("old", {})[invar] = _describe[outvar]

    if need_update:
        if __opts__["test"]:
            ret["comment"] = f"CloudWatch event rule {Name} set to be modified."
            ret["result"] = None
            return ret

        ret["comment"] = os.linesep.join([ret["comment"], "CloudWatch event rule to be modified"])
        r = __salt__["boto3_cloudwatch_event.create_or_update"](
            Name=Name,
            ScheduleExpression=ScheduleExpression,
            EventPattern=EventPattern,
            Description=Description,
            RoleArn=RoleArn,
            State=State,
            region=region,
            key=key,
            keyid=keyid,
            profile=profile,
        )
        if not r.get("created"):
            ret["result"] = False
            ret["comment"] = "Failed to update event rule: {}.".format(r["error"]["message"])
            ret["changes"] = {}
            return ret

        if _describe["Targets"] != Targets:
            removes = [i.get("Id") for i in _describe["Targets"]]
            if bool(Targets):
                for target in Targets:
                    tid = target.get("Id", None)
                    if tid is not None and tid in removes:
                        removes.remove(tid)
                r = __salt__["boto3_cloudwatch_event.put_targets"](
                    Rule=Name,
                    Targets=Targets,
                    region=region,
                    key=key,
                    keyid=keyid,
                    profile=profile,
                )
                if "error" in r:
                    ret["result"] = False
                    ret["comment"] = "Failed to update event rule: {}.".format(
                        r["error"]["message"]
                    )
                    ret["changes"] = {}
                    return ret
            if bool(removes):
                r = __salt__["boto3_cloudwatch_event.remove_targets"](
                    Rule=Name,
                    Ids=removes,
                    region=region,
                    key=key,
                    keyid=keyid,
                    profile=profile,
                )
                if "error" in r:
                    ret["result"] = False
                    ret["comment"] = "Failed to update event rule: {}.".format(
                        r["error"]["message"]
                    )
                    ret["changes"] = {}
                    return ret
    return ret


def absent(name, Name=None, region=None, key=None, keyid=None, profile=None):
    """
    Ensure CloudWatch event rule with passed properties is absent.

    name
        The name of the state definition.

    Name
        Name of the event rule. Defaults to the value of ``name`` if not provided.

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
          boto3_cloudwatch_event.absent:
            - name: example

    """
    ret = {"name": Name, "result": True, "comment": "", "changes": {}}

    Name = Name if Name else name

    r = __salt__["boto3_cloudwatch_event.exists"](
        Name, region=region, key=key, keyid=keyid, profile=profile
    )
    if "error" in r:
        ret["result"] = False
        ret["comment"] = "Failed to delete event rule: {}.".format(r["error"]["message"])
        return ret

    if r and not r["exists"]:
        ret["comment"] = f"CloudWatch event rule {Name} does not exist."
        return ret

    if __opts__["test"]:
        ret["comment"] = f"CloudWatch event rule {Name} is set to be removed."
        ret["result"] = None
        return ret

    r = __salt__["boto3_cloudwatch_event.list_targets"](
        Rule=Name, region=region, key=key, keyid=keyid, profile=profile
    )
    if "error" in r:
        ret["result"] = False
        ret["comment"] = "Failed to delete event rule: {}.".format(r["error"]["message"])
        return ret
    ids = [t.get("Id") for t in (r.get("targets") or [])]
    if bool(ids):
        r = __salt__["boto3_cloudwatch_event.remove_targets"](
            Rule=Name, Ids=ids, region=region, key=key, keyid=keyid, profile=profile
        )
        if "error" in r:
            ret["result"] = False
            ret["comment"] = "Failed to delete event rule: {}.".format(r["error"]["message"])
            return ret
        if r.get("failures"):
            ret["result"] = False
            ret["comment"] = "Failed to delete event rule: {}.".format(r["failures"])
            return ret

    r = __salt__["boto3_cloudwatch_event.delete"](
        Name, region=region, key=key, keyid=keyid, profile=profile
    )
    if not r["deleted"]:
        ret["result"] = False
        ret["comment"] = "Failed to delete event rule: {}.".format(r["error"]["message"])
        return ret
    ret["changes"]["old"] = {"rule": Name}
    ret["changes"]["new"] = {"rule": None}
    ret["comment"] = f"CloudWatch event rule {Name} deleted."
    return ret
