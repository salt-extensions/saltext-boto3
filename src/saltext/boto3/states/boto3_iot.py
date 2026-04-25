"""
Manage IoT Objects using boto3.
===============================

    Renamed from ``boto_iot`` to ``boto3_iot`` and updated to call the
    refactored ``boto3_iot`` execution module.

Create and destroy IoT objects. Be aware that this interacts with Amazon's services,
and so may incur charges.

:depends:
  - boto3 >= 1.28.0
  - botocore >= 1.31.0

This module uses boto3, which can be installed via package, or pip.

This module accepts explicit IoT credentials but can also utilize
IAM roles assigned to the instance through Instance Profiles. Dynamic
credentials are then automatically obtained from AWS API and no further
configuration is necessary. More Information available at:

.. code-block:: text

    http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html

If IAM roles are not used you need to specify them either in the minion's config file
or as a profile. For example, to specify them in the minion's config file:

.. code-block:: yaml

    iot.keyid: GKTADJGHEIQSXMKKRBJ08H
    iot.key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs

It's also possible to specify key, keyid and region via a profile, either
as a passed in dict, or as a string to pull from pillars or minion config:

.. code-block:: yaml

    myprofile:
        keyid: GKTADJGHEIQSXMKKRBJ08H
        key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs
        region: us-east-1

.. code-block:: yaml

    Ensure policy exists:
        boto3_iot.policy_present:
            - policyName: mypolicy
            - policyDocument:
                Version: "2012-10-17"
                Statement:
                  Action:
                    - iot:Publish
                  Resource:
                    - "*"
                  Effect: "Allow"

    Ensure topic rule exists:
        boto3_iot.topic_rule_present:
            - ruleName: myrule
            - sql: "SELECT * FROM 'iot/test'"
            - description: 'test rule'
            - ruleDisabled: false
            - actions:
              - lambda:
                  functionArn: "arn:aws:us-east-1:1234:function/functionname"

.. versionadded:: 1.0.0
"""

import datetime
import logging
import os
import time

import salt.utils.data
import salt.utils.json

log = logging.getLogger(__name__)

__virtualname__ = "boto3_iot"


def __virtual__():
    """
    Only load if the boto3_iot execution module is available.
    """
    if "boto3_iot.policy_exists" in __salt__:
        return __virtualname__
    return (
        False,
        "boto3_iot state module could not be loaded: "
        "boto3_iot execution module is not available.",
    )


def thing_type_present(
    name,
    thingTypeName,
    thingTypeDescription,
    searchableAttributesList,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):  # pylint: disable=unused-argument
    """
    Ensure thing type exists.

    Example:

    .. code-block:: yaml

        ensure-thing-type-present:
          boto3_iot.thing_type_present:
            - name: example

    """
    ret = {"name": thingTypeName, "result": True, "comment": "", "changes": {}}

    r = __salt__["boto3_iot.thing_type_exists"](
        thingTypeName=thingTypeName,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )

    if "error" in r:
        ret["result"] = False
        ret["comment"] = "Failed to create thing type: {}.".format(r["error"]["message"])
        return ret

    if r.get("exists"):
        ret["result"] = True
        ret["comment"] = f"Thing type with given name {thingTypeName} already exists"
        return ret

    if __opts__["test"]:
        ret["comment"] = f"Thing type {thingTypeName} is set to be created."
        ret["result"] = None
        return ret

    r = __salt__["boto3_iot.create_thing_type"](
        thingTypeName=thingTypeName,
        thingTypeDescription=thingTypeDescription,
        searchableAttributesList=searchableAttributesList,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )

    if not r.get("created"):
        ret["result"] = False
        ret["comment"] = "Failed to create thing type: {}.".format(r["error"]["message"])
        return ret

    _describe = __salt__["boto3_iot.describe_thing_type"](
        thingTypeName=thingTypeName,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )
    ret["changes"]["old"] = {"thing_type": None}
    ret["changes"]["new"] = _describe
    ret["comment"] = f"Thing Type {thingTypeName} created."
    return ret


def thing_type_absent(
    name, thingTypeName, region=None, key=None, keyid=None, profile=None
):  # pylint: disable=unused-argument
    """
    Ensure thing type with passed properties is absent.

    Example:

    .. code-block:: yaml

        ensure-thing-type-absent:
          boto3_iot.thing_type_absent:
            - name: example

    """
    ret = {"name": thingTypeName, "result": True, "comment": "", "changes": {}}

    _describe = __salt__["boto3_iot.describe_thing_type"](
        thingTypeName=thingTypeName,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )
    if "error" in _describe:
        ret["result"] = False
        ret["comment"] = "Failed to delete thing type: {}.".format(_describe["error"]["message"])
        return ret

    if _describe and not _describe["thing_type"]:
        ret["comment"] = f"Thing Type {thingTypeName} does not exist."
        return ret

    _existing_thing_type = _describe["thing_type"]
    _thing_type_metadata = _existing_thing_type.get("thingTypeMetadata")
    _deprecated = _thing_type_metadata.get("deprecated", False)

    if __opts__["test"]:
        if _deprecated:
            _change_desc = "removed"
        else:
            _change_desc = "deprecated and removed"
        ret["comment"] = f"Thing Type {thingTypeName} is set to be {_change_desc}."
        ret["result"] = None
        return ret

    # AWS does not allow delete thing type until 5 minutes after deprecation.
    _delete_wait_timer = 300

    if _deprecated is False:
        _deprecate = __salt__["boto3_iot.deprecate_thing_type"](
            thingTypeName=thingTypeName,
            undoDeprecate=False,
            region=region,
            key=key,
            keyid=keyid,
            profile=profile,
        )
        if "error" in _deprecate:
            ret["result"] = False
            ret["comment"] = "Failed to deprecate thing type: {}.".format(
                _deprecate["error"]["message"]
            )
            return ret
    else:
        _deprecation_date_str = _thing_type_metadata.get("deprecationDate")
        if _deprecation_date_str:
            _tz_index = _deprecation_date_str.find("+")
            if _tz_index != -1:
                _deprecation_date_str = _deprecation_date_str[:_tz_index]

            _deprecation_date = datetime.datetime.strptime(
                _deprecation_date_str, "%Y-%m-%d %H:%M:%S.%f"
            )

            _elapsed_time_delta = datetime.datetime.utcnow() - _deprecation_date
            if _elapsed_time_delta.seconds >= 300:
                _delete_wait_timer = 0
            else:
                _delete_wait_timer = 300 - _elapsed_time_delta.seconds

    if _delete_wait_timer:
        log.warning(
            "wait for %s seconds per AWS (5 minutes after deprecation time) "
            "before we can delete iot thing type",
            _delete_wait_timer,
        )
        time.sleep(_delete_wait_timer)

    r = __salt__["boto3_iot.delete_thing_type"](
        thingTypeName=thingTypeName,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )
    if not r["deleted"]:
        ret["result"] = False
        ret["comment"] = "Failed to delete thing type: {}.".format(r["error"]["message"])
        return ret
    ret["changes"]["old"] = _describe
    ret["changes"]["new"] = {"thing_type": None}
    ret["comment"] = f"Thing Type {thingTypeName} deleted."
    return ret


def policy_present(
    name, policyName, policyDocument, region=None, key=None, keyid=None, profile=None
):  # pylint: disable=unused-argument
    """
    Ensure policy exists.

    Example:

    .. code-block:: yaml

        ensure-policy-present:
          boto3_iot.policy_present:
            - name: example

    """
    ret = {"name": policyName, "result": True, "comment": "", "changes": {}}

    r = __salt__["boto3_iot.policy_exists"](
        policyName=policyName, region=region, key=key, keyid=keyid, profile=profile
    )

    if "error" in r:
        ret["result"] = False
        ret["comment"] = "Failed to create policy: {}.".format(r["error"]["message"])
        return ret

    if not r.get("exists"):
        if __opts__["test"]:
            ret["comment"] = f"Policy {policyName} is set to be created."
            ret["result"] = None
            return ret
        r = __salt__["boto3_iot.create_policy"](
            policyName=policyName,
            policyDocument=policyDocument,
            region=region,
            key=key,
            keyid=keyid,
            profile=profile,
        )
        if not r.get("created"):
            ret["result"] = False
            ret["comment"] = "Failed to create policy: {}.".format(r["error"]["message"])
            return ret
        _describe = __salt__["boto3_iot.describe_policy"](
            policyName, region=region, key=key, keyid=keyid, profile=profile
        )
        ret["changes"]["old"] = {"policy": None}
        ret["changes"]["new"] = _describe
        ret["comment"] = f"Policy {policyName} created."
        return ret

    ret["comment"] = os.linesep.join([ret["comment"], f"Policy {policyName} is present."])
    ret["changes"] = {}
    _describe = __salt__["boto3_iot.describe_policy"](
        policyName=policyName, region=region, key=key, keyid=keyid, profile=profile
    )["policy"]

    if isinstance(_describe["policyDocument"], str):
        describeDict = salt.utils.json.loads(_describe["policyDocument"])
    else:
        describeDict = _describe["policyDocument"]

    if isinstance(policyDocument, str):
        policyDocument = salt.utils.json.loads(policyDocument)

    r = salt.utils.data.compare_dicts(describeDict, policyDocument)
    if bool(r):
        if __opts__["test"]:
            msg = f"Policy {policyName} set to be modified."
            ret["comment"] = msg
            ret["result"] = None
            return ret

        ret["comment"] = os.linesep.join([ret["comment"], "Policy to be modified"])
        policyDocument = salt.utils.json.dumps(policyDocument)

        r = __salt__["boto3_iot.create_policy_version"](
            policyName=policyName,
            policyDocument=policyDocument,
            setAsDefault=True,
            region=region,
            key=key,
            keyid=keyid,
            profile=profile,
        )
        if not r.get("created"):
            ret["result"] = False
            ret["comment"] = "Failed to update policy: {}.".format(r["error"]["message"])
            ret["changes"] = {}
            return ret

        __salt__["boto3_iot.delete_policy_version"](
            policyName=policyName,
            policyVersionId=_describe["defaultVersionId"],
            region=region,
            key=key,
            keyid=keyid,
            profile=profile,
        )

        ret["changes"].setdefault("new", {})["policyDocument"] = policyDocument
        ret["changes"].setdefault("old", {})["policyDocument"] = _describe["policyDocument"]
    return ret


def policy_absent(
    name, policyName, region=None, key=None, keyid=None, profile=None
):  # pylint: disable=unused-argument
    """
    Ensure policy with passed properties is absent.

    Example:

    .. code-block:: yaml

        ensure-policy-absent:
          boto3_iot.policy_absent:
            - name: example

    """
    ret = {"name": policyName, "result": True, "comment": "", "changes": {}}

    r = __salt__["boto3_iot.policy_exists"](
        policyName, region=region, key=key, keyid=keyid, profile=profile
    )
    if "error" in r:
        ret["result"] = False
        ret["comment"] = "Failed to delete policy: {}.".format(r["error"]["message"])
        return ret

    if r and not r["exists"]:
        ret["comment"] = f"Policy {policyName} does not exist."
        return ret

    if __opts__["test"]:
        ret["comment"] = f"Policy {policyName} is set to be removed."
        ret["result"] = None
        return ret
    # delete non-default versions
    versions = __salt__["boto3_iot.list_policy_versions"](
        policyName, region=region, key=key, keyid=keyid, profile=profile
    )
    if versions:
        for version in versions.get("policyVersions", []):
            if version.get("isDefaultVersion", False):
                continue
            r = __salt__["boto3_iot.delete_policy_version"](
                policyName,
                policyVersionId=version.get("versionId"),
                region=region,
                key=key,
                keyid=keyid,
                profile=profile,
            )
            if not r["deleted"]:
                ret["result"] = False
                ret["comment"] = "Failed to delete policy: {}.".format(r["error"]["message"])
                return ret

    r = __salt__["boto3_iot.delete_policy"](
        policyName, region=region, key=key, keyid=keyid, profile=profile
    )
    if not r["deleted"]:
        ret["result"] = False
        ret["comment"] = "Failed to delete policy: {}.".format(r["error"]["message"])
        return ret
    ret["changes"]["old"] = {"policy": policyName}
    ret["changes"]["new"] = {"policy": None}
    ret["comment"] = f"Policy {policyName} deleted."
    return ret


def policy_attached(
    name, policyName, principal, region=None, key=None, keyid=None, profile=None
):  # pylint: disable=unused-argument
    """
    Ensure policy is attached to the given principal.

    Example:

    .. code-block:: yaml

        ensure-policy-attached:
          boto3_iot.policy_attached:
            - name: example

    """
    ret = {"name": policyName, "result": True, "comment": "", "changes": {}}

    r = __salt__["boto3_iot.list_principal_policies"](
        principal=principal, region=region, key=key, keyid=keyid, profile=profile
    )

    if "error" in r:
        ret["result"] = False
        ret["comment"] = "Failed to attach policy: {}.".format(r["error"]["message"])
        return ret

    attached = False
    for policy in r.get("policies", []):
        if policy.get("policyName") == policyName:
            attached = True
            break
    if not attached:
        if __opts__["test"]:
            ret["comment"] = f"Policy {policyName} is set to be attached to {principal}."
            ret["result"] = None
            return ret
        r = __salt__["boto3_iot.attach_principal_policy"](
            policyName=policyName,
            principal=principal,
            region=region,
            key=key,
            keyid=keyid,
            profile=profile,
        )
        if not r.get("attached"):
            ret["result"] = False
            ret["comment"] = "Failed to attach policy: {}.".format(r["error"]["message"])
            return ret
        ret["changes"]["old"] = {"attached": False}
        ret["changes"]["new"] = {"attached": True}
        ret["comment"] = f"Policy {policyName} attached to {principal}."
        return ret

    ret["comment"] = os.linesep.join([ret["comment"], f"Policy {policyName} is attached."])
    ret["changes"] = {}
    return ret


def policy_detached(
    name, policyName, principal, region=None, key=None, keyid=None, profile=None
):  # pylint: disable=unused-argument
    """
    Ensure policy is detached from the given principal.

    Example:

    .. code-block:: yaml

        ensure-policy-detached:
          boto3_iot.policy_detached:
            - name: example

    """
    ret = {"name": policyName, "result": True, "comment": "", "changes": {}}

    r = __salt__["boto3_iot.list_principal_policies"](
        principal=principal, region=region, key=key, keyid=keyid, profile=profile
    )

    if "error" in r:
        ret["result"] = False
        ret["comment"] = "Failed to detached policy: {}.".format(r["error"]["message"])
        return ret

    attached = False
    for policy in r.get("policies", []):
        if policy.get("policyName") == policyName:
            attached = True
            break
    if attached:
        if __opts__["test"]:
            ret["comment"] = f"Policy {policyName} is set to be detached from {principal}."
            ret["result"] = None
            return ret
        r = __salt__["boto3_iot.detach_principal_policy"](
            policyName=policyName,
            principal=principal,
            region=region,
            key=key,
            keyid=keyid,
            profile=profile,
        )
        if not r.get("detached"):
            ret["result"] = False
            ret["comment"] = "Failed to detach policy: {}.".format(r["error"]["message"])
            return ret
        ret["changes"]["old"] = {"attached": True}
        ret["changes"]["new"] = {"attached": False}
        ret["comment"] = f"Policy {policyName} detached from {principal}."
        return ret

    ret["comment"] = os.linesep.join([ret["comment"], f"Policy {policyName} is detached."])
    ret["changes"] = {}
    return ret


def topic_rule_present(
    name,
    ruleName,
    sql,
    actions,
    description="",
    ruleDisabled=False,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):  # pylint: disable=unused-argument
    """
    Ensure topic rule exists.

    Example:

    .. code-block:: yaml

        ensure-topic-rule-present:
          boto3_iot.topic_rule_present:
            - name: example

    """
    ret = {"name": ruleName, "result": True, "comment": "", "changes": {}}

    r = __salt__["boto3_iot.topic_rule_exists"](
        ruleName=ruleName, region=region, key=key, keyid=keyid, profile=profile
    )

    if "error" in r:
        ret["result"] = False
        ret["comment"] = "Failed to create rule: {}.".format(r["error"]["message"])
        return ret

    if not r.get("exists"):
        if __opts__["test"]:
            ret["comment"] = f"Rule {ruleName} is set to be created."
            ret["result"] = None
            return ret
        r = __salt__["boto3_iot.create_topic_rule"](
            ruleName=ruleName,
            sql=sql,
            actions=actions,
            description=description,
            ruleDisabled=ruleDisabled,
            region=region,
            key=key,
            keyid=keyid,
            profile=profile,
        )
        if not r.get("created"):
            ret["result"] = False
            ret["comment"] = "Failed to create rule: {}.".format(r["error"]["message"])
            return ret
        _describe = __salt__["boto3_iot.describe_topic_rule"](
            ruleName, region=region, key=key, keyid=keyid, profile=profile
        )
        ret["changes"]["old"] = {"rule": None}
        ret["changes"]["new"] = _describe
        ret["comment"] = f"Rule {ruleName} created."
        return ret

    ret["comment"] = os.linesep.join([ret["comment"], f"Rule {ruleName} is present."])
    ret["changes"] = {}
    _describe = __salt__["boto3_iot.describe_topic_rule"](
        ruleName=ruleName, region=region, key=key, keyid=keyid, profile=profile
    )["rule"]

    if isinstance(actions, str):
        actions = salt.utils.json.loads(actions)

    need_update = False
    r = (_describe["actions"] > actions) - (_describe["actions"] < actions)
    if bool(r):
        need_update = True
        ret["changes"].setdefault("new", {})["actions"] = actions
        ret["changes"].setdefault("old", {})["actions"] = _describe["actions"]

    for var in ("sql", "description", "ruleDisabled"):
        if _describe[var] != locals()[var]:
            need_update = True
            ret["changes"].setdefault("new", {})[var] = locals()[var]
            ret["changes"].setdefault("old", {})[var] = _describe[var]
    if need_update:
        if __opts__["test"]:
            msg = f"Rule {ruleName} set to be modified."
            ret["changes"] = {}
            ret["comment"] = msg
            ret["result"] = None
            return ret
        ret["comment"] = os.linesep.join([ret["comment"], "Rule to be modified"])
        r = __salt__["boto3_iot.replace_topic_rule"](
            ruleName=ruleName,
            sql=sql,
            actions=actions,
            description=description,
            ruleDisabled=ruleDisabled,
            region=region,
            key=key,
            keyid=keyid,
            profile=profile,
        )
        if not r.get("replaced"):
            ret["result"] = False
            ret["comment"] = "Failed to update rule: {}.".format(r["error"]["message"])
            ret["changes"] = {}
    return ret


def topic_rule_absent(
    name, ruleName, region=None, key=None, keyid=None, profile=None
):  # pylint: disable=unused-argument
    """
    Ensure topic rule with passed properties is absent.

    Example:

    .. code-block:: yaml

        ensure-topic-rule-absent:
          boto3_iot.topic_rule_absent:
            - name: example

    """
    ret = {"name": ruleName, "result": True, "comment": "", "changes": {}}

    r = __salt__["boto3_iot.topic_rule_exists"](
        ruleName, region=region, key=key, keyid=keyid, profile=profile
    )
    if "error" in r:
        ret["result"] = False
        ret["comment"] = "Failed to delete rule: {}.".format(r["error"]["message"])
        return ret

    if r and not r["exists"]:
        ret["comment"] = f"Rule {ruleName} does not exist."
        return ret

    if __opts__["test"]:
        ret["comment"] = f"Rule {ruleName} is set to be removed."
        ret["result"] = None
        return ret
    r = __salt__["boto3_iot.delete_topic_rule"](
        ruleName, region=region, key=key, keyid=keyid, profile=profile
    )
    if not r["deleted"]:
        ret["result"] = False
        ret["comment"] = "Failed to delete rule: {}.".format(r["error"]["message"])
        return ret
    ret["changes"]["old"] = {"rule": ruleName}
    ret["changes"]["new"] = {"rule": None}
    ret["comment"] = f"Rule {ruleName} deleted."
    return ret
