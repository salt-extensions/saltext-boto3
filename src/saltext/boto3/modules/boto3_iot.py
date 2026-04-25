"""
Connection module for Amazon IoT using boto3.
=============================================

    Renamed from ``boto_iot`` to ``boto3_iot`` and rewritten to use the
    boto3 ``iot`` client APIs directly via
    :py:mod:`saltext.boto3.utils.boto3mod`.  The legacy boto2 code path
    (object-style access, retry loops) has been removed.

:depends:
  - boto3 >= 1.28.0
  - botocore >= 1.31.0

:configuration: This module accepts explicit IoT credentials but can
    also utilize IAM roles assigned to the instance through Instance Profiles.
    Dynamic credentials are then automatically obtained from AWS API and no
    further configuration is necessary. More Information available at:

    .. code-block:: text

        http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html

    If IAM roles are not used you need to specify them either in the minion's
    config file or as a profile. For example, to specify them in the minion's
    config file:

.. code-block:: yaml

    iot.keyid: GKTADJGHEIQSXMKKRBJ08H
    iot.key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs

A region may also be specified in the configuration:

.. code-block:: yaml

    iot.region: us-east-1

It's also possible to specify key, keyid and region via a profile, either
as a passed in dict, or as a string to pull from pillars or minion config:

.. code-block:: yaml

    myprofile:
        keyid: GKTADJGHEIQSXMKKRBJ08H
        key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs
        region: us-east-1

.. versionadded:: 1.0.0
"""

import datetime
import logging

import salt.utils.json

from saltext.boto3.utils import boto3mod

try:
    from botocore.exceptions import ClientError

    logging.getLogger("boto3").setLevel(logging.CRITICAL)
    logging.getLogger("botocore").setLevel(logging.CRITICAL)
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

log = logging.getLogger(__name__)

__virtualname__ = "boto3_iot"


def __virtual__():
    """
    Only load if boto3 is available.
    """
    if HAS_BOTO3:
        return __virtualname__
    return (False, "The boto3_iot module could not be loaded: boto3 is not available.")


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


def thing_type_exists(thingTypeName, region=None, key=None, keyid=None, profile=None):
    """
    Check to see if the given thing type exists.

    Returns ``{'exists': True}`` / ``{'exists': False}``.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_iot.thing_type_exists

    """
    try:
        conn = _get_conn("iot", region, key, keyid, profile)
        res = conn.describe_thing_type(thingTypeName=thingTypeName)
        return {"exists": bool(res.get("thingTypeName"))}
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
            return {"exists": False}
        return {"error": boto3mod.get_error(e)}


def describe_thing_type(thingTypeName, region=None, key=None, keyid=None, profile=None):
    """
    Describe the given thing type.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_iot.describe_thing_type

    """
    try:
        conn = _get_conn("iot", region, key, keyid, profile)
        res = conn.describe_thing_type(thingTypeName=thingTypeName)
        if res:
            res.pop("ResponseMetadata", None)
            thingTypeMetadata = res.get("thingTypeMetadata")
            if thingTypeMetadata:
                for dtype in ("creationDate", "deprecationDate"):
                    dval = thingTypeMetadata.get(dtype)
                    if dval and isinstance(dval, datetime.date):
                        thingTypeMetadata[dtype] = f"{dval}"
            return {"thing_type": res}
        return {"thing_type": None}
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
            return {"thing_type": None}
        return {"error": boto3mod.get_error(e)}


def create_thing_type(
    thingTypeName,
    thingTypeDescription,
    searchableAttributesList,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Create a thing type.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_iot.create_thing_type

    """
    try:
        conn = _get_conn("iot", region, key, keyid, profile)
        thingTypeProperties = {
            "thingTypeDescription": thingTypeDescription,
            "searchableAttributes": searchableAttributesList,
        }
        thingtype = conn.create_thing_type(
            thingTypeName=thingTypeName, thingTypeProperties=thingTypeProperties
        )
        if thingtype:
            log.info("The newly created thing type ARN is %s", thingtype["thingTypeArn"])
            return {"created": True, "thingTypeArn": thingtype["thingTypeArn"]}
        log.warning("thing type was not created")
        return {"created": False}
    except ClientError as e:
        return {"created": False, "error": boto3mod.get_error(e)}


def deprecate_thing_type(
    thingTypeName, undoDeprecate=False, region=None, key=None, keyid=None, profile=None
):
    """
    Deprecate or undeprecate the given thing type.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_iot.deprecate_thing_type

    """
    try:
        conn = _get_conn("iot", region, key, keyid, profile)
        conn.deprecate_thing_type(thingTypeName=thingTypeName, undoDeprecate=undoDeprecate)
        return {"deprecated": undoDeprecate is False}
    except ClientError as e:
        return {"deprecated": False, "error": boto3mod.get_error(e)}


def delete_thing_type(thingTypeName, region=None, key=None, keyid=None, profile=None):
    """
    Delete the given thing type.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_iot.delete_thing_type

    """
    try:
        conn = _get_conn("iot", region, key, keyid, profile)
        conn.delete_thing_type(thingTypeName=thingTypeName)
        return {"deleted": True}
    except ClientError as e:
        err = boto3mod.get_error(e)
        if e.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
            return {"deleted": True}
        return {"deleted": False, "error": err}


def policy_exists(policyName, region=None, key=None, keyid=None, profile=None):
    """
    Check to see if the given policy exists.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_iot.policy_exists

    """
    try:
        conn = _get_conn("iot", region, key, keyid, profile)
        conn.get_policy(policyName=policyName)
        return {"exists": True}
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
            return {"exists": False}
        return {"error": boto3mod.get_error(e)}


def create_policy(policyName, policyDocument, region=None, key=None, keyid=None, profile=None):
    """
    Create a policy.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_iot.create_policy

    """
    try:
        conn = _get_conn("iot", region, key, keyid, profile)
        if not isinstance(policyDocument, str):
            policyDocument = salt.utils.json.dumps(policyDocument)
        policy = conn.create_policy(policyName=policyName, policyDocument=policyDocument)
        if policy:
            log.info("The newly created policy version is %s", policy["policyVersionId"])
            return {"created": True, "versionId": policy["policyVersionId"]}
        log.warning("Policy was not created")
        return {"created": False}
    except ClientError as e:
        return {"created": False, "error": boto3mod.get_error(e)}


def delete_policy(policyName, region=None, key=None, keyid=None, profile=None):
    """
    Delete the given policy.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_iot.delete_policy

    """
    try:
        conn = _get_conn("iot", region, key, keyid, profile)
        conn.delete_policy(policyName=policyName)
        return {"deleted": True}
    except ClientError as e:
        return {"deleted": False, "error": boto3mod.get_error(e)}


def describe_policy(policyName, region=None, key=None, keyid=None, profile=None):
    """
    Describe the given policy.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_iot.describe_policy

    """
    try:
        conn = _get_conn("iot", region, key, keyid, profile)
        policy = conn.get_policy(policyName=policyName)
        if policy:
            keys = ("policyName", "policyArn", "policyDocument", "defaultVersionId")
            return {"policy": {k: policy.get(k) for k in keys}}
        return {"policy": None}
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
            return {"policy": None}
        return {"error": boto3mod.get_error(e)}


def policy_version_exists(
    policyName, policyVersionId, region=None, key=None, keyid=None, profile=None
):
    """
    Check to see if the given policy version exists.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_iot.policy_version_exists

    """
    try:
        conn = _get_conn("iot", region, key, keyid, profile)
        policy = conn.get_policy_version(policyName=policyName, policyVersionId=policyVersionId)
        return {"exists": bool(policy)}
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
            return {"exists": False}
        return {"error": boto3mod.get_error(e)}


def create_policy_version(
    policyName,
    policyDocument,
    setAsDefault=False,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Create a new version of a policy.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_iot.create_policy_version

    """
    try:
        conn = _get_conn("iot", region, key, keyid, profile)
        if not isinstance(policyDocument, str):
            policyDocument = salt.utils.json.dumps(policyDocument)
        policy = conn.create_policy_version(
            policyName=policyName,
            policyDocument=policyDocument,
            setAsDefault=setAsDefault,
        )
        if policy:
            log.info("The newly created policy version is %s", policy["policyVersionId"])
            return {"created": True, "name": policy["policyVersionId"]}
        log.warning("Policy version was not created")
        return {"created": False}
    except ClientError as e:
        return {"created": False, "error": boto3mod.get_error(e)}


def delete_policy_version(
    policyName, policyVersionId, region=None, key=None, keyid=None, profile=None
):
    """
    Delete the given policy version.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_iot.delete_policy_version

    """
    try:
        conn = _get_conn("iot", region, key, keyid, profile)
        conn.delete_policy_version(policyName=policyName, policyVersionId=policyVersionId)
        return {"deleted": True}
    except ClientError as e:
        return {"deleted": False, "error": boto3mod.get_error(e)}


def describe_policy_version(
    policyName, policyVersionId, region=None, key=None, keyid=None, profile=None
):
    """
    Describe the given policy version.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_iot.describe_policy_version

    """
    try:
        conn = _get_conn("iot", region, key, keyid, profile)
        policy = conn.get_policy_version(policyName=policyName, policyVersionId=policyVersionId)
        if policy:
            keys = (
                "policyName",
                "policyArn",
                "policyDocument",
                "policyVersionId",
                "isDefaultVersion",
            )
            return {"policy": {k: policy.get(k) for k in keys}}
        return {"policy": None}
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
            return {"policy": None}
        return {"error": boto3mod.get_error(e)}


def list_policies(region=None, key=None, keyid=None, profile=None):
    """
    List all policies.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_iot.list_policies

    """
    try:
        conn = _get_conn("iot", region, key, keyid, profile)
        policies = []
        for ret in boto3mod.paged_call(
            conn.list_policies, marker_flag="nextMarker", marker_arg="marker"
        ):
            policies.extend(ret.get("policies", []))
        if not policies:
            log.warning("No policies found")
        return {"policies": policies}
    except ClientError as e:
        return {"error": boto3mod.get_error(e)}


def list_policy_versions(policyName, region=None, key=None, keyid=None, profile=None):
    """
    List the versions available for the given policy.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_iot.list_policy_versions

    """
    try:
        conn = _get_conn("iot", region, key, keyid, profile)
        vers = []
        for ret in boto3mod.paged_call(
            conn.list_policy_versions,
            marker_flag="nextMarker",
            marker_arg="marker",
            policyName=policyName,
        ):
            vers.extend(ret.get("policyVersions", []))
        if not vers:
            log.warning("No versions found")
        return {"policyVersions": vers}
    except ClientError as e:
        return {"error": boto3mod.get_error(e)}


def set_default_policy_version(
    policyName, policyVersionId, region=None, key=None, keyid=None, profile=None
):
    """
    Set the given version as the default for the policy.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_iot.set_default_policy_version

    """
    try:
        conn = _get_conn("iot", region, key, keyid, profile)
        conn.set_default_policy_version(policyName=policyName, policyVersionId=str(policyVersionId))
        return {"changed": True}
    except ClientError as e:
        return {"changed": False, "error": boto3mod.get_error(e)}


def list_principal_policies(principal, region=None, key=None, keyid=None, profile=None):
    """
    List the policies attached to the given principal.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_iot.list_principal_policies

    """
    try:
        conn = _get_conn("iot", region, key, keyid, profile)
        vers = []
        for ret in boto3mod.paged_call(
            conn.list_principal_policies,
            principal=principal,
            marker_flag="nextMarker",
            marker_arg="marker",
        ):
            vers.extend(ret.get("policies", []))
        if not vers:
            log.warning("No policies found")
        return {"policies": vers}
    except ClientError as e:
        return {"error": boto3mod.get_error(e)}


def attach_principal_policy(policyName, principal, region=None, key=None, keyid=None, profile=None):
    """
    Attach the specified policy to the specified principal.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_iot.attach_principal_policy

    """
    try:
        conn = _get_conn("iot", region, key, keyid, profile)
        conn.attach_principal_policy(policyName=policyName, principal=principal)
        return {"attached": True}
    except ClientError as e:
        return {"attached": False, "error": boto3mod.get_error(e)}


def detach_principal_policy(policyName, principal, region=None, key=None, keyid=None, profile=None):
    """
    Detach the specified policy from the specified principal.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_iot.detach_principal_policy

    """
    try:
        conn = _get_conn("iot", region, key, keyid, profile)
        conn.detach_principal_policy(policyName=policyName, principal=principal)
        return {"detached": True}
    except ClientError as e:
        return {"detached": False, "error": boto3mod.get_error(e)}


def topic_rule_exists(ruleName, region=None, key=None, keyid=None, profile=None):
    """
    Check to see if the given rule exists.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_iot.topic_rule_exists

    """
    try:
        conn = _get_conn("iot", region, key, keyid, profile)
        conn.get_topic_rule(ruleName=ruleName)
        return {"exists": True}
    except ClientError as e:
        # Nonexistent rules show up as unauthorized exceptions. It's unclear how
        # to distinguish this from a real authorization exception. In practical
        # use, it's more useful to assume lack of existence than to assume a
        # genuine authorization problem; authorization problems should not be
        # the common case.
        if e.response.get("Error", {}).get("Code") == "UnauthorizedException":
            return {"exists": False}
        return {"error": boto3mod.get_error(e)}


def create_topic_rule(
    ruleName,
    sql,
    actions,
    description,
    ruleDisabled=False,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Create a topic rule.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_iot.create_topic_rule

    """
    try:
        conn = _get_conn("iot", region, key, keyid, profile)
        conn.create_topic_rule(
            ruleName=ruleName,
            topicRulePayload={
                "sql": sql,
                "description": description,
                "actions": actions,
                "ruleDisabled": ruleDisabled,
            },
        )
        return {"created": True}
    except ClientError as e:
        return {"created": False, "error": boto3mod.get_error(e)}


def replace_topic_rule(
    ruleName,
    sql,
    actions,
    description,
    ruleDisabled=False,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Replace a topic rule with the new values.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_iot.replace_topic_rule

    """
    try:
        conn = _get_conn("iot", region, key, keyid, profile)
        conn.replace_topic_rule(
            ruleName=ruleName,
            topicRulePayload={
                "sql": sql,
                "description": description,
                "actions": actions,
                "ruleDisabled": ruleDisabled,
            },
        )
        return {"replaced": True}
    except ClientError as e:
        return {"replaced": False, "error": boto3mod.get_error(e)}


def delete_topic_rule(ruleName, region=None, key=None, keyid=None, profile=None):
    """
    Delete the given topic rule.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_iot.delete_topic_rule

    """
    try:
        conn = _get_conn("iot", region, key, keyid, profile)
        conn.delete_topic_rule(ruleName=ruleName)
        return {"deleted": True}
    except ClientError as e:
        return {"deleted": False, "error": boto3mod.get_error(e)}


def describe_topic_rule(ruleName, region=None, key=None, keyid=None, profile=None):
    """
    Describe the given topic rule.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_iot.describe_topic_rule

    """
    try:
        conn = _get_conn("iot", region, key, keyid, profile)
        rule = conn.get_topic_rule(ruleName=ruleName)
        if rule and "rule" in rule:
            rule = rule["rule"]
            keys = ("ruleName", "sql", "description", "actions", "ruleDisabled")
            return {"rule": {k: rule.get(k) for k in keys}}
        return {"rule": None}
    except ClientError as e:
        return {"error": boto3mod.get_error(e)}


def list_topic_rules(
    topic=None, ruleDisabled=None, region=None, key=None, keyid=None, profile=None
):
    """
    List all rules (for a given topic, if specified).

    CLI Example:

    .. code-block:: bash

        salt-call boto3_iot.list_topic_rules

    """
    try:
        conn = _get_conn("iot", region, key, keyid, profile)
        kwargs = {}
        if topic is not None:
            kwargs["topic"] = topic
        if ruleDisabled is not None:
            kwargs["ruleDisabled"] = ruleDisabled
        rules = []
        for ret in boto3mod.paged_call(
            conn.list_topic_rules,
            marker_flag="nextToken",
            marker_arg="nextToken",
            **kwargs,
        ):
            rules.extend(ret.get("rules", []))
        if not rules:
            log.warning("No rules found")
        return {"rules": rules}
    except ClientError as e:
        return {"error": boto3mod.get_error(e)}
