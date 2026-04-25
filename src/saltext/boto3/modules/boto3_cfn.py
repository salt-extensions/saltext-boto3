"""
Connection module for Amazon Cloud Formation using boto3.
=========================================================

    Renamed from ``boto_cfn`` to ``boto3_cfn`` and rewritten to use the
    boto3 ``cloudformation`` client APIs directly via
    :py:mod:`saltext.boto3.utils.boto3mod`.  The legacy boto2 code path
    (object-style access, retry loops) has been removed.

:depends:
  - boto3 >= 1.28.0
  - botocore >= 1.31.0

:configuration: This module accepts explicit Cloud Formation credentials but can
    also utilize IAM roles assigned to the instance through Instance Profiles.
    Dynamic credentials are then automatically obtained from AWS API and no
    further configuration is necessary. More Information available at:

    .. code-block:: text

        http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html

    If IAM roles are not used you need to specify them either in the minion's
    config file or as a profile. For example, to specify them in the minion's
    config file:

.. code-block:: yaml

    cfn.keyid: GKTADJGHEIQSXMKKRBJ08H
    cfn.key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs

A region may also be specified in the configuration:

.. code-block:: yaml

    cfn.region: us-east-1

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

__virtualname__ = "boto3_cfn"


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
    return (False, "The boto3_cfn module could not be loaded: boto3 is not available.")


def _convert_parameters(parameters):
    """
    Accept legacy list-of-tuples ``[(key, value[, use_previous])]`` and return
    the boto3 list-of-dicts ``[{"ParameterKey": ..., "ParameterValue": ...}]``
    form. Items already in dict form are passed through unchanged.
    """
    if not parameters:
        return None
    converted = []
    for item in parameters:
        if isinstance(item, dict):
            converted.append(item)
            continue
        entry = {"ParameterKey": item[0], "ParameterValue": item[1]}
        if len(item) >= 3:
            entry["UsePreviousValue"] = bool(item[2])
        converted.append(entry)
    return converted


def _convert_tags(tags):
    """
    Accept legacy ``{key: value}`` tags and return the boto3
    ``[{"Key": ..., "Value": ...}]`` form. Lists are passed through unchanged.
    """
    if not tags:
        return None
    if isinstance(tags, dict):
        return [{"Key": k, "Value": v} for k, v in tags.items()]
    return tags


def exists(name, region=None, key=None, keyid=None, profile=None):
    """
    Check to see if a stack exists.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_cfn.exists mystack region=us-east-1
    """
    conn = _get_conn("cloudformation", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.describe_stacks(StackName=name)
        log.debug("Stack %s exists.", name)
        return True
    except ClientError:
        log.debug("boto3_cfn.exists raised an exception", exc_info=True)
        return False


def describe(name, region=None, key=None, keyid=None, profile=None):
    """
    Describe a stack.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_cfn.describe mystack region=us-east-1
    """
    conn = _get_conn("cloudformation", region=region, key=key, keyid=keyid, profile=profile)
    try:
        response = conn.describe_stacks(StackName=name)
        stacks = response.get("Stacks") or []
        if not stacks:
            log.debug("Stack %s not found.", name)
            return True
        stack = stacks[0]
        ret = {
            "stack_id": stack.get("StackId"),
            "description": stack.get("Description"),
            "stack_status": stack.get("StackStatus"),
            "stack_status_reason": stack.get("StackStatusReason"),
            "tags": stack.get("Tags"),
        }
        ret["outputs"] = {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}
        ret["parameters"] = {
            p["ParameterKey"]: p["ParameterValue"] for p in stack.get("Parameters", [])
        }
        return {"stack": ret}
    except ClientError as exc:
        log.warning("Could not describe stack %s.\n%s", name, exc)
        return False


def create(
    name,
    template_body=None,
    template_url=None,
    parameters=None,
    notification_arns=None,
    disable_rollback=None,
    timeout_in_minutes=None,
    capabilities=None,
    tags=None,
    on_failure=None,
    stack_policy_body=None,
    stack_policy_url=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Create a CFN stack.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_cfn.create mystack template_url='https://s3.amazonaws.com/bucket/template.cft' \
        region=us-east-1
    """
    conn = _get_conn("cloudformation", region=region, key=key, keyid=keyid, profile=profile)
    params = {"StackName": name}
    if template_body is not None:
        params["TemplateBody"] = template_body
    if template_url is not None:
        params["TemplateURL"] = template_url
    _params = _convert_parameters(parameters)
    if _params is not None:
        params["Parameters"] = _params
    if notification_arns is not None:
        params["NotificationARNs"] = notification_arns
    if disable_rollback is not None:
        params["DisableRollback"] = disable_rollback
    if timeout_in_minutes is not None:
        params["TimeoutInMinutes"] = timeout_in_minutes
    if capabilities is not None:
        params["Capabilities"] = capabilities
    _tags = _convert_tags(tags)
    if _tags is not None:
        params["Tags"] = _tags
    if on_failure is not None:
        params["OnFailure"] = on_failure
    if stack_policy_body is not None:
        params["StackPolicyBody"] = stack_policy_body
    if stack_policy_url is not None:
        params["StackPolicyURL"] = stack_policy_url

    try:
        return conn.create_stack(**params)
    except ClientError as exc:
        log.error("Failed to create stack %s.\n%s", name, exc)
        return False


def update_stack(
    name,
    template_body=None,
    template_url=None,
    parameters=None,
    notification_arns=None,
    capabilities=None,
    tags=None,
    use_previous_template=None,
    stack_policy_during_update_body=None,
    stack_policy_during_update_url=None,
    stack_policy_body=None,
    stack_policy_url=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Update a CFN stack.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_cfn.update_stack mystack template_url='https://s3.amazonaws.com/bucket/template.cft' \
        region=us-east-1
    """
    conn = _get_conn("cloudformation", region=region, key=key, keyid=keyid, profile=profile)
    params = {"StackName": name}
    if template_body is not None:
        params["TemplateBody"] = template_body
    if template_url is not None:
        params["TemplateURL"] = template_url
    _params = _convert_parameters(parameters)
    if _params is not None:
        params["Parameters"] = _params
    if notification_arns is not None:
        params["NotificationARNs"] = notification_arns
    if capabilities is not None:
        params["Capabilities"] = capabilities
    _tags = _convert_tags(tags)
    if _tags is not None:
        params["Tags"] = _tags
    if use_previous_template is not None:
        params["UsePreviousTemplate"] = use_previous_template
    if stack_policy_during_update_body is not None:
        params["StackPolicyDuringUpdateBody"] = stack_policy_during_update_body
    if stack_policy_during_update_url is not None:
        params["StackPolicyDuringUpdateURL"] = stack_policy_during_update_url
    if stack_policy_body is not None:
        params["StackPolicyBody"] = stack_policy_body
    if stack_policy_url is not None:
        params["StackPolicyURL"] = stack_policy_url

    try:
        update = conn.update_stack(**params)
        log.debug("Updated result is : %s.", update)
        return update
    except ClientError as exc:
        log.error("Failed to update stack %s.", name)
        log.debug(exc)
        return str(exc)


def delete(name, region=None, key=None, keyid=None, profile=None):
    """
    Delete a CFN stack.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_cfn.delete mystack region=us-east-1
    """
    conn = _get_conn("cloudformation", region=region, key=key, keyid=keyid, profile=profile)
    try:
        return conn.delete_stack(StackName=name)
    except ClientError as exc:
        log.error("Failed to delete stack %s.", name)
        log.debug(exc)
        return str(exc)


def get_template(name, region=None, key=None, keyid=None, profile=None):
    """
    Retrieve the template body of a CFN stack.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_cfn.get_template mystack
    """
    conn = _get_conn("cloudformation", region=region, key=key, keyid=keyid, profile=profile)
    try:
        template = conn.get_template(StackName=name)
        log.info("Retrieved template for stack %s", name)
        return template
    except ClientError as exc:
        log.debug(exc)
        log.error("Template %s does not exist", name)
        return str(exc)


def validate_template(
    template_body=None,
    template_url=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Validate cloudformation template.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_cfn.validate_template mystack-template
    """
    conn = _get_conn("cloudformation", region=region, key=key, keyid=keyid, profile=profile)
    params = {}
    if template_body is not None:
        params["TemplateBody"] = template_body
    if template_url is not None:
        params["TemplateURL"] = template_url
    try:
        return conn.validate_template(**params)
    except ClientError as exc:
        log.debug(exc)
        log.error("Error while trying to validate template.")
        return str(exc)
