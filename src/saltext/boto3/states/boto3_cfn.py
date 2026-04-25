"""
Manage CloudFormation stacks using boto3.
=========================================

    Renamed from ``boto_cfn`` to ``boto3_cfn`` and updated to call the
    refactored ``boto3_cfn`` execution module.

Create and destroy CloudFormation stacks. Be aware that this interacts with Amazon's
services, and so may incur charges.

:depends:
  - boto3 >= 1.28.0
  - botocore >= 1.31.0

This module uses boto3, which can be installed via package, or pip.

This module accepts explicit CloudFormation credentials but can also utilize
IAM roles assigned to the instance through Instance Profiles. Dynamic
credentials are then automatically obtained from AWS API and no further
configuration is necessary. More Information available at:

.. code-block:: text

    http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html

If IAM roles are not used you need to specify them either in the minion's config file
or as a profile. For example, to specify them in the minion's config file:

.. code-block:: yaml

    cfn.keyid: GKTADJGHEIQSXMKKRBJ08H
    cfn.key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs

It's also possible to specify key, keyid and region via a profile, either
as a passed in dict, or as a string to pull from pillars or minion config:

.. code-block:: yaml

    myprofile:
        keyid: GKTADJGHEIQSXMKKRBJ08H
        key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs
        region: us-east-1

.. code-block:: yaml

    stack-present:
      boto3_cfn.present:
        - name: mystack
        - template_body: salt://base/mytemplate.json
        - disable_rollback: true
        - region: eu-west-1
        - keyid: 'AKIAJHTMIQ2ASDFLASDF'
        - key: 'fdkjsafkljsASSADFalkfjasdf'

.. code-block:: yaml

    stack-absent:
      boto3_cfn.absent:
        - name: mystack

.. versionadded:: 1.0.0
"""

import logging

import salt.utils.compat
import salt.utils.json

log = logging.getLogger(__name__)

__virtualname__ = "boto3_cfn"


def __virtual__():
    """
    Only load if the boto3_cfn execution module is available.
    """
    if "boto3_cfn.exists" in __salt__:
        return __virtualname__
    return (
        False,
        f"Cannot load {__virtualname__} state: boto3_cfn module unavailable",
    )


def present(
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
    use_previous_template=None,
    stack_policy_during_update_body=None,
    stack_policy_during_update_url=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Ensure cloud formation stack is present.

    name (string) - Name of the stack.

    template_body (string) - Structure containing the template body. Can also be
    loaded from a file by using ``salt://``.

    template_url (string) - Location of file containing the template body. The
    URL must point to a template located in an S3 bucket in the same region as
    the stack.

    parameters (list) - A list of ``(key, value)`` tuples or
    ``{"ParameterKey": ..., "ParameterValue": ...}`` dicts that specify input
    parameters for the stack. A 3-tuple ``(key, value, use_previous_value)``
    may be used to specify the ``UsePreviousValue`` option.

    notification_arns (list) - The Simple Notification Service (SNS) topic
    ARNs to publish stack related events.

    disable_rollback (bool) - Indicates whether or not to rollback on failure.

    timeout_in_minutes (integer) - The amount of time that can pass before the
    stack status becomes ``CREATE_FAILED``.

    capabilities (list) - The list of capabilities you want to allow in the
    stack.

    tags (dict or list) - Tags to associate with this stack. A dict is
    converted to the boto3 ``[{"Key": ..., "Value": ...}]`` form.

    on_failure (string) - One of ``DO_NOTHING``, ``ROLLBACK``, or ``DELETE``.

    stack_policy_body (string) - Structure containing the stack policy body.
    Can also be loaded from a file by using ``salt://``.

    stack_policy_url (string) - Location of a file containing the stack policy.

    use_previous_template (boolean) - Set to True to use the previous template
    instead of uploading a new one via ``template_body`` or ``template_url``.

    stack_policy_during_update_body (string) - Temporary overriding stack
    policy body used during an update. Can also be loaded from a file by using
    ``salt://``.

    stack_policy_during_update_url (string) - Location of a file containing
    the temporary overriding stack policy.

    region (string) - Region to connect to.

    key (string) - Secret key to be used.

    keyid (string) - Access key to be used.

    profile (dict) - A dict with region, key and keyid, or a pillar key
    (string) that contains a dict with region, key and keyid.

    Example:

    .. code-block:: yaml

        ensure-present:
          boto3_cfn.present:
            - name: example

    """
    ret = {"name": name, "result": True, "comment": "", "changes": {}}

    template_body = _get_template(template_body, name)
    stack_policy_body = _get_template(stack_policy_body, name)
    stack_policy_during_update_body = _get_template(stack_policy_during_update_body, name)

    for i in [template_body, stack_policy_body, stack_policy_during_update_body]:
        if isinstance(i, dict):
            return i

    _valid = _validate(template_body, template_url, region, key, keyid, profile)
    log.debug("Validate is : %s.", _valid)
    if _valid is not True:
        ret["result"] = False
        ret["comment"] = f"Template could not be validated.\n{_valid}"
        return ret
    log.debug("Template %s is valid.", name)
    if __salt__["boto3_cfn.exists"](name, region, key, keyid, profile):
        template = __salt__["boto3_cfn.get_template"](name, region, key, keyid, profile)
        if isinstance(template, str):
            ret["result"] = False
            ret["comment"] = f"Could not retrieve stack template.\n{template}"
            return ret
        current_body = template["TemplateBody"]
        if isinstance(current_body, str):
            current_body = salt.utils.json.loads(current_body)
        _template_body = salt.utils.json.loads(template_body)
        compare = salt.utils.compat.cmp(current_body, _template_body)
        if compare != 0:
            log.debug("Templates are not the same. Compare value is %s", compare)
            if __opts__["test"]:
                ret["comment"] = f"Stack {name} is set to be updated."
                ret["result"] = None
                return ret
            updated = __salt__["boto3_cfn.update_stack"](
                name,
                template_body,
                template_url,
                parameters,
                notification_arns,
                disable_rollback,
                timeout_in_minutes,
                capabilities,
                tags,
                use_previous_template,
                stack_policy_during_update_body,
                stack_policy_during_update_url,
                stack_policy_body,
                stack_policy_url,
                region,
                key,
                keyid,
                profile,
            )
            if isinstance(updated, str):
                ret["result"] = False
                ret["comment"] = f"Stack {name} could not be updated.\n{updated}"
                return ret
            ret["comment"] = f"Cloud formation template {name} has been updated."
            ret["changes"]["new"] = updated
            return ret
        ret["comment"] = f"Stack {name} exists."
        ret["changes"] = {}
        return ret
    if __opts__["test"]:
        ret["comment"] = f"Stack {name} is set to be created."
        ret["result"] = None
        return ret
    created = __salt__["boto3_cfn.create"](
        name,
        template_body,
        template_url,
        parameters,
        notification_arns,
        disable_rollback,
        timeout_in_minutes,
        capabilities,
        tags,
        on_failure,
        stack_policy_body,
        stack_policy_url,
        region,
        key,
        keyid,
        profile,
    )
    if created:
        ret["comment"] = f"Stack {name} was created."
        ret["changes"]["new"] = created
        return ret
    ret["result"] = False
    return ret


def absent(name, region=None, key=None, keyid=None, profile=None):
    """
    Ensure cloud formation stack is absent.

    name (string) - The name of the stack to delete.

    region (string) - Region to connect to.

    key (string) - Secret key to be used.

    keyid (string) - Access key to be used.

    profile (dict) - A dict with region, key and keyid, or a pillar key
    (string) that contains a dict with region, key and keyid.

    Example:

    .. code-block:: yaml

        ensure-absent:
          boto3_cfn.absent:
            - name: example

    """
    ret = {"name": name, "result": True, "comment": "", "changes": {}}
    if not __salt__["boto3_cfn.exists"](name, region, key, keyid, profile):
        ret["comment"] = f"Stack {name} does not exist."
        ret["changes"] = {}
        return ret
    if __opts__["test"]:
        ret["comment"] = f"Stack {name} is set to be deleted."
        ret["result"] = None
        return ret
    deleted = __salt__["boto3_cfn.delete"](name, region, key, keyid, profile)
    if isinstance(deleted, str):
        ret["comment"] = f"Stack {name} could not be deleted.\n{deleted}"
        ret["result"] = False
        ret["changes"] = {}
        return ret
    if deleted:
        ret["comment"] = f"Stack {name} was deleted."
        ret["changes"]["deleted"] = name
        return ret
    ret["result"] = False
    ret["comment"] = f"Stack {name} could not be deleted."
    return ret


def _get_template(template, name):
    # Checks if template is a file in salt defined by salt://.
    ret = {"name": name, "result": True, "comment": "", "changes": {}}
    if template is not None and "salt://" in template:
        try:
            return __salt__["cp.get_file_str"](template)
        except OSError as e:
            log.debug(e)
            ret["comment"] = f"File {template} not found."
            ret["result"] = False
            return ret
    return template


def _validate(
    template_body=None,
    template_url=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    # Validates template. Returns True if syntax is correct.
    validate = __salt__["boto3_cfn.validate_template"](
        template_body, template_url, region, key, keyid, profile
    )
    log.debug("Validate result is %s.", validate)
    if isinstance(validate, str):
        return validate
    return True
