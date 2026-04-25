"""
Connection module for Amazon SSM using boto3.
=============================================

    Renamed from ``boto_ssm`` to ``boto3_ssm`` and rewritten to use the
    boto3 ``ssm`` client APIs directly via
    :py:mod:`saltext.boto3.utils.boto3mod`.  The legacy boto2 code path
    (object-style access, retry loops) has been removed.

:depends:
  - boto3 >= 1.28.0
  - botocore >= 1.31.0

:configuration: This module accepts explicit SSM credentials but can
    also utilize IAM roles assigned to the instance through Instance Profiles.
    Dynamic credentials are then automatically obtained from AWS API and no
    further configuration is necessary. More Information available at:

    .. code-block:: text

        http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html

    If IAM roles are not used you need to specify them either in the minion's
    config file or as a profile. For example, to specify them in the minion's
    config file:

.. code-block:: yaml

    ssm.keyid: GKTADJGHEIQSXMKKRBJ08H
    ssm.key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs

A region may also be specified in the configuration:

.. code-block:: yaml

    ssm.region: us-east-1

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

from salt.utils import json

from saltext.boto3.utils import boto3mod

log = logging.getLogger(__name__)

try:
    from botocore.exceptions import ClientError

    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

__virtualname__ = "boto3_ssm"


def __virtual__():
    """
    Only load if boto3 is available.
    """
    if HAS_BOTO3:
        return __virtualname__
    return (False, "The boto3_ssm module could not be loaded: boto3 is not available.")


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


def get_parameter(
    name,
    withdecryption=False,
    resp_json=False,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Retrieves a parameter from SSM Parameter Store.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ssm.get_parameter test-param withdecryption=True
    """
    conn = _get_conn("ssm", region=region, key=key, keyid=keyid, profile=profile)
    try:
        resp = conn.get_parameter(Name=name, WithDecryption=withdecryption)
    except conn.exceptions.ParameterNotFound:
        log.warning("get_parameter: Unable to locate name: %s", name)
        return False

    if resp_json:
        return json.loads(resp["Parameter"]["Value"])
    return resp["Parameter"]["Value"]


def put_parameter(
    Name,
    Value,
    Description=None,
    Type="String",
    KeyId=None,
    Overwrite=False,
    AllowedPattern=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Set a parameter in the SSM parameter store.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ssm.put_parameter test-param test_value Type=SecureString KeyId=alias/aws/ssm
    """
    conn = _get_conn("ssm", region=region, key=key, keyid=keyid, profile=profile)
    if Type not in ("String", "StringList", "SecureString"):
        raise AssertionError("Type needs to be String|StringList|SecureString")
    if Type == "SecureString" and not KeyId:
        raise AssertionError("Require KeyId with SecureString")

    boto_args = {}
    if Description:
        boto_args["Description"] = Description
    if KeyId:
        boto_args["KeyId"] = KeyId
    if AllowedPattern:
        boto_args["AllowedPattern"] = AllowedPattern

    try:
        resp = conn.put_parameter(
            Name=Name, Value=Value, Type=Type, Overwrite=Overwrite, **boto_args
        )
    except conn.exceptions.ParameterAlreadyExists:
        log.warning(
            "The parameter already exists. "
            "To overwrite this value, set the Overwrite option in the request to True"
        )
        return False
    return resp["Version"]


def delete_parameter(Name, region=None, key=None, keyid=None, profile=None):
    """
    Remove a parameter from the SSM parameter store.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_ssm.delete_parameter test-param
    """
    conn = _get_conn("ssm", region=region, key=key, keyid=keyid, profile=profile)
    try:
        resp = conn.delete_parameter(Name=Name)
    except conn.exceptions.ParameterNotFound:
        log.warning("delete_parameter: Unable to locate name: %s", Name)
        return False
    return resp["ResponseMetadata"]["HTTPStatusCode"] == 200


def _build_send_command_kwargs(
    targets,
    document_name,
    parameters=None,
    comment=None,
    timeout_seconds=None,
    output_s3_bucket_name=None,
    output_s3_key_prefix=None,
    max_concurrency=None,
    max_errors=None,
):
    if isinstance(targets, str):
        targets = [targets]
    if targets and all(isinstance(t, str) for t in targets):
        kwargs = {
            "InstanceIds": list(targets),
            "DocumentName": document_name,
        }
    else:
        kwargs = {
            "Targets": list(targets),
            "DocumentName": document_name,
        }
    if parameters:
        kwargs["Parameters"] = {
            k: (v if isinstance(v, list) else [str(v)]) for k, v in parameters.items()
        }
    if comment is not None:
        kwargs["Comment"] = comment
    if timeout_seconds is not None:
        kwargs["TimeoutSeconds"] = int(timeout_seconds)
    if output_s3_bucket_name:
        kwargs["OutputS3BucketName"] = output_s3_bucket_name
    if output_s3_key_prefix:
        kwargs["OutputS3KeyPrefix"] = output_s3_key_prefix
    if max_concurrency is not None:
        kwargs["MaxConcurrency"] = str(max_concurrency)
    if max_errors is not None:
        kwargs["MaxErrors"] = str(max_errors)
    return kwargs


def send_command(
    targets,
    document_name="AWS-RunShellScript",
    parameters=None,
    comment=None,
    timeout_seconds=None,
    output_s3_bucket_name=None,
    output_s3_key_prefix=None,
    max_concurrency=None,
    max_errors=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Invoke an SSM document against the given targets.

    targets
        Either a list of EC2 instance IDs (strings) or a list of Targets dicts
        (``[{"Key": "tag:Env", "Values": ["prod"]}]``). A single instance ID
        string is also accepted.
    document_name
        Name of the SSM document to run. Defaults to ``AWS-RunShellScript``.
    parameters
        Dict of parameters to pass to the document. Scalar values are wrapped in
        a single-element list automatically.
    comment
        Optional user-supplied comment.
    timeout_seconds
        How long (in seconds) the command can remain in ``Pending`` state.
    output_s3_bucket_name, output_s3_key_prefix
        Optional S3 location for command output.
    max_concurrency, max_errors
        Optional concurrency/error thresholds (pass a number or a percentage
        string such as ``"50%"``).

    Returns the ``Command`` dict from the API on success, or ``{"error": ...}``.

    CLI Example:

    .. code-block:: bash

        salt '*' boto3_ssm.send_command i-0123 parameters='{"commands": ["uptime"]}'
    """
    conn = _get_conn("ssm", region=region, key=key, keyid=keyid, profile=profile)
    kwargs = _build_send_command_kwargs(
        targets,
        document_name,
        parameters=parameters,
        comment=comment,
        timeout_seconds=timeout_seconds,
        output_s3_bucket_name=output_s3_bucket_name,
        output_s3_key_prefix=output_s3_key_prefix,
        max_concurrency=max_concurrency,
        max_errors=max_errors,
    )
    try:
        resp = conn.send_command(**kwargs)
        return resp.get("Command", {})
    except ClientError as e:
        return {"error": boto3mod.get_error(e)}


def run_shell_script(
    command,
    targets,
    comment=None,
    timeout_seconds=None,
    execution_timeout=None,
    output_s3_bucket_name=None,
    output_s3_key_prefix=None,
    max_concurrency=None,
    max_errors=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Run one or more shell commands on the given targets via the
    ``AWS-RunShellScript`` SSM document.

    command
        A single shell command string or a list of command strings.
    targets
        Either a list of EC2 instance IDs or a list of Targets dicts.
    execution_timeout
        Per-command execution timeout in seconds (document parameter
        ``executionTimeout``). Distinct from ``timeout_seconds`` which bounds
        only the ``Pending`` state.

    CLI Example:

    .. code-block:: bash

        salt '*' boto3_ssm.run_shell_script 'uptime' i-0123
    """
    if isinstance(command, str):
        command = [command]
    parameters = {"commands": list(command)}
    if execution_timeout is not None:
        parameters["executionTimeout"] = [str(int(execution_timeout))]
    return send_command(
        targets,
        document_name="AWS-RunShellScript",
        parameters=parameters,
        comment=comment,
        timeout_seconds=timeout_seconds,
        output_s3_bucket_name=output_s3_bucket_name,
        output_s3_key_prefix=output_s3_key_prefix,
        max_concurrency=max_concurrency,
        max_errors=max_errors,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )


def get_command_invocation(
    command_id, instance_id, region=None, key=None, keyid=None, profile=None
):
    """
    Fetch the result of a single Run Command invocation.

    command_id
        The Command ID returned by :py:func:`send_command`.
    instance_id
        The EC2 instance ID the command ran on.

    CLI Example:

    .. code-block:: bash

        salt '*' boto3_ssm.get_command_invocation abc123 i-0123
    """
    conn = _get_conn("ssm", region=region, key=key, keyid=keyid, profile=profile)
    try:
        return conn.get_command_invocation(CommandId=command_id, InstanceId=instance_id)
    except ClientError as e:
        return {"error": boto3mod.get_error(e)}


def list_command_invocations(
    command_id=None,
    instance_id=None,
    details=False,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    List Run Command invocations, optionally filtered by ``command_id`` or
    ``instance_id``. Set ``details=True`` to include command plugin output.

    CLI Example:

    .. code-block:: bash

        salt '*' boto3_ssm.list_command_invocations command_id=abc123 details=True
    """
    conn = _get_conn("ssm", region=region, key=key, keyid=keyid, profile=profile)
    kwargs = {"Details": bool(details)}
    if command_id:
        kwargs["CommandId"] = command_id
    if instance_id:
        kwargs["InstanceId"] = instance_id
    invocations = []
    try:
        paginator = conn.get_paginator("list_command_invocations")
        for page in paginator.paginate(**kwargs):
            invocations.extend(page.get("CommandInvocations", []))
    except ClientError as e:
        return {"error": boto3mod.get_error(e)}
    return invocations
