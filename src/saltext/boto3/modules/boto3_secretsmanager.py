"""
Connection module for AWS Secrets Manager using boto3.
=======================================================

Create, inspect, and update secrets in AWS Secrets Manager. Secret values are
never written to the log by this module.

:depends:
    - boto3 >= 1.28.0
    - botocore >= 1.31.0

This module accepts explicit AWS credentials but can also utilize IAM roles
assigned to the instance through Instance Profiles. Dynamic credentials are
then automatically obtained from AWS API and no further configuration is
necessary. More information is available at:

.. code-block:: text

        https://docs.aws.amazon.com/secretsmanager/latest/userguide/auth-and-access.html

If IAM roles are not used, specify credentials in the minion configuration or
through a profile:

.. code-block:: yaml

        aws.keyid: GKTADJGHEIQSXMKKRBJ08H
        aws.key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs
        aws.region: us-east-1

.. versionadded:: 1.1.0
"""

import logging

from salt.exceptions import SaltInvocationError

from saltext.boto3.utils import boto3mod

log = logging.getLogger(__name__)

try:
    import botocore.exceptions

    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

__virtualname__ = "boto3_secretsmanager"


def __virtual__():
    """Only load when boto3 is available."""
    if HAS_BOTO3:
        return __virtualname__
    return (False, "The boto3_secretsmanager module could not be loaded: boto3 is not available.")


def _get_conn(region=None, key=None, keyid=None, profile=None):
    """Return a Secrets Manager boto3 client."""
    return boto3mod.get_connection(
        "secretsmanager",
        opts=__opts__,
        context=__context__,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )


def _tags(tags):
    """Convert a Salt tag mapping to the AWS tag list format."""
    return [{"Key": key, "Value": value} for key, value in (tags or {}).items()]


def get(name, version_id=None, version_stage=None, region=None, key=None, keyid=None, profile=None):
    """
    Return a Secrets Manager secret value without logging its contents.

    name (str):
        Name or ARN of the secret.

    version_id (str):
        Optional version identifier to retrieve.

    version_stage (str):
        Optional staging label identifying the version to retrieve.

    region (str):
        AWS region where the secret is stored.

    key (str):
        AWS access key ID.

    keyid (str):
        AWS secret access key.

    profile (str):
        AWS profile to use for the connection.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_secretsmanager.get name=mysecret
    """
    args = {"SecretId": name}
    if version_id:
        args["VersionId"] = version_id
    if version_stage:
        args["VersionStage"] = version_stage

    try:
        response = _get_conn(region, key, keyid, profile).get_secret_value(**args)
    except botocore.exceptions.ClientError as exc:
        if exc.response["Error"].get("Code") == "ResourceNotFoundException":
            return {"exists": False}
        return {"error": boto3mod.get_error(exc)}

    return {
        "exists": True,
        "arn": response.get("ARN"),
        "name": response.get("Name"),
        "version_id": response.get("VersionId"),
        "version_stages": response.get("VersionStages", []),
        "secret_string": response.get("SecretString"),
        "secret_binary": response.get("SecretBinary"),
    }


def exists(name, region=None, key=None, keyid=None, profile=None):
    """
    Return whether a Secrets Manager secret exists without retrieving its value.

    name (str):
        Name or ARN of the secret.

    region (str):
        AWS region where the secret is stored.

    key (str):
        AWS access key ID.

    keyid (str):
        AWS secret access key.

    profile (str):
        AWS profile to use for the connection.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_secretsmanager.exists name=mysecret
    """
    try:
        _get_conn(region, key, keyid, profile).describe_secret(SecretId=name)
    except botocore.exceptions.ClientError as exc:
        if exc.response["Error"].get("Code") == "ResourceNotFoundException":
            return {"exists": False}
        return {"error": boto3mod.get_error(exc)}
    return {"exists": True}


def get_rotation(name, region=None, key=None, keyid=None, profile=None):
    """Return rotation metadata without retrieving a secret value.

    name (str):
        Name or ARN of the secret.

    region (str):
        AWS region where the secret is stored.

    key (str):
        AWS access key ID.

    keyid (str):
        AWS secret access key.

    profile (str):
        AWS profile to use for the connection.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_secretsmanager.get_rotation name=mysecret
    """
    try:
        response = _get_conn(region, key, keyid, profile).describe_secret(SecretId=name)
    except botocore.exceptions.ClientError as exc:
        if exc.response["Error"].get("Code") == "ResourceNotFoundException":
            return {"exists": False}
        return {"error": boto3mod.get_error(exc)}

    return {
        "exists": True,
        "rotation_enabled": response.get("RotationEnabled", False),
        "rotation_lambda_arn": response.get("RotationLambdaARN"),
        "rotation_rules": response.get("RotationRules") or {},
        "owning_service": response.get("OwningService"),
    }


def _build_rotation_rules(
    schedule_expression,
    automatically_after_days,
    duration,
    require_lambda_arn=False,
    rotation_lambda_arn=None,
):
    """Validate rotation schedule args and return a RotationRules dict.

    Raises SaltInvocationError on invalid input.
    """
    if require_lambda_arn and not rotation_lambda_arn:
        raise SaltInvocationError("rotation_lambda_arn is required for Lambda-based rotation")
    if schedule_expression is not None and automatically_after_days is not None:
        raise SaltInvocationError(
            "schedule_expression and automatically_after_days are mutually exclusive"
        )
    if schedule_expression is None and automatically_after_days is None:
        raise SaltInvocationError("a rotation schedule is required")
    if duration is not None and schedule_expression is None:
        raise SaltInvocationError("duration requires schedule_expression")
    if automatically_after_days is not None and automatically_after_days < 1:
        raise SaltInvocationError("automatically_after_days must be at least 1")

    rules = {}
    if schedule_expression is not None:
        rules["ScheduleExpression"] = schedule_expression
    if automatically_after_days is not None:
        rules["AutomaticallyAfterDays"] = automatically_after_days
    if duration is not None:
        rules["Duration"] = duration
    return rules


def configure_rotation(
    name,
    rotation_lambda_arn,
    schedule_expression=None,
    automatically_after_days=None,
    duration=None,
    rotate_immediately=False,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """Configure Lambda-based automatic rotation for an existing secret.

    ``rotate_immediately`` defaults to ``False`` so a state apply does not
    change the credential. Secrets Manager still invokes the Lambda's
    ``testSecret`` step to validate the scheduled-rotation configuration.

    name (str):
        Name or ARN of an existing secret.

    rotation_lambda_arn (str):
        ARN of the Lambda rotation function.

    schedule_expression (str, optional):
        Secrets Manager ``rate()`` or ``cron()`` rotation schedule.

    automatically_after_days (int, optional):
        Number of days between rotations. Mutually exclusive with
        ``schedule_expression``.

    duration (str, optional):
        Rotation window duration, such as ``2h``. Requires
        ``schedule_expression``.

    rotate_immediately (bool):
        Whether to start an actual rotation now. Defaults to ``False``.

    region (str, optional):
        AWS region where the secret is stored.

    key (str, optional):
        AWS access key ID.

    keyid (str, optional):
        AWS secret access key.

    profile (str, optional):
        AWS profile to use for the connection.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_secretsmanager.configure_rotation \\
            name=mysecret \\
            rotation_lambda_arn=arn:aws:lambda:us-east-1:123456789012:function:rotate \\
            automatically_after_days=90
    """
    rotation_rules = _build_rotation_rules(
        schedule_expression,
        automatically_after_days,
        duration,
        require_lambda_arn=True,
        rotation_lambda_arn=rotation_lambda_arn,
    )
    args = {
        "SecretId": name,
        "RotationLambdaARN": rotation_lambda_arn,
        "RotationRules": rotation_rules,
        "RotateImmediately": rotate_immediately,
    }
    try:
        response = _get_conn(region, key, keyid, profile).rotate_secret(**args)
    except botocore.exceptions.ClientError as exc:
        return {"error": boto3mod.get_error(exc)}

    return {
        "configured": True,
        "arn": response.get("ARN"),
        "name": response.get("Name"),
        "version_id": response.get("VersionId"),
    }


def configure_managed_rotation(
    name,
    schedule_expression=None,
    automatically_after_days=None,
    duration=None,
    rotate_immediately=False,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """Update the schedule for a secret that uses service-managed rotation.

    Unlike ``configure_rotation``, this omits ``RotationLambdaARN`` because
    services such as RDS perform the rotation themselves.

    name (str):
        Name or ARN of an existing secret.

    schedule_expression (str, optional):
        Secrets Manager ``rate()`` or ``cron()`` rotation schedule.

    automatically_after_days (int, optional):
        Number of days between rotations. Mutually exclusive with
        ``schedule_expression``.

    duration (str, optional):
        Rotation window duration, such as ``2h``. Requires
        ``schedule_expression``.

    rotate_immediately (bool):
        Whether to start an actual rotation now. Defaults to ``False``.

    region (str, optional):
        AWS region where the secret is stored.

    key (str, optional):
        AWS access key ID.

    keyid (str, optional):
        AWS secret access key.

    profile (str, optional):
        AWS profile to use for the connection.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_secretsmanager.configure_managed_rotation \\
            name=rds-master automatically_after_days=90
    """
    rotation_rules = _build_rotation_rules(schedule_expression, automatically_after_days, duration)
    try:
        response = _get_conn(region, key, keyid, profile).rotate_secret(
            SecretId=name,
            RotationRules=rotation_rules,
            RotateImmediately=rotate_immediately,
        )
    except botocore.exceptions.ClientError as exc:
        return {"error": boto3mod.get_error(exc)}

    return {
        "configured": True,
        "arn": response.get("ARN"),
        "name": response.get("Name"),
        "version_id": response.get("VersionId"),
    }


def create(
    name,
    secret_string,
    description=None,
    kms_key_id=None,
    tags=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Create a Secrets Manager secret with an initial value.

    name (str):
        Name of the secret.

    secret_string (str):
        Initial secret value. Do not include secret values in logs or state
        output.

    description (str):
        Optional description for the secret.

    kms_key_id (str):
        Optional KMS key ID or ARN used to encrypt the secret.

    tags (dict):
        Optional mapping of tag names to tag values.

    region (str):
        AWS region where the secret is stored.

    key (str):
        AWS access key ID.

    keyid (str):
        AWS secret access key.

    profile (str):
        AWS profile to use for the connection.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_secretsmanager.create name=mysecret secret_string='mysecretvalue'
    """
    args = {"Name": name, "SecretString": secret_string}
    if description:
        args["Description"] = description
    if kms_key_id:
        args["KmsKeyId"] = kms_key_id
    if tags:
        args["Tags"] = _tags(tags)

    try:
        response = _get_conn(region, key, keyid, profile).create_secret(**args)
    except botocore.exceptions.ClientError as exc:
        return {"error": boto3mod.get_error(exc)}

    return {"created": True, "arn": response.get("ARN"), "name": response.get("Name")}


def put(
    name,
    secret_string,
    version_stages=None,
    client_request_token=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Store a new value for an existing Secrets Manager secret.

    name (str):
        Name or ARN of the secret.

    secret_string (str):
        New secret value. Do not include secret values in logs or state
        output.

    version_stages (list):
        Optional staging labels to attach to the new version.

    client_request_token (str):
        Optional idempotency token for the request.

    region (str):
        AWS region where the secret is stored.

    key (str):
        AWS access key ID.

    keyid (str):
        AWS secret access key.

    profile (str):
        AWS profile to use for the connection.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_secretsmanager.put name=mysecret secret_string='mynewsecretvalue'
    """
    args = {"SecretId": name, "SecretString": secret_string}
    if version_stages:
        args["VersionStages"] = version_stages
    if client_request_token:
        args["ClientRequestToken"] = client_request_token

    try:
        response = _get_conn(region, key, keyid, profile).put_secret_value(**args)
    except botocore.exceptions.ClientError as exc:
        return {"error": boto3mod.get_error(exc)}

    return {
        "updated": True,
        "arn": response.get("ARN"),
        "name": response.get("Name"),
        "version_id": response.get("VersionId"),
        "version_stages": response.get("VersionStages", []),
    }
