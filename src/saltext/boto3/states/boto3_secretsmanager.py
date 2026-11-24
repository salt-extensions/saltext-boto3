"""
Manage AWS Secrets Manager secrets with boto3.
==============================================

Create and optionally update secrets in AWS Secrets Manager. Secret values are
accepted through state data but are not included in state changes.

:depends:
    - boto3 >= 1.28.0
    - botocore >= 1.31.0

This state module uses the ``boto3_secretsmanager`` execution module and the
standard AWS credential and profile configuration supported by that module.

.. versionadded:: 1.1.0
"""

import json


def __virtual__():
    """Only load when the Secrets Manager execution module is available."""
    if "boto3_secretsmanager.exists" in __salt__:
        return "boto3_secretsmanager"
    return (
        False,
        "The boto3_secretsmanager state module requires the execution module.",
    )


def present(
    name,
    secret_string=None,
    secret_data=None,
    description=None,
    kms_key_id=None,
    tags=None,
    update=False,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Ensure a Secrets Manager secret exists, optionally updating its value.

    name (str):
        Name or ARN of the secret.

    secret_string (str):
        String value for the secret. Mutually exclusive with ``secret_data``.

    secret_data (dict):
        JSON-serializable data to serialize as the secret value. Mutually
        exclusive with ``secret_string``.

    description (str):
        Optional description for a newly created secret.

    kms_key_id (str):
        Optional KMS key ID or ARN used to encrypt a newly created secret.

    tags (dict):
        Optional mapping of tag names to tag values for a newly created secret.

    update (bool):
        If ``True``, store the requested value as a new version when the secret
        already exists. The default is ``False``.

    region (str):
        AWS region where the secret is stored.

    key (str):
        AWS access key ID.

    keyid (str):
        AWS secret access key.

    profile (str):
        AWS profile to use for the connection.

    Example:

    .. code-block:: yaml

        mysecret:
          boto3_secretsmanager.present:
            - secret_string: 'mysecretvalue'
            - description: 'My secret description'
            - kms_key_id: 'my-kms-key-id'
            - tags:
                Environment: 'production'
            - update: True
            - region: 'us-east-1'
            - key: 'my-access-key-id'
            - keyid: 'my-secret-access-key'
            - profile: 'my-aws-profile'
    """
    if secret_string is not None and secret_data is not None:
        raise ValueError("secret_string and secret_data are mutually exclusive")
    if secret_data is not None:
        secret_string = json.dumps(secret_data, separators=(",", ":"), sort_keys=True)
    if secret_string is None:
        raise ValueError("one of secret_string or secret_data is required")

    ret = {"name": name, "result": True, "comment": "", "changes": {}}
    existing = __salt__["boto3_secretsmanager.exists"](
        name, region=region, key=key, keyid=keyid, profile=profile
    )
    if existing.get("error"):
        ret["result"] = False
        ret["comment"] = existing["error"]
        return ret

    if not existing.get("exists"):
        if __opts__["test"]:
            ret["result"] = None
            ret["comment"] = f"Secret {name} would be created."
            return ret
        created = __salt__["boto3_secretsmanager.create"](
            name,
            secret_string,
            description=description,
            kms_key_id=kms_key_id,
            tags=tags,
            region=region,
            key=key,
            keyid=keyid,
            profile=profile,
        )
        if not created.get("created"):
            ret["result"] = False
            ret["comment"] = created.get("error", f"Failed to create secret {name}.")
            return ret
        ret["changes"] = {"created": True, "arn": created.get("arn")}
        ret["comment"] = f"Secret {name} created."
        return ret

    if update:
        if __opts__["test"]:
            ret["result"] = None
            ret["comment"] = f"Secret {name} would be updated."
            return ret
        updated = __salt__["boto3_secretsmanager.put"](
            name,
            secret_string,
            region=region,
            key=key,
            keyid=keyid,
            profile=profile,
        )
        if not updated.get("updated"):
            ret["result"] = False
            ret["comment"] = updated.get("error", f"Failed to update secret {name}.")
            return ret
        ret["changes"] = {"updated": True, "version_id": updated.get("version_id")}
        ret["comment"] = f"Secret {name} updated."
        return ret

    ret["comment"] = f"Secret {name} exists."
    return ret


def rotation_present(
    name,
    rotation_lambda_arn,
    schedule_expression=None,
    automatically_after_days=None,
    duration=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """Ensure Lambda-based automatic rotation is configured for a secret.

    This configures the schedule without immediately rotating the credential.
    Secrets Manager validates the rotation configuration by
    invoking the Lambda's ``testSecret`` step. This state does not configure
    service-managed rotation, such as an RDS-managed master password.

    Example:

    .. code-block:: yaml

        rotate_dso_app_password:
          boto3_secretsmanager.rotation_present:
            - name: dhs-dso-dev-rds-app
            - rotation_lambda_arn: arn:aws:lambda:us-east-1:123456789012:function:rotate-dso-rds
            - automatically_after_days: 90
    """
    ret = {"name": name, "changes": {}, "result": True, "comment": ""}
    if not rotation_lambda_arn:
        ret["result"] = False
        ret["comment"] = "rotation_lambda_arn is required for Lambda-based rotation"
        return ret
    if schedule_expression is not None and automatically_after_days is not None:
        ret["result"] = False
        ret["comment"] = "schedule_expression and automatically_after_days are mutually exclusive"
        return ret
    if schedule_expression is None and automatically_after_days is None:
        ret["result"] = False
        ret["comment"] = "a rotation schedule is required"
        return ret
    if duration is not None and schedule_expression is None:
        ret["result"] = False
        ret["comment"] = "duration requires schedule_expression"
        return ret
    if automatically_after_days is not None and automatically_after_days < 1:
        ret["result"] = False
        ret["comment"] = "automatically_after_days must be at least 1"
        return ret

    rotation_rules = {}
    if schedule_expression is not None:
        rotation_rules["ScheduleExpression"] = schedule_expression
    if automatically_after_days is not None:
        rotation_rules["AutomaticallyAfterDays"] = automatically_after_days
    if duration is not None:
        rotation_rules["Duration"] = duration
    current = __salt__["boto3_secretsmanager.get_rotation"](
        name, region=region, key=key, keyid=keyid, profile=profile
    )
    if current.get("error"):
        ret["result"] = False
        ret["comment"] = current["error"]
        return ret
    if not current.get("exists"):
        ret["result"] = False
        ret["comment"] = f"Secret {name} does not exist."
        return ret
    if current.get("owning_service"):
        ret["result"] = False
        ret["comment"] = (
            f"Secret {name} is managed by {current['owning_service']}; "
            "configure rotation through its managing service."
        )
        return ret

    current_rules = current.get("rotation_rules", {})
    if (
        current.get("rotation_enabled")
        and current.get("rotation_lambda_arn") == rotation_lambda_arn
        and current_rules == rotation_rules
    ):
        ret["comment"] = f"Rotation is configured for secret {name}."
        return ret

    if __opts__["test"]:
        ret["result"] = None
        ret["comment"] = f"Rotation would be configured for secret {name}."
        return ret

    configured = __salt__["boto3_secretsmanager.configure_rotation"](
        name,
        rotation_lambda_arn,
        schedule_expression=schedule_expression,
        automatically_after_days=automatically_after_days,
        duration=duration,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )
    if not configured.get("configured"):
        ret["result"] = False
        ret["comment"] = configured.get("error", f"Failed to configure rotation for {name}.")
        return ret

    ret["changes"] = {
        "rotation_enabled": True,
        "rotation_lambda_arn": rotation_lambda_arn,
        "rotation_rules": rotation_rules,
    }
    ret["comment"] = f"Rotation configured for secret {name}."
    return ret


def managed_rotation_present(
    name,
    schedule_expression=None,
    automatically_after_days=None,
    duration=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """Ensure a service-managed rotation schedule without a Lambda function.

    The secret must already have service-managed rotation enabled. This state
    updates its schedule only and never initiates an immediate rotation.

    name (str):
        Name or ARN of an existing service-managed secret.

    schedule_expression (str, optional):
        Secrets Manager ``rate()`` or ``cron()`` rotation schedule.

    automatically_after_days (int, optional):
        Number of days between rotations. Mutually exclusive with
        ``schedule_expression``.

    duration (str, optional):
        Rotation window duration, such as ``2h``. Requires
        ``schedule_expression``.

    region (str, optional):
        AWS region where the secret is stored.

    key (str, optional):
        AWS access key ID.

    keyid (str, optional):
        AWS secret access key.

    profile (str, optional):
        AWS profile to use for the connection.

    Example:

    .. code-block:: yaml

        rotate_dso_master_password_schedule:
          boto3_secretsmanager.managed_rotation_present:
            - name: dhs-dso-dev-rds-master
            - automatically_after_days: 90
    """
    ret = {"name": name, "changes": {}, "result": True, "comment": ""}
    if schedule_expression is not None and automatically_after_days is not None:
        ret["result"] = False
        ret["comment"] = "schedule_expression and automatically_after_days are mutually exclusive"
        return ret
    if schedule_expression is None and automatically_after_days is None:
        ret["result"] = False
        ret["comment"] = "a rotation schedule is required"
        return ret
    if duration is not None and schedule_expression is None:
        ret["result"] = False
        ret["comment"] = "duration requires schedule_expression"
        return ret
    if automatically_after_days is not None and automatically_after_days < 1:
        ret["result"] = False
        ret["comment"] = "automatically_after_days must be at least 1"
        return ret

    rotation_rules = {}
    if schedule_expression is not None:
        rotation_rules["ScheduleExpression"] = schedule_expression
    if automatically_after_days is not None:
        rotation_rules["AutomaticallyAfterDays"] = automatically_after_days
    if duration is not None:
        rotation_rules["Duration"] = duration

    current = __salt__["boto3_secretsmanager.get_rotation"](
        name, region=region, key=key, keyid=keyid, profile=profile
    )
    if current.get("error"):
        ret["result"] = False
        ret["comment"] = current["error"]
        return ret
    if not current.get("exists"):
        ret["result"] = False
        ret["comment"] = f"Secret {name} does not exist."
        return ret
    if not current.get("owning_service"):
        ret["result"] = False
        ret["comment"] = f"Secret {name} is not managed by an AWS service."
        return ret
    if not current.get("rotation_enabled"):
        if __opts__["test"]:
            ret["result"] = None
            ret["comment"] = f"Service-managed rotation would be configured for secret {name}."
            return ret
        ret["result"] = False
        ret["comment"] = f"Service-managed rotation is not enabled for secret {name}."
        return ret

    current_rules = current.get("rotation_rules", {})
    if current_rules == rotation_rules:
        ret["comment"] = f"Managed rotation is configured for secret {name}."
        return ret

    if __opts__["test"]:
        ret["result"] = None
        ret["comment"] = f"Managed rotation would be configured for secret {name}."
        return ret

    configured = __salt__["boto3_secretsmanager.configure_managed_rotation"](
        name,
        schedule_expression=schedule_expression,
        automatically_after_days=automatically_after_days,
        duration=duration,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )
    if not configured.get("configured"):
        ret["result"] = False
        ret["comment"] = configured.get("error", f"Failed to configure rotation for {name}.")
        return ret

    ret["changes"] = {"rotation_rules": rotation_rules}
    ret["comment"] = f"Managed rotation configured for secret {name}."
    return ret


def absent(
    name,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Ensure a Secrets Manager secret is absent.

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

    Example:

    .. code-block:: yaml

        mysecret:
          boto3_secretsmanager.absent:
            - region: 'us-east-1'
            - key: 'my-access-key-id'
            - keyid: 'my-secret-access-key'
            - profile: 'my-aws-profile'
    """
    ret = {"name": name, "changes": {}, "result": True, "comment": ""}

    existing = __salt__["boto3_secretsmanager.exists"](
        name, region=region, key=key, keyid=keyid, profile=profile
    )
    if existing.get("error"):
        ret["result"] = False
        ret["comment"] = existing["error"]
        return ret

    if not existing.get("exists"):
        ret["comment"] = f"Secret {name} does not exist."
        return ret

    if __opts__["test"]:
        ret["result"] = None
        ret["comment"] = f"Secret {name} would be deleted."
        return ret

    deleted = __salt__["boto3_secretsmanager.delete"](
        name, region=region, key=key, keyid=keyid, profile=profile
    )
    if not deleted.get("deleted"):
        ret["result"] = False
        ret["comment"] = deleted.get("error", f"Failed to delete secret {name}.")
        return ret

    ret["changes"] = {"deleted": True}
    ret["comment"] = f"Secret {name} deleted."
    return ret
