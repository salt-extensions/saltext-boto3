"""
Connection module for Amazon Lambda using boto3.
================================================

    Renamed from ``boto_lambda`` to ``boto3_lambda`` and rewritten to use the
    boto3 ``lambda`` client APIs directly via
    :py:mod:`saltext.boto3.utils.boto3mod`.  The legacy boto2 code path
    (object-style access, retry loops) has been removed.

:depends:
  - boto3 >= 1.28.0
  - botocore >= 1.31.0

:configuration: This module accepts explicit lambda credentials but can
    also utilize IAM roles assigned to the instance through Instance Profiles.
    Dynamic credentials are then automatically obtained from AWS API and no
    further configuration is necessary. More Information available at:

    .. code-block:: text

        http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html

    If IAM roles are not used you need to specify them either in the minion's
    config file or as a profile. For example, to specify them in the minion's
    config file:

.. code-block:: yaml

    lambda.keyid: GKTADJGHEIQSXMKKRBJ08H
    lambda.key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs

A region may also be specified in the configuration:

.. code-block:: yaml

    lambda.region: us-east-1

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
import random
import time

import salt.utils.files
import salt.utils.json
from salt.exceptions import SaltInvocationError

from saltext.boto3.utils import boto3mod

log = logging.getLogger(__name__)

try:
    from botocore.exceptions import ClientError

    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

__virtualname__ = "boto3_lambda"

_FUNCTION_KEYS = (
    "FunctionName",
    "Runtime",
    "Role",
    "Handler",
    "CodeSha256",
    "CodeSize",
    "Description",
    "Timeout",
    "MemorySize",
    "FunctionArn",
    "LastModified",
    "VpcConfig",
    "Environment",
)

_ESM_KEYS = (
    "UUID",
    "BatchSize",
    "EventSourceArn",
    "FunctionArn",
    "LastModified",
    "LastProcessingResult",
    "State",
    "StateTransitionReason",
)


def __virtual__():
    """
    Only load if boto3 is available.
    """
    if HAS_BOTO3:
        return __virtualname__
    return (False, "The boto3_lambda module could not be loaded: boto3 is not available.")


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


def _filedata(infile):
    with salt.utils.files.fopen(infile, "rb") as f:
        return f.read()


def _get_role_arn(name, region=None, key=None, keyid=None, profile=None):
    if name.startswith("arn:aws:iam:"):
        return name
    account_id = __salt__["boto3_iam.get_account_id"](
        region=region, key=key, keyid=keyid, profile=profile
    )
    return f"arn:aws:iam::{account_id}:role/{name}"


def _resolve_vpcconfig(conf, region=None, key=None, keyid=None, profile=None):
    if isinstance(conf, str):
        conf = salt.utils.json.loads(conf)
    if not conf:
        return None
    if not isinstance(conf, dict):
        raise SaltInvocationError("VpcConfig must be a dict.")
    sns = [
        __salt__["boto3_vpc.get_resource_id"](
            "subnet", s, region=region, key=key, keyid=keyid, profile=profile
        ).get("id")
        for s in conf.pop("SubnetNames", [])
    ]
    sgs = [
        __salt__["boto3_secgroup.get_group_id"](
            s, region=region, key=key, keyid=keyid, profile=profile
        )
        for s in conf.pop("SecurityGroupNames", [])
    ]
    conf.setdefault("SubnetIds", []).extend(sns)
    conf.setdefault("SecurityGroupIds", []).extend(sgs)
    return conf


def _find_function(name, region=None, key=None, keyid=None, profile=None):
    conn = _get_conn("lambda", region=region, key=key, keyid=keyid, profile=profile)
    for page in boto3mod.paged_call(conn.list_functions):
        for func in page.get("Functions", []):
            if func["FunctionName"] == name:
                return func
    return None


def function_exists(FunctionName, region=None, key=None, keyid=None, profile=None):
    """
    Check whether a Lambda function exists.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_lambda.function_exists myfunction
    """
    try:
        func = _find_function(FunctionName, region=region, key=key, keyid=keyid, profile=profile)
        return {"exists": bool(func)}
    except ClientError as e:
        return {"error": boto3mod.get_error(e)}


def create_function(
    FunctionName,
    Runtime,
    Role,
    Handler,
    ZipFile=None,
    S3Bucket=None,
    S3Key=None,
    S3ObjectVersion=None,
    Description="",
    Timeout=3,
    MemorySize=128,
    Publish=False,
    WaitForRole=False,
    RoleRetries=5,
    region=None,
    key=None,
    keyid=None,
    profile=None,
    VpcConfig=None,
    Environment=None,
):
    """
    Create a Lambda function.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_lambda.create_function my_function python3.9 my_role my_file.handler my_function.zip
    """
    role_arn = _get_role_arn(Role, region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn = _get_conn("lambda", region=region, key=key, keyid=keyid, profile=profile)
        if ZipFile:
            if S3Bucket or S3Key or S3ObjectVersion:
                raise SaltInvocationError(
                    "Either ZipFile must be specified, or S3Bucket and S3Key must be provided."
                )
            if "://" in ZipFile:
                dl = __salt__["cp.cache_file"](path=ZipFile)
                if dl is False:
                    return {
                        "created": False,
                        "error": {"message": f"Failed to cache ZipFile `{ZipFile}`."},
                    }
                ZipFile = dl
            code = {"ZipFile": _filedata(ZipFile)}
        else:
            if not S3Bucket or not S3Key:
                raise SaltInvocationError(
                    "Either ZipFile must be specified, or S3Bucket and S3Key must be provided."
                )
            code = {"S3Bucket": S3Bucket, "S3Key": S3Key}
            if S3ObjectVersion:
                code["S3ObjectVersion"] = S3ObjectVersion
        kwargs = {}
        if VpcConfig is not None:
            kwargs["VpcConfig"] = _resolve_vpcconfig(
                VpcConfig, region=region, key=key, keyid=keyid, profile=profile
            )
        if Environment is not None:
            kwargs["Environment"] = Environment
        retrycount = RoleRetries if WaitForRole else 1
        func = None
        for retry in range(retrycount, 0, -1):
            try:
                func = conn.create_function(
                    FunctionName=FunctionName,
                    Runtime=Runtime,
                    Role=role_arn,
                    Handler=Handler,
                    Code=code,
                    Description=Description,
                    Timeout=Timeout,
                    MemorySize=MemorySize,
                    Publish=Publish,
                    **kwargs,
                )
            except ClientError as e:
                code_name = e.response.get("Error", {}).get("Code")
                if retry > 1 and code_name == "InvalidParameterValueException":
                    log.info("Function not created; IAM role may not have propagated, will retry")
                    time.sleep((2 ** (RoleRetries - retry)) + (random.randint(0, 1000) / 1000))
                    continue
                raise
            break
        if func:
            log.info("The newly created function name is %s", func["FunctionName"])
            return {"created": True, "name": func["FunctionName"]}
        log.warning("Function was not created")
        return {"created": False}
    except ClientError as e:
        return {"created": False, "error": boto3mod.get_error(e)}


def delete_function(FunctionName, Qualifier=None, region=None, key=None, keyid=None, profile=None):
    """
    Delete a Lambda function.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_lambda.delete_function myfunction
    """
    try:
        conn = _get_conn("lambda", region=region, key=key, keyid=keyid, profile=profile)
        if Qualifier:
            conn.delete_function(FunctionName=FunctionName, Qualifier=Qualifier)
        else:
            conn.delete_function(FunctionName=FunctionName)
        return {"deleted": True}
    except ClientError as e:
        return {"deleted": False, "error": boto3mod.get_error(e)}


def describe_function(FunctionName, region=None, key=None, keyid=None, profile=None):
    """
    Describe the given Lambda function.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_lambda.describe_function myfunction
    """
    try:
        func = _find_function(FunctionName, region=region, key=key, keyid=keyid, profile=profile)
        if func:
            return {"function": {k: func.get(k) for k in _FUNCTION_KEYS}}
        return {"function": None}
    except ClientError as e:
        return {"error": boto3mod.get_error(e)}


def update_function_config(
    FunctionName,
    Role=None,
    Handler=None,
    Description=None,
    Timeout=None,
    MemorySize=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
    VpcConfig=None,
    WaitForRole=False,
    RoleRetries=5,
    Environment=None,
):
    """
    Update the named Lambda function configuration.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_lambda.update_function_config my_function Description="..."
    """
    args = {"FunctionName": FunctionName}
    options = {
        "Handler": Handler,
        "Description": Description,
        "Timeout": Timeout,
        "MemorySize": MemorySize,
        "VpcConfig": VpcConfig,
        "Environment": Environment,
    }
    conn = _get_conn("lambda", region=region, key=key, keyid=keyid, profile=profile)
    for val, var in options.items():
        if var:
            args[val] = var
    if Role:
        args["Role"] = _get_role_arn(Role, region, key, keyid, profile)
    if VpcConfig:
        args["VpcConfig"] = _resolve_vpcconfig(
            VpcConfig, region=region, key=key, keyid=keyid, profile=profile
        )
    try:
        retrycount = RoleRetries if WaitForRole else 1
        r = None
        for retry in range(retrycount, 0, -1):
            try:
                r = conn.update_function_configuration(**args)
            except ClientError as e:
                code_name = e.response.get("Error", {}).get("Code")
                if retry > 1 and code_name == "InvalidParameterValueException":
                    log.info("Function not updated; IAM role may not have propagated, will retry")
                    time.sleep((2 ** (RoleRetries - retry)) + (random.randint(0, 1000) / 1000))
                    continue
                raise
            break
        if r:
            return {"updated": True, "function": {k: r.get(k) for k in _FUNCTION_KEYS}}
        log.warning("Function was not updated")
        return {"updated": False}
    except ClientError as e:
        return {"updated": False, "error": boto3mod.get_error(e)}


def update_function_code(
    FunctionName,
    ZipFile=None,
    S3Bucket=None,
    S3Key=None,
    S3ObjectVersion=None,
    Publish=False,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Update the named Lambda function's code.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_lambda.update_function_code my_function ZipFile=function.zip
    """
    conn = _get_conn("lambda", region=region, key=key, keyid=keyid, profile=profile)
    try:
        if ZipFile:
            if S3Bucket or S3Key or S3ObjectVersion:
                raise SaltInvocationError(
                    "Either ZipFile must be specified, or S3Bucket and S3Key must be provided."
                )
            r = conn.update_function_code(
                FunctionName=FunctionName, ZipFile=_filedata(ZipFile), Publish=Publish
            )
        else:
            if not S3Bucket or not S3Key:
                raise SaltInvocationError(
                    "Either ZipFile must be specified, or S3Bucket and S3Key must be provided."
                )
            args = {"S3Bucket": S3Bucket, "S3Key": S3Key}
            if S3ObjectVersion:
                args["S3ObjectVersion"] = S3ObjectVersion
            r = conn.update_function_code(FunctionName=FunctionName, Publish=Publish, **args)
        if r:
            return {"updated": True, "function": {k: r.get(k) for k in _FUNCTION_KEYS}}
        log.warning("Function was not updated")
        return {"updated": False}
    except ClientError as e:
        return {"updated": False, "error": boto3mod.get_error(e)}


def add_permission(
    FunctionName,
    StatementId,
    Action,
    Principal,
    SourceArn=None,
    SourceAccount=None,
    Qualifier=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):  # pylint: disable=unused-argument
    """
    Add a permission to a Lambda function.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_lambda.add_permission my_function my_id "lambda:*" s3.amazonaws.com
    """
    try:
        conn = _get_conn("lambda", region=region, key=key, keyid=keyid, profile=profile)
        kwargs = {}
        for kname in ("SourceArn", "SourceAccount", "Qualifier"):
            if locals()[kname] is not None:
                kwargs[kname] = str(locals()[kname])
        conn.add_permission(
            FunctionName=FunctionName,
            StatementId=StatementId,
            Action=Action,
            Principal=str(Principal),
            **kwargs,
        )
        return {"updated": True}
    except ClientError as e:
        return {"updated": False, "error": boto3mod.get_error(e)}


def remove_permission(
    FunctionName,
    StatementId,
    Qualifier=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Remove a permission from a Lambda function.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_lambda.remove_permission my_function my_id
    """
    try:
        conn = _get_conn("lambda", region=region, key=key, keyid=keyid, profile=profile)
        kwargs = {}
        if Qualifier is not None:
            kwargs["Qualifier"] = Qualifier
        conn.remove_permission(FunctionName=FunctionName, StatementId=StatementId, **kwargs)
        return {"updated": True}
    except ClientError as e:
        return {"updated": False, "error": boto3mod.get_error(e)}


def get_permissions(FunctionName, Qualifier=None, region=None, key=None, keyid=None, profile=None):
    """
    Get resource permissions for the given Lambda function.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_lambda.get_permissions my_function
    """
    try:
        conn = _get_conn("lambda", region=region, key=key, keyid=keyid, profile=profile)
        kwargs = {}
        if Qualifier is not None:
            kwargs["Qualifier"] = Qualifier
        policy = conn.get_policy(FunctionName=FunctionName, **kwargs)
        policy = policy.get("Policy", {})
        if isinstance(policy, str):
            policy = salt.utils.json.loads(policy)
        if policy is None:
            policy = {}
        permissions = {}
        for statement in policy.get("Statement", []):
            condition = statement.get("Condition", {})
            principal = statement.get("Principal", {})
            if "AWS" in principal:
                principal = principal["AWS"].split(":")[4]
            else:
                principal = principal.get("Service")
            permission = {
                "Action": statement.get("Action"),
                "Principal": principal,
            }
            if "ArnLike" in condition:
                permission["SourceArn"] = condition["ArnLike"].get("AWS:SourceArn")
            if "StringEquals" in condition:
                permission["SourceAccount"] = condition["StringEquals"].get("AWS:SourceAccount")
            permissions[statement.get("Sid")] = permission
        return {"permissions": permissions}
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
            return {"permissions": None}
        return {"permissions": None, "error": boto3mod.get_error(e)}


def list_functions(region=None, key=None, keyid=None, profile=None):
    """
    List all Lambda functions visible in the current scope.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_lambda.list_functions
    """
    conn = _get_conn("lambda", region=region, key=key, keyid=keyid, profile=profile)
    ret = []
    for page in boto3mod.paged_call(conn.list_functions):
        ret.extend(page.get("Functions", []))
    return ret


def list_function_versions(FunctionName, region=None, key=None, keyid=None, profile=None):
    """
    List the versions available for the given function.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_lambda.list_function_versions myfunction
    """
    try:
        conn = _get_conn("lambda", region=region, key=key, keyid=keyid, profile=profile)
        vers = []
        for page in boto3mod.paged_call(conn.list_versions_by_function, FunctionName=FunctionName):
            vers.extend(page.get("Versions", []))
        if not vers:
            log.warning("No versions found")
        return {"Versions": vers}
    except ClientError as e:
        return {"error": boto3mod.get_error(e)}


def create_alias(
    FunctionName,
    Name,
    FunctionVersion,
    Description="",
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Create an alias for a Lambda function.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_lambda.create_alias my_function my_alias '$LATEST'
    """
    try:
        conn = _get_conn("lambda", region=region, key=key, keyid=keyid, profile=profile)
        alias = conn.create_alias(
            FunctionName=FunctionName,
            Name=Name,
            FunctionVersion=FunctionVersion,
            Description=Description,
        )
        if alias:
            log.info("The newly created alias name is %s", alias["Name"])
            return {"created": True, "name": alias["Name"]}
        log.warning("Alias was not created")
        return {"created": False}
    except ClientError as e:
        return {"created": False, "error": boto3mod.get_error(e)}


def delete_alias(FunctionName, Name, region=None, key=None, keyid=None, profile=None):
    """
    Delete an alias.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_lambda.delete_alias myfunction myalias
    """
    try:
        conn = _get_conn("lambda", region=region, key=key, keyid=keyid, profile=profile)
        conn.delete_alias(FunctionName=FunctionName, Name=Name)
        return {"deleted": True}
    except ClientError as e:
        return {"deleted": False, "error": boto3mod.get_error(e)}


def _find_alias(
    FunctionName,
    Name,
    FunctionVersion=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    conn = _get_conn("lambda", region=region, key=key, keyid=keyid, profile=profile)
    args = {"FunctionName": FunctionName}
    if FunctionVersion:
        args["FunctionVersion"] = FunctionVersion
    for page in boto3mod.paged_call(conn.list_aliases, **args):
        for alias in page.get("Aliases", []):
            if alias["Name"] == Name:
                return alias
    return None


def alias_exists(FunctionName, Name, region=None, key=None, keyid=None, profile=None):
    """
    Check whether a Lambda alias exists.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_lambda.alias_exists myfunction myalias
    """
    try:
        alias = _find_alias(
            FunctionName, Name, region=region, key=key, keyid=keyid, profile=profile
        )
        return {"exists": bool(alias)}
    except ClientError as e:
        return {"error": boto3mod.get_error(e)}


def describe_alias(FunctionName, Name, region=None, key=None, keyid=None, profile=None):
    """
    Describe a Lambda alias.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_lambda.describe_alias myfunction myalias
    """
    try:
        alias = _find_alias(
            FunctionName, Name, region=region, key=key, keyid=keyid, profile=profile
        )
        if alias:
            keys = ("AliasArn", "Name", "FunctionVersion", "Description")
            return {"alias": {k: alias.get(k) for k in keys}}
        return {"alias": None}
    except ClientError as e:
        return {"error": boto3mod.get_error(e)}


def update_alias(
    FunctionName,
    Name,
    FunctionVersion=None,
    Description=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Update a Lambda alias.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_lambda.update_alias my_lambda my_alias '$LATEST'
    """
    try:
        conn = _get_conn("lambda", region=region, key=key, keyid=keyid, profile=profile)
        args = {}
        if FunctionVersion:
            args["FunctionVersion"] = FunctionVersion
        if Description:
            args["Description"] = Description
        r = conn.update_alias(FunctionName=FunctionName, Name=Name, **args)
        if r:
            keys = ("Name", "FunctionVersion", "Description")
            return {"updated": True, "alias": {k: r.get(k) for k in keys}}
        log.warning("Alias was not updated")
        return {"updated": False}
    except ClientError as e:
        return {"updated": False, "error": boto3mod.get_error(e)}


def create_event_source_mapping(
    EventSourceArn,
    FunctionName,
    StartingPosition,
    Enabled=True,
    BatchSize=100,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Create an event source mapping.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_lambda.create_event_source_mapping arn::::eventsource myfunction LATEST
    """
    try:
        conn = _get_conn("lambda", region=region, key=key, keyid=keyid, profile=profile)
        obj = conn.create_event_source_mapping(
            EventSourceArn=EventSourceArn,
            FunctionName=FunctionName,
            Enabled=Enabled,
            BatchSize=BatchSize,
            StartingPosition=StartingPosition,
        )
        if obj:
            log.info("The newly created event source mapping ID is %s", obj["UUID"])
            return {"created": True, "id": obj["UUID"]}
        log.warning("Event source mapping was not created")
        return {"created": False}
    except ClientError as e:
        return {"created": False, "error": boto3mod.get_error(e)}


def get_event_source_mapping_ids(
    EventSourceArn, FunctionName, region=None, key=None, keyid=None, profile=None
):
    """
    Given an event source and function name, return a list of mapping IDs.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_lambda.get_event_source_mapping_ids arn:::: myfunction
    """
    conn = _get_conn("lambda", region=region, key=key, keyid=keyid, profile=profile)
    try:
        mappings = []
        for page in boto3mod.paged_call(
            conn.list_event_source_mappings,
            EventSourceArn=EventSourceArn,
            FunctionName=FunctionName,
        ):
            mappings.extend([m["UUID"] for m in page.get("EventSourceMappings", [])])
        return mappings
    except ClientError as e:
        return {"error": boto3mod.get_error(e)}


def _get_ids(
    UUID=None,
    EventSourceArn=None,
    FunctionName=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    if UUID:
        if EventSourceArn or FunctionName:
            raise SaltInvocationError(
                "Either UUID must be specified, or "
                "`EventSourceArn` and `FunctionName` must be provided."
            )
        return [UUID]
    if not EventSourceArn or not FunctionName:
        raise SaltInvocationError(
            "Either UUID must be specified, or "
            "`EventSourceArn` and `FunctionName` must be provided."
        )
    return get_event_source_mapping_ids(
        EventSourceArn=EventSourceArn,
        FunctionName=FunctionName,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )


def delete_event_source_mapping(
    UUID=None,
    EventSourceArn=None,
    FunctionName=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Delete an event source mapping.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_lambda.delete_event_source_mapping 260c423d-...
    """
    ids = _get_ids(UUID, EventSourceArn=EventSourceArn, FunctionName=FunctionName)
    try:
        conn = _get_conn("lambda", region=region, key=key, keyid=keyid, profile=profile)
        for mid in ids:
            conn.delete_event_source_mapping(UUID=mid)
        return {"deleted": True}
    except ClientError as e:
        return {"deleted": False, "error": boto3mod.get_error(e)}


def event_source_mapping_exists(
    UUID=None,
    EventSourceArn=None,
    FunctionName=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Check whether an event source mapping exists.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_lambda.event_source_mapping_exists uuid
    """
    desc = describe_event_source_mapping(
        UUID=UUID,
        EventSourceArn=EventSourceArn,
        FunctionName=FunctionName,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )
    if "error" in desc:
        return desc
    return {"exists": bool(desc.get("event_source_mapping"))}


def describe_event_source_mapping(
    UUID=None,
    EventSourceArn=None,
    FunctionName=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Describe an event source mapping.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_lambda.describe_event_source_mapping uuid
    """
    ids = _get_ids(UUID, EventSourceArn=EventSourceArn, FunctionName=FunctionName)
    if not ids:
        return {"event_source_mapping": None}
    UUID = ids[0]
    try:
        conn = _get_conn("lambda", region=region, key=key, keyid=keyid, profile=profile)
        desc = conn.get_event_source_mapping(UUID=UUID)
        if desc:
            return {"event_source_mapping": {k: desc.get(k) for k in _ESM_KEYS}}
        return {"event_source_mapping": None}
    except ClientError as e:
        return {"error": boto3mod.get_error(e)}


def update_event_source_mapping(
    UUID,
    FunctionName=None,
    Enabled=None,
    BatchSize=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Update an event source mapping.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_lambda.update_event_source_mapping uuid FunctionName=new_function
    """
    try:
        conn = _get_conn("lambda", region=region, key=key, keyid=keyid, profile=profile)
        args = {}
        if FunctionName is not None:
            args["FunctionName"] = FunctionName
        if Enabled is not None:
            args["Enabled"] = Enabled
        if BatchSize is not None:
            args["BatchSize"] = BatchSize
        r = conn.update_event_source_mapping(UUID=UUID, **args)
        if r:
            return {
                "updated": True,
                "event_source_mapping": {k: r.get(k) for k in _ESM_KEYS},
            }
        log.warning("Mapping was not updated")
        return {"updated": False}
    except ClientError as e:
        return {"updated": False, "error": boto3mod.get_error(e)}
