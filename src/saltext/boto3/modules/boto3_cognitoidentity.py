"""
Connection module for Amazon Cognito Identity using boto3.
==========================================================

    Renamed from ``boto_cognitoidentity`` to ``boto3_cognitoidentity`` and rewritten
    to use the boto3 ``cognito-identity`` client APIs directly via
    :py:mod:`saltext.boto3.utils.boto3mod`.  The legacy boto2 code path
    (object-style access, retry loops) has been removed.

:depends:
  - boto3 >= 1.28.0
  - botocore >= 1.31.0

:configuration: This module accepts explicit Cognito Identity credentials but can
    also utilize IAM roles assigned to the instance through Instance Profiles.
    Dynamic credentials are then automatically obtained from AWS API and no
    further configuration is necessary. More Information available at:

    .. code-block:: text

        http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html

    If IAM roles are not used you need to specify them either in the minion's
    config file or as a profile. For example, to specify them in the minion's
    config file:

.. code-block:: yaml

    cognito_identity.keyid: GKTADJGHEIQSXMKKRBJ08H
    cognito_identity.key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs

A region may also be specified in the configuration:

.. code-block:: yaml

    cognito_identity.region: us-east-1

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

__virtualname__ = "boto3_cognitoidentity"


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
        "The boto3_cognitoidentity module could not be loaded: boto3 is not available.",
    )


def _find_identity_pool_ids(name, pool_id, conn):
    """
    Given identity pool name (or optionally a pool_id and name will be ignored),
    find and return list of matching identity pool ids.
    """
    ids = []
    if pool_id is None:
        for pools in boto3mod.paged_call(
            conn.list_identity_pools,
            marker_flag="NextToken",
            marker_arg="NextToken",
            MaxResults=25,
        ):
            for pool in pools["IdentityPools"]:
                if pool["IdentityPoolName"] == name:
                    ids.append(pool["IdentityPoolId"])
    else:
        ids.append(pool_id)
    return ids


def describe_identity_pools(
    IdentityPoolName,
    IdentityPoolId=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Given an identity pool name (or optionally an identity pool id, in which
    case the given name will be ignored), return the matching identity pool
    properties.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_cognitoidentity.describe_identity_pools my_id_pool_name
        salt myminion boto3_cognitoidentity.describe_identity_pools '' IdentityPoolId=my_id_pool_id
    """
    try:
        conn = _get_conn("cognito-identity", region=region, key=key, keyid=keyid, profile=profile)
        ids = _find_identity_pool_ids(IdentityPoolName, IdentityPoolId, conn)
        if ids:
            results = []
            for pool_id in ids:
                response = conn.describe_identity_pool(IdentityPoolId=pool_id)
                response.pop("ResponseMetadata", None)
                results.append(response)
            return {"identity_pools": results}
        return {"identity_pools": None}
    except ClientError as e:
        return {"error": boto3mod.get_error(e)}


def create_identity_pool(
    IdentityPoolName,
    AllowUnauthenticatedIdentities=False,
    SupportedLoginProviders=None,
    DeveloperProviderName=None,
    OpenIdConnectProviderARNs=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Create a new identity pool. All parameters except for ``IdentityPoolName``
    are optional. ``SupportedLoginProviders`` should be a dict mapping provider
    names to provider app IDs. ``OpenIdConnectProviderARNs`` should be a list
    of OpenID Connect provider ARNs.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_cognitoidentity.create_identity_pool my_id_pool_name \\
            DeveloperProviderName=custom_developer_provider
    """
    if SupportedLoginProviders is None:
        SupportedLoginProviders = {}
    if OpenIdConnectProviderARNs is None:
        OpenIdConnectProviderARNs = []
    try:
        conn = _get_conn("cognito-identity", region=region, key=key, keyid=keyid, profile=profile)
        request_params = {
            "IdentityPoolName": IdentityPoolName,
            "AllowUnauthenticatedIdentities": AllowUnauthenticatedIdentities,
            "SupportedLoginProviders": SupportedLoginProviders,
            "OpenIdConnectProviderARNs": OpenIdConnectProviderARNs,
        }
        if DeveloperProviderName:
            request_params["DeveloperProviderName"] = DeveloperProviderName
        response = conn.create_identity_pool(**request_params)
        response.pop("ResponseMetadata", None)
        return {"created": True, "identity_pool": response}
    except ClientError as e:
        return {"created": False, "error": boto3mod.get_error(e)}


def delete_identity_pools(
    IdentityPoolName,
    IdentityPoolId=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Given an identity pool name (or optionally an identity pool id, in which
    case the given name will be ignored), delete all matching identity pools.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_cognitoidentity.delete_identity_pools my_id_pool_name
        salt myminion boto3_cognitoidentity.delete_identity_pools '' IdentityPoolId=my_id_pool_id
    """
    try:
        conn = _get_conn("cognito-identity", region=region, key=key, keyid=keyid, profile=profile)
        ids = _find_identity_pool_ids(IdentityPoolName, IdentityPoolId, conn)
        count = 0
        if ids:
            for pool_id in ids:
                conn.delete_identity_pool(IdentityPoolId=pool_id)
                count += 1
            return {"deleted": True, "count": count}
        return {"deleted": False, "count": count}
    except ClientError as e:
        return {"deleted": False, "error": boto3mod.get_error(e)}


def get_identity_pool_roles(
    IdentityPoolName,
    IdentityPoolId=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Given an identity pool name (or optionally an identity pool id, in which
    case the given name will be ignored), return a list of associated roles.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_cognitoidentity.get_identity_pool_roles my_id_pool_name
        salt myminion boto3_cognitoidentity.get_identity_pool_roles '' IdentityPoolId=my_id_pool_id
    """
    try:
        conn = _get_conn("cognito-identity", region=region, key=key, keyid=keyid, profile=profile)
        ids = _find_identity_pool_ids(IdentityPoolName, IdentityPoolId, conn)
        if ids:
            results = []
            for pool_id in ids:
                response = conn.get_identity_pool_roles(IdentityPoolId=pool_id)
                response.pop("ResponseMetadata", None)
                results.append(response)
            return {"identity_pool_roles": results}
        return {"identity_pool_roles": None}
    except ClientError as e:
        return {"error": boto3mod.get_error(e)}


def _get_role_arn(name, **conn_params):
    """
    Helper function to turn a name into an arn string, returns None if not
    able to resolve.
    """
    if name.startswith("arn:aws:iam"):
        return name
    role = __salt__["boto3_iam.describe_role"](name, **conn_params)
    return role.get("arn") if role else None


def set_identity_pool_roles(
    IdentityPoolId,
    AuthenticatedRole=None,
    UnauthenticatedRole=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Given an identity pool id, set the given ``AuthenticatedRole`` and
    ``UnauthenticatedRole`` (each can be an iam arn or a role name). If either
    role is not given, the previously associated role is cleared.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_cognitoidentity.set_identity_pool_roles my_id_pool_id
    """
    conn_params = {"region": region, "key": key, "keyid": keyid, "profile": profile}
    try:
        conn = boto3mod.get_connection(
            "cognito-identity",
            opts=__opts__,
            context=__context__,
            **conn_params,
        )
        if AuthenticatedRole:
            role_arn = _get_role_arn(AuthenticatedRole, **conn_params)
            if role_arn is None:
                return {
                    "set": False,
                    "error": f"invalid AuthenticatedRole {AuthenticatedRole}",
                }
            AuthenticatedRole = role_arn
        if UnauthenticatedRole:
            role_arn = _get_role_arn(UnauthenticatedRole, **conn_params)
            if role_arn is None:
                return {
                    "set": False,
                    "error": f"invalid UnauthenticatedRole {UnauthenticatedRole}",
                }
            UnauthenticatedRole = role_arn

        Roles = {}
        if AuthenticatedRole:
            Roles["authenticated"] = AuthenticatedRole
        if UnauthenticatedRole:
            Roles["unauthenticated"] = UnauthenticatedRole

        conn.set_identity_pool_roles(IdentityPoolId=IdentityPoolId, Roles=Roles)
        return {"set": True, "roles": Roles}
    except ClientError as e:
        return {"set": False, "error": boto3mod.get_error(e)}


def update_identity_pool(
    IdentityPoolId,
    IdentityPoolName=None,
    AllowUnauthenticatedIdentities=False,
    SupportedLoginProviders=None,
    DeveloperProviderName=None,
    OpenIdConnectProviderARNs=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Update the given ``IdentityPoolId``'s properties. All parameters except
    for ``IdentityPoolId`` are optional. ``SupportedLoginProviders`` should be
    a dict mapping provider names to provider app IDs.
    ``OpenIdConnectProviderARNs`` should be a list of OpenID Connect provider
    ARNs.

    To clear ``SupportedLoginProviders`` pass ``{}``.
    To clear ``OpenIdConnectProviderARNs`` pass ``[]``.

    ``DeveloperProviderName`` cannot be updated after it has been set.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_cognitoidentity.update_identity_pool my_id_pool_id my_id_pool_name \\
            DeveloperProviderName=custom_developer_provider
    """
    conn_params = {"region": region, "key": key, "keyid": keyid, "profile": profile}
    response = describe_identity_pools("", IdentityPoolId=IdentityPoolId, **conn_params)
    error = response.get("error")
    if error is None and response.get("identity_pools") is None:
        error = "No matching pool"
    if error:
        return {"updated": False, "error": error}

    id_pool = response.get("identity_pools")[0]
    request_params = id_pool.copy()

    if IdentityPoolName is not None and IdentityPoolName != request_params.get("IdentityPoolName"):
        request_params["IdentityPoolName"] = IdentityPoolName

    if AllowUnauthenticatedIdentities != request_params.get("AllowUnauthenticatedIdentities"):
        request_params["AllowUnauthenticatedIdentities"] = AllowUnauthenticatedIdentities

    current_val = request_params.pop("SupportedLoginProviders", None)
    if SupportedLoginProviders is not None and SupportedLoginProviders != current_val:
        request_params["SupportedLoginProviders"] = SupportedLoginProviders

    # DeveloperProviderName can only be set once per AWS account.
    current_val = request_params.pop("DeveloperProviderName", None)
    if current_val is None and DeveloperProviderName is not None:
        request_params["DeveloperProviderName"] = DeveloperProviderName

    current_val = request_params.pop("OpenIdConnectProviderARNs", None)
    if OpenIdConnectProviderARNs is not None and OpenIdConnectProviderARNs != current_val:
        request_params["OpenIdConnectProviderARNs"] = OpenIdConnectProviderARNs

    try:
        conn = boto3mod.get_connection(
            "cognito-identity",
            opts=__opts__,
            context=__context__,
            **conn_params,
        )
        response = conn.update_identity_pool(**request_params)
        response.pop("ResponseMetadata", None)
        return {"updated": True, "identity_pool": response}
    except ClientError as e:
        return {"updated": False, "error": boto3mod.get_error(e)}
