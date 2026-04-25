"""
Manage Cognito Identity Pools using boto3.
==========================================

    Renamed from ``boto_cognitoidentity`` to ``boto3_cognitoidentity`` and updated to call the
    refactored ``boto3_cognitoidentity`` execution module.

Create and destroy Cognito Identity Pools. Be aware that this interacts with Amazon's
services, and so may incur charges.

:depends:
  - boto3 >= 1.28.0
  - botocore >= 1.31.0

This module uses boto3, which can be installed via package, or pip.

This module accepts explicit Cognito Identity credentials but can also utilize
IAM roles assigned to the instance through Instance Profiles. Dynamic
credentials are then automatically obtained from AWS API and no further
configuration is necessary. More Information available at:

.. code-block:: text

    http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html

If IAM roles are not used you need to specify them either in the minion's config file
or as a profile. For example, to specify them in the minion's config file:

.. code-block:: yaml

    cognitoidentity.keyid: GKTADJGHEIQSXMKKRBJ08H
    cognitoidentity.key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs

It's also possible to specify key, keyid and region via a profile, either
as a passed in dict, or as a string to pull from pillars or minion config:

.. code-block:: yaml

    myprofile:
        keyid: GKTADJGHEIQSXMKKRBJ08H
        key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs
        region: us-east-1

.. code-block:: yaml

    ensure-pool-present:
      boto3_cognitoidentity.pool_present:
        - name: example

.. versionadded:: 1.0.0
"""

import logging

log = logging.getLogger(__name__)

__virtualname__ = "boto3_cognitoidentity"


def __virtual__():
    """
    Only load if the boto3_cognitoidentity execution module is available.
    """
    if "boto3_cognitoidentity.describe_identity_pools" in __salt__:
        return __virtualname__
    return (
        False,
        "boto3_cognitoidentity state module could not be loaded: "
        "boto3_cognitoidentity execution module is not available.",
    )


# TODO: switch to __salt__["pillar.get"] to remove direct __pillar__ access.
def _get_object(objname, objtype):
    """
    Helper function to retrieve objtype from pillars if objname is a string,
    used for SupportedLoginProviders and OpenIdConnectProviderARNs.
    """
    ret = None
    if objname is None:
        return ret

    if isinstance(objname, str):
        if objname in __opts__:
            ret = __opts__[objname]
        master_opts = __pillar__.get("master", {})
        if objname in master_opts:
            ret = master_opts[objname]
        if objname in __pillar__:
            ret = __pillar__[objname]
    elif isinstance(objname, objtype):
        ret = objname

    if not isinstance(ret, objtype):
        ret = None

    return ret


def _role_present(ret, IdentityPoolId, AuthenticatedRole, UnauthenticatedRole, conn_params):
    """
    Helper function to set the Roles on the identity pool.
    """
    r = __salt__["boto3_cognitoidentity.get_identity_pool_roles"](
        IdentityPoolName="", IdentityPoolId=IdentityPoolId, **conn_params
    )
    if r.get("error"):
        ret["result"] = False
        failure_comment = "Failed to get existing identity pool roles: {}".format(
            r["error"].get("message", r["error"])
        )
        ret["comment"] = "{}\n{}".format(ret["comment"], failure_comment)
        return

    existing_identity_pool_role = r.get("identity_pool_roles")[0].get("Roles", {})
    r = __salt__["boto3_cognitoidentity.set_identity_pool_roles"](
        IdentityPoolId=IdentityPoolId,
        AuthenticatedRole=AuthenticatedRole,
        UnauthenticatedRole=UnauthenticatedRole,
        **conn_params,
    )
    if not r.get("set"):
        ret["result"] = False
        failure_comment = "Failed to set roles: {}".format(r["error"].get("message", r["error"]))
        ret["comment"] = "{}\n{}".format(ret["comment"], failure_comment)
        return

    updated_identity_pool_role = r.get("roles")

    if existing_identity_pool_role != updated_identity_pool_role:
        if not ret["changes"]:
            ret["changes"]["old"] = {}
            ret["changes"]["new"] = {}
        ret["changes"]["old"]["Roles"] = existing_identity_pool_role
        ret["changes"]["new"]["Roles"] = r.get("roles")
        ret["comment"] = "{}\n{}".format(ret["comment"], "identity pool roles updated.")
    else:
        ret["comment"] = "{}\n{}".format(ret["comment"], "identity pool roles is already current.")


def pool_present(
    name,
    IdentityPoolName,
    AuthenticatedRole,
    AllowUnauthenticatedIdentities=False,
    UnauthenticatedRole=None,
    SupportedLoginProviders=None,
    DeveloperProviderName=None,
    OpenIdConnectProviderARNs=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):  # pylint: disable=unused-argument
    """
    Ensure the given Cognito Identity Pool exists.

    name
        The name of the state definition.

    IdentityPoolName
        Name of the Cognito Identity Pool.

    AuthenticatedRole
        An IAM role name or ARN that will be associated with temporary AWS
        credentials for an authenticated cognito identity.

    AllowUnauthenticatedIdentities
        Whether to allow anonymous user identities.

    UnauthenticatedRole
        An IAM role name or ARN that will be associated with anonymous user
        identities.

    SupportedLoginProviders
        A dictionary or pillar key that contains key:value pairs mapping
        provider names to provider app IDs.

    DeveloperProviderName
        The domain by which Cognito will refer to your users. Once set, it
        cannot be changed.

    OpenIdConnectProviderARNs
        A list or pillar key that contains a list of OpenID Connect provider
        ARNs.

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

        ensure-pool-present:
          boto3_cognitoidentity.pool_present:
            - name: example

    """
    ret = {"name": IdentityPoolName, "result": True, "comment": "", "changes": {}}
    conn_params = {"region": region, "key": key, "keyid": keyid, "profile": profile}

    r = __salt__["boto3_cognitoidentity.describe_identity_pools"](
        IdentityPoolName=IdentityPoolName, **conn_params
    )

    if r.get("error"):
        ret["result"] = False
        ret["comment"] = "Failed to describe identity pools {}".format(r["error"]["message"])
        return ret

    identity_pools = r.get("identity_pools")
    if identity_pools and len(identity_pools) > 1:
        ret["result"] = False
        ret["comment"] = (
            "More than one identity pool for the given name matched "
            "Cannot execute pool_present function.\n"
            "Matched Identity Pools:\n{}".format(identity_pools)
        )
        return ret
    existing_identity_pool = None if identity_pools is None else identity_pools[0]
    IdentityPoolId = (
        None if existing_identity_pool is None else existing_identity_pool.get("IdentityPoolId")
    )

    if __opts__["test"]:
        if identity_pools is None:
            ret["comment"] = f"A new identity pool named {IdentityPoolName} will be created."
        else:
            ret["comment"] = (
                f"An existing identity pool named {IdentityPoolName} with id "
                f"{IdentityPoolId} will be updated."
            )
        ret["result"] = None
        return ret

    SupportedLoginProviders = _get_object(SupportedLoginProviders, dict)
    OpenIdConnectProviderARNs = _get_object(OpenIdConnectProviderARNs, list)

    request_params = {
        "IdentityPoolName": IdentityPoolName,
        "AllowUnauthenticatedIdentities": AllowUnauthenticatedIdentities,
        "SupportedLoginProviders": SupportedLoginProviders,
        "DeveloperProviderName": DeveloperProviderName,
        "OpenIdConnectProviderARNs": OpenIdConnectProviderARNs,
    }
    request_params.update(conn_params)

    updated_identity_pool = None
    if IdentityPoolId is None:
        r = __salt__["boto3_cognitoidentity.create_identity_pool"](**request_params)
        if r.get("created"):
            updated_identity_pool = r.get("identity_pool")
            IdentityPoolId = updated_identity_pool.get("IdentityPoolId")
            ret["comment"] = (
                f"A new identity pool with name {IdentityPoolName}, id "
                f"{IdentityPoolId} is created."
            )
        else:
            ret["result"] = False
            ret["comment"] = "Failed to add a new identity pool: {}".format(
                r["error"].get("message", r["error"])
            )
            return ret
    else:
        request_params["IdentityPoolId"] = IdentityPoolId
        # IdentityPoolName is never changed from the state module
        request_params.pop("IdentityPoolName", None)
        r = __salt__["boto3_cognitoidentity.update_identity_pool"](**request_params)
        if r.get("updated"):
            updated_identity_pool = r.get("identity_pool")
            ret["comment"] = (
                f"Existing identity pool with name {IdentityPoolName}, id "
                f"{IdentityPoolId} is updated."
            )
        else:
            ret["result"] = False
            ret["comment"] = (
                f"Failed to update an existing identity pool {IdentityPoolName} "
                f"{IdentityPoolId}: {r['error'].get('message', r['error'])}"
            )
            return ret

    if existing_identity_pool != updated_identity_pool:
        ret["changes"]["old"] = {}
        ret["changes"]["new"] = {}
        change_key = f"Identity Pool Name {IdentityPoolName}"
        ret["changes"]["old"][change_key] = existing_identity_pool
        ret["changes"]["new"][change_key] = updated_identity_pool
    else:
        ret["comment"] = "Identity Pool state is current, no changes."

    # Update the Auth/Unauth Roles
    _role_present(ret, IdentityPoolId, AuthenticatedRole, UnauthenticatedRole, conn_params)

    return ret


def pool_absent(
    name,
    IdentityPoolName,
    RemoveAllMatched=False,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):  # pylint: disable=unused-argument
    """
    Ensure a Cognito Identity Pool is absent.

    name
        The name of the state definition.

    IdentityPoolName
        Name of the Cognito Identity Pool.

    RemoveAllMatched
        If True, all identity pools matching ``IdentityPoolName`` are removed.
        If False and multiple pools match, no action is taken.

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

        ensure-pool-absent:
          boto3_cognitoidentity.pool_absent:
            - name: example

    """
    ret = {"name": IdentityPoolName, "result": True, "comment": "", "changes": {}}
    conn_params = {"region": region, "key": key, "keyid": keyid, "profile": profile}

    r = __salt__["boto3_cognitoidentity.describe_identity_pools"](
        IdentityPoolName=IdentityPoolName, **conn_params
    )

    if r.get("error"):
        ret["result"] = False
        ret["comment"] = "Failed to describe identity pools {}".format(r["error"]["message"])
        return ret

    identity_pools = r.get("identity_pools")

    if identity_pools is None:
        ret["result"] = True
        ret["comment"] = f"No matching identity pool for the given name {IdentityPoolName}"
        return ret

    if not RemoveAllMatched and len(identity_pools) > 1:
        ret["result"] = False
        ret["comment"] = (
            "More than one identity pool for the given name matched "
            "and RemoveAllMatched flag is False.\n"
            "Matched Identity Pools:\n{}".format(identity_pools)
        )
        return ret

    if __opts__["test"]:
        ret["comment"] = f"The following matched identity pools will be deleted.\n{identity_pools}"
        ret["result"] = None
        return ret

    for identity_pool in identity_pools:
        IdentityPoolId = identity_pool.get("IdentityPoolId")
        r = __salt__["boto3_cognitoidentity.delete_identity_pools"](
            IdentityPoolName="", IdentityPoolId=IdentityPoolId, **conn_params
        )
        if r.get("error"):
            ret["result"] = False
            failure_comment = "Failed to delete identity pool {}: {}".format(
                IdentityPoolId, r["error"].get("message", r["error"])
            )
            ret["comment"] = "{}\n{}".format(ret["comment"], failure_comment)
            return ret

        if r.get("deleted"):
            if not ret["changes"]:
                ret["changes"]["old"] = {}
                ret["changes"]["new"] = {}
            change_key = f"Identity Pool Id {IdentityPoolId}"
            ret["changes"]["old"][change_key] = IdentityPoolName
            ret["changes"]["new"][change_key] = None
            ret["comment"] = "{}\n{}".format(ret["comment"], f"{change_key} deleted")
        else:
            ret["result"] = False
            failure_comment = f"Identity Pool Id {IdentityPoolId} not deleted, returned count 0"
            ret["comment"] = "{}\n{}".format(ret["comment"], failure_comment)
            return ret

    return ret
