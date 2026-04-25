"""
Connection module for Amazon IAM using boto3.
=============================================

    Renamed from ``boto_iam`` to ``boto3_iam`` and rewritten to use the
    boto3 IAM client API directly via
    :py:mod:`saltext.boto3.utils.boto3mod`. The legacy boto2 code path has
    been removed.

:depends:
  - boto3 >= 1.28.0
  - botocore >= 1.31.0

:configuration: This module accepts explicit IAM credentials but can
    also utilize IAM roles assigned to the instance through Instance Profiles.
    Dynamic credentials are then automatically obtained from AWS API and no
    further configuration is necessary. More Information available at:

    .. code-block:: text

        http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html

    If IAM roles are not used you need to specify them either in the minion's
    config file or as a profile. For example, to specify them in the minion's
    config file:

.. code-block:: yaml

    iam.keyid: GKTADJGHEIQSXMKKRBJ08H
    iam.key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs

A region may also be specified in the configuration:

.. code-block:: yaml

    iam.region: us-east-1

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
import time
import urllib.parse

import salt.utils.json
import salt.utils.yaml
from salt.utils import odict

from saltext.boto3.utils import boto3mod

try:
    from botocore.exceptions import ClientError

    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

log = logging.getLogger(__name__)

__virtualname__ = "boto3_iam"


def __virtual__():
    """
    Only load if boto3 is available.
    """
    if HAS_BOTO3:
        return __virtualname__
    return (False, "The boto3_iam module could not be loaded: boto3 is not available.")


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


def _client_error_code(exc):
    if isinstance(exc, ClientError):
        return exc.response.get("Error", {}).get("Code")
    return None


def _is_not_found(exc):
    return _client_error_code(exc) in ("NoSuchEntity", "NoSuchEntityException")


def _decode_policy_document(doc):
    if doc is None:
        return doc
    if isinstance(doc, (dict, list)):
        return doc
    return salt.utils.json.loads(urllib.parse.unquote(doc), object_pairs_hook=odict.OrderedDict)


def instance_profile_exists(name, region=None, key=None, keyid=None, profile=None):
    """
    Check to see if an instance profile exists.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.instance_profile_exists myiprofile
    """
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.get_instance_profile(InstanceProfileName=name)
        return True
    except ClientError:
        return False


def create_instance_profile(name, region=None, key=None, keyid=None, profile=None):
    """
    Create an instance profile.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.create_instance_profile myiprofile
    """
    if instance_profile_exists(name, region, key, keyid, profile):
        return True
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.create_instance_profile(InstanceProfileName=name)
        log.info("Created %s instance profile.", name)
        return True
    except ClientError as exc:
        log.debug(exc)
        log.error("Failed to create %s instance profile.", name)
        return False


def delete_instance_profile(name, region=None, key=None, keyid=None, profile=None):
    """
    Delete an instance profile.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.delete_instance_profile myiprofile
    """
    if not instance_profile_exists(name, region, key, keyid, profile):
        return True
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.delete_instance_profile(InstanceProfileName=name)
        log.info("Deleted %s instance profile.", name)
        return True
    except ClientError as exc:
        log.debug(exc)
        log.error("Failed to delete %s instance profile.", name)
        return False


def get_all_instance_profiles(path_prefix="/", region=None, key=None, keyid=None, profile=None):
    """
    Get and return all IAM instance profiles, starting at the optional path.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_iam.get_all_instance_profiles
    """
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    profiles = []
    marker = None
    while True:
        params = {"PathPrefix": path_prefix}
        if marker:
            params["Marker"] = marker
        res = conn.list_instance_profiles(**params)
        profiles.extend(res.get("InstanceProfiles", []))
        if res.get("IsTruncated"):
            marker = res.get("Marker")
        else:
            break
    return profiles


def list_instance_profiles(path_prefix="/", region=None, key=None, keyid=None, profile=None):
    """
    List all IAM instance profiles, starting at the optional path.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_iam.list_instance_profiles
    """
    return [
        p["InstanceProfileName"]
        for p in get_all_instance_profiles(path_prefix, region, key, keyid, profile)
    ]


def role_exists(name, region=None, key=None, keyid=None, profile=None):
    """
    Check to see if an IAM role exists.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.role_exists myirole
    """
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.get_role(RoleName=name)
        return True
    except ClientError:
        return False


def describe_role(name, region=None, key=None, keyid=None, profile=None):
    """
    Get information for a role.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.describe_role myirole
    """
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    try:
        info = conn.get_role(RoleName=name)
        role = info.get("Role")
        if not role:
            return False
        role["assume_role_policy_document"] = _decode_policy_document(
            role.get("AssumeRolePolicyDocument")
        )
        # If Sid wasn't defined by the user, remove empty Sid fields for
        # idempotent comparison.
        doc = role["assume_role_policy_document"]
        if isinstance(doc, dict):
            for policy_key, policy in doc.items():
                if policy_key == "Statement" and isinstance(policy, list):
                    for val in policy:
                        if isinstance(val, dict) and "Sid" in val and not val["Sid"]:
                            del val["Sid"]
        return role
    except ClientError as exc:
        log.debug(exc)
        log.error("Failed to get %s information.", name)
        return False


def create_role(
    name,
    policy_document=None,
    path=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Create an instance role.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.create_role myrole
    """
    if role_exists(name, region, key, keyid, profile):
        return True
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    params = {"RoleName": name}
    if policy_document is not None:
        if not isinstance(policy_document, str):
            policy_document = salt.utils.json.dumps(policy_document)
        params["AssumeRolePolicyDocument"] = policy_document
    if path is not None:
        params["Path"] = path
    try:
        conn.create_role(**params)
        log.info("Created IAM role %s.", name)
        return True
    except ClientError as exc:
        log.error(exc)
        log.error("Failed to create IAM role %s.", name)
        return False


def delete_role(name, region=None, key=None, keyid=None, profile=None):
    """
    Delete an IAM role.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.delete_role myirole
    """
    if not role_exists(name, region, key, keyid, profile):
        return True
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.delete_role(RoleName=name)
        log.info("Deleted %s IAM role.", name)
        return True
    except ClientError as exc:
        log.debug(exc)
        log.error("Failed to delete %s IAM role.", name)
        return False


def profile_associated(role_name, profile_name, region, key, keyid, profile):
    """
    Check to see if an instance profile is associated with an IAM role.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.profile_associated myirole myiprofile
    """
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    try:
        res = conn.list_instance_profiles_for_role(RoleName=role_name)
    except ClientError as exc:
        log.debug(exc)
        return False
    for ip in res.get("InstanceProfiles", []):
        if ip.get("InstanceProfileName") == profile_name:
            return True
    return False


def associate_profile_to_role(
    profile_name, role_name, region=None, key=None, keyid=None, profile=None
):
    """
    Associate an instance profile with an IAM role.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.associate_profile_to_role myirole myiprofile
    """
    if not role_exists(role_name, region, key, keyid, profile):
        log.error("IAM role %s does not exist.", role_name)
        return False
    if not instance_profile_exists(profile_name, region, key, keyid, profile):
        log.error("Instance profile %s does not exist.", profile_name)
        return False
    if profile_associated(role_name, profile_name, region, key, keyid, profile):
        return True
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.add_role_to_instance_profile(InstanceProfileName=profile_name, RoleName=role_name)
        log.info("Added %s instance profile to IAM role %s.", profile_name, role_name)
        return True
    except ClientError as exc:
        log.debug(exc)
        log.error(
            "Failed to add %s instance profile to IAM role %s",
            profile_name,
            role_name,
        )
        return False


def disassociate_profile_from_role(
    profile_name, role_name, region=None, key=None, keyid=None, profile=None
):
    """
    Disassociate an instance profile from an IAM role.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.disassociate_profile_from_role myirole myiprofile
    """
    if not role_exists(role_name, region, key, keyid, profile):
        log.error("IAM role %s does not exist.", role_name)
        return False
    if not instance_profile_exists(profile_name, region, key, keyid, profile):
        log.error("Instance profile %s does not exist.", profile_name)
        return False
    if not profile_associated(role_name, profile_name, region, key, keyid, profile):
        return True
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.remove_role_from_instance_profile(InstanceProfileName=profile_name, RoleName=role_name)
        log.info("Removed %s instance profile from IAM role %s.", profile_name, role_name)
        return True
    except ClientError as exc:
        log.debug(exc)
        log.error(
            "Failed to remove %s instance profile from IAM role %s.",
            profile_name,
            role_name,
        )
        return False


def list_role_policies(role_name, region=None, key=None, keyid=None, profile=None):
    """
    Get a list of inline policy names from a role.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.list_role_policies myirole
    """
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    try:
        names = []
        marker = None
        while True:
            params = {"RoleName": role_name}
            if marker:
                params["Marker"] = marker
            res = conn.list_role_policies(**params)
            names.extend(res.get("PolicyNames", []))
            if res.get("IsTruncated"):
                marker = res.get("Marker")
            else:
                break
        return names
    except ClientError as exc:
        log.debug(exc)
        return []


def get_role_policy(role_name, policy_name, region=None, key=None, keyid=None, profile=None):
    """
    Get a role policy.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.get_role_policy myirole mypolicy
    """
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    try:
        res = conn.get_role_policy(RoleName=role_name, PolicyName=policy_name)
        return _decode_policy_document(res.get("PolicyDocument"))
    except ClientError:
        return {}


def create_role_policy(
    role_name, policy_name, policy, region=None, key=None, keyid=None, profile=None
):
    """
    Create or modify a role policy.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.create_role_policy myirole mypolicy '{...}'
    """
    _policy = get_role_policy(role_name, policy_name, region, key, keyid, profile)
    mode = "create"
    if _policy:
        if _policy == policy:
            return True
        mode = "modify"
    if isinstance(policy, str):
        policy = salt.utils.json.loads(policy, object_pairs_hook=odict.OrderedDict)
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.put_role_policy(
            RoleName=role_name,
            PolicyName=policy_name,
            PolicyDocument=salt.utils.json.dumps(policy),
        )
        if mode == "create":
            log.info("Successfully added policy %s to IAM role %s.", policy_name, role_name)
        else:
            log.info("Successfully modified policy %s for IAM role %s.", policy_name, role_name)
        return True
    except ClientError as exc:
        log.error(exc)
        log.error("Failed to %s policy %s for IAM role %s.", mode, policy_name, role_name)
        return False


def delete_role_policy(role_name, policy_name, region=None, key=None, keyid=None, profile=None):
    """
    Delete a role policy.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.delete_role_policy myirole mypolicy
    """
    _policy = get_role_policy(role_name, policy_name, region, key, keyid, profile)
    if not _policy:
        return True
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.delete_role_policy(RoleName=role_name, PolicyName=policy_name)
        log.info("Successfully deleted policy %s for IAM role %s.", policy_name, role_name)
        return True
    except ClientError as exc:
        log.debug(exc)
        log.error("Failed to delete policy %s for IAM role %s.", policy_name, role_name)
        return False


def update_assume_role_policy(
    role_name, policy_document, region=None, key=None, keyid=None, profile=None
):
    """
    Update an assume role policy for a role.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.update_assume_role_policy myrole '{"Statement":"..."}'
    """
    if isinstance(policy_document, str):
        policy_document = salt.utils.json.loads(
            policy_document, object_pairs_hook=odict.OrderedDict
        )
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.update_assume_role_policy(
            RoleName=role_name,
            PolicyDocument=salt.utils.json.dumps(policy_document),
        )
        log.info("Successfully updated assume role policy for IAM role %s.", role_name)
        return True
    except ClientError as exc:
        log.error(exc)
        log.error("Failed to update assume role policy for IAM role %s.", role_name)
        return False


def build_policy(
    region=None, key=None, keyid=None, profile=None
):  # pylint: disable=unused-argument
    """
    Build a default assume role policy for EC2.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.build_policy
    """
    return {
        "Version": "2008-10-17",
        "Statement": [
            {
                "Action": "sts:AssumeRole",
                "Effect": "Allow",
                "Principal": {"Service": "ec2.amazonaws.com"},
            }
        ],
    }


def get_all_roles(path_prefix=None, region=None, key=None, keyid=None, profile=None):
    """
    Get and return all IAM role details, starting at the optional path.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_iam.get_all_roles
    """
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    roles = []
    marker = None
    while True:
        params = {}
        if path_prefix is not None:
            params["PathPrefix"] = path_prefix
        if marker:
            params["Marker"] = marker
        res = conn.list_roles(**params)
        roles.extend(res.get("Roles", []))
        if res.get("IsTruncated"):
            marker = res.get("Marker")
        else:
            break
    return roles


def get_user(user_name=None, region=None, key=None, keyid=None, profile=None):
    """
    Get user information.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.get_user myuser
    """
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    try:
        params = {}
        if user_name:
            params["UserName"] = user_name
        res = conn.get_user(**params)
        return res.get("User") or False
    except ClientError as exc:
        log.debug(exc)
        log.error("Failed to get IAM user %s info.", user_name)
        return False


def create_user(user_name, path=None, region=None, key=None, keyid=None, profile=None):
    """
    Create a user.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.create_user myuser
    """
    if get_user(user_name, region, key, keyid, profile):
        return True
    if not path:
        path = "/"
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.create_user(UserName=user_name, Path=path)
        log.info("Created IAM user : %s.", user_name)
        return True
    except ClientError as exc:
        log.debug(exc)
        log.error("Failed to create IAM user %s.", user_name)
        return False


def delete_user(user_name, region=None, key=None, keyid=None, profile=None):
    """
    Delete a user.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.delete_user myuser
    """
    if not get_user(user_name, region, key, keyid, profile):
        return True
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.delete_user(UserName=user_name)
        log.info("Deleted IAM user : %s .", user_name)
        return True
    except ClientError as exc:
        log.debug(exc)
        log.error("Failed to delete IAM user %s", user_name)
        return str(exc)


def get_all_access_keys(
    user_name,
    marker=None,
    max_items=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Get all access keys for a user.

    Returns a dict with an ``AccessKeyMetadata`` list.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.get_all_access_keys myuser
    """
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    try:
        params = {"UserName": user_name}
        if marker:
            params["Marker"] = marker
        if max_items:
            params["MaxItems"] = max_items
        return conn.list_access_keys(**params)
    except ClientError as exc:
        log.debug(exc)
        log.error("Failed to get access keys for IAM user %s.", user_name)
        return str(exc)


def create_access_key(user_name, region=None, key=None, keyid=None, profile=None):
    """
    Create access key id for a user.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.create_access_key myuser
    """
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    try:
        return conn.create_access_key(UserName=user_name)
    except ClientError as exc:
        log.debug(exc)
        log.error("Failed to create access key.")
        return str(exc)


def delete_access_key(
    access_key_id, user_name=None, region=None, key=None, keyid=None, profile=None
):
    """
    Delete access key id from a user.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.delete_access_key myuser
    """
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    try:
        params = {"AccessKeyId": access_key_id}
        if user_name:
            params["UserName"] = user_name
        return conn.delete_access_key(**params)
    except ClientError as exc:
        log.debug(exc)
        log.error("Failed to delete access key id %s.", access_key_id)
        return str(exc)


def get_all_users(path_prefix="/", region=None, key=None, keyid=None, profile=None):
    """
    Get and return all IAM user details, starting at the optional path.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_iam.get_all_users
    """
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    users = []
    marker = None
    while True:
        params = {"PathPrefix": path_prefix}
        if marker:
            params["Marker"] = marker
        res = conn.list_users(**params)
        users.extend(res.get("Users", []))
        if res.get("IsTruncated"):
            marker = res.get("Marker")
        else:
            break
    return users


def get_all_user_policies(
    user_name,
    marker=None,
    max_items=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Get all inline user policy names.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.get_all_user_policies myuser
    """
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    try:
        params = {"UserName": user_name}
        if marker:
            params["Marker"] = marker
        if max_items:
            params["MaxItems"] = max_items
        res = conn.list_user_policies(**params)
        return res.get("PolicyNames", [])
    except ClientError as exc:
        log.debug(exc)
        log.error("Failed to get policies for user %s.", user_name)
        return False


def get_user_policy(user_name, policy_name, region=None, key=None, keyid=None, profile=None):
    """
    Retrieves the specified inline policy document for the specified user.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.get_user_policy myuser mypolicyname
    """
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    try:
        res = conn.get_user_policy(UserName=user_name, PolicyName=policy_name)
        return _decode_policy_document(res.get("PolicyDocument"))
    except ClientError as exc:
        log.debug(exc)
        log.error("Failed to get policy %s for IAM user %s.", policy_name, user_name)
        return False


def put_user_policy(
    user_name, policy_name, policy_json, region=None, key=None, keyid=None, profile=None
):
    """
    Adds or updates the specified inline policy document for the specified user.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.put_user_policy myuser policyname policyrules
    """
    if not get_user(user_name, region, key, keyid, profile):
        log.error("IAM user %s does not exist", user_name)
        return False
    if not isinstance(policy_json, str):
        policy_json = salt.utils.json.dumps(policy_json)
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.put_user_policy(
            UserName=user_name,
            PolicyName=policy_name,
            PolicyDocument=policy_json,
        )
        log.info("Created policy %s for IAM user %s.", policy_name, user_name)
        return True
    except ClientError as exc:
        log.debug(exc)
        log.error("Failed to create policy %s for IAM user %s.", policy_name, user_name)
        return False


def delete_user_policy(user_name, policy_name, region=None, key=None, keyid=None, profile=None):
    """
    Delete an inline user policy.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.delete_user_policy myuser mypolicy
    """
    _policy = get_user_policy(user_name, policy_name, region, key, keyid, profile)
    if not _policy:
        return True
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.delete_user_policy(UserName=user_name, PolicyName=policy_name)
        log.info("Successfully deleted policy %s for IAM user %s.", policy_name, user_name)
        return True
    except ClientError as exc:
        log.debug(exc)
        log.error("Failed to delete policy %s for IAM user %s.", policy_name, user_name)
        return False


def get_group(group_name, region=None, key=None, keyid=None, profile=None):
    """
    Get group information.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.get_group mygroup
    """
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    try:
        res = conn.get_group(GroupName=group_name, MaxItems=1)
        return res.get("Group") or False
    except ClientError as exc:
        log.debug(exc)
        log.error("Failed to get IAM group %s info.", group_name)
        return False


def create_group(group_name, path=None, region=None, key=None, keyid=None, profile=None):
    """
    Create a group.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.create_group group
    """
    if get_group(group_name, region=region, key=key, keyid=keyid, profile=profile):
        return True
    if not path:
        path = "/"
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.create_group(GroupName=group_name, Path=path)
        log.info("Created IAM group : %s.", group_name)
        return True
    except ClientError as exc:
        log.debug(exc)
        log.error("Failed to create IAM group %s.", group_name)
        return False


def get_group_members(group_name, region=None, key=None, keyid=None, profile=None):
    """
    Get the users that are members of a group.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.get_group_members mygroup
    """
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    try:
        users = []
        marker = None
        while True:
            params = {"GroupName": group_name, "MaxItems": 1000}
            if marker:
                params["Marker"] = marker
            res = conn.get_group(**params)
            users.extend(res.get("Users", []))
            if res.get("IsTruncated"):
                marker = res.get("Marker")
            else:
                break
        return users
    except ClientError as exc:
        log.debug(exc)
        log.error("Failed to get members for IAM group %s.", group_name)
        return False


def user_exists_in_group(user_name, group_name, region=None, key=None, keyid=None, profile=None):
    """
    Check if user exists in group.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.user_exists_in_group myuser mygroup
    """
    users = get_group_members(
        group_name=group_name, region=region, key=key, keyid=keyid, profile=profile
    )
    if users:
        for _user in users:
            if user_name == _user.get("UserName"):
                return True
    return False


def add_user_to_group(user_name, group_name, region=None, key=None, keyid=None, profile=None):
    """
    Add user to group.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.add_user_to_group myuser mygroup
    """
    if not get_user(user_name, region, key, keyid, profile):
        log.error("Username : %s does not exist.", user_name)
        return False
    if user_exists_in_group(
        user_name, group_name, region=region, key=key, keyid=keyid, profile=profile
    ):
        return True
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.add_user_to_group(GroupName=group_name, UserName=user_name)
        return True
    except ClientError as exc:
        log.debug(exc)
        log.error("Failed to add IAM user %s to group %s.", user_name, group_name)
        return False


def remove_user_from_group(group_name, user_name, region=None, key=None, keyid=None, profile=None):
    """
    Remove user from group.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.remove_user_from_group mygroup myuser
    """
    if not get_user(user_name, region, key, keyid, profile):
        log.error("IAM user %s does not exist.", user_name)
        return False
    if not user_exists_in_group(
        user_name, group_name, region=region, key=key, keyid=keyid, profile=profile
    ):
        return True
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.remove_user_from_group(GroupName=group_name, UserName=user_name)
        return True
    except ClientError as exc:
        log.debug(exc)
        log.error("Failed to remove IAM user %s from group %s", user_name, group_name)
        return False


def put_group_policy(
    group_name,
    policy_name,
    policy_json,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Adds or updates the specified inline policy document for the specified group.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.put_group_policy mygroup policyname policyrules
    """
    if not get_group(group_name, region=region, key=key, keyid=keyid, profile=profile):
        log.error("Group %s does not exist", group_name)
        return False
    if not isinstance(policy_json, str):
        policy_json = salt.utils.json.dumps(policy_json)
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.put_group_policy(
            GroupName=group_name, PolicyName=policy_name, PolicyDocument=policy_json
        )
        log.info("Created policy for IAM group %s.", group_name)
        return True
    except ClientError as exc:
        log.debug(exc)
        log.error("Failed to create policy for IAM group %s", group_name)
        return False


def delete_group_policy(group_name, policy_name, region=None, key=None, keyid=None, profile=None):
    """
    Delete a group policy.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.delete_group_policy mygroup mypolicy
    """
    _policy = get_group_policy(group_name, policy_name, region, key, keyid, profile)
    if not _policy:
        return True
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.delete_group_policy(GroupName=group_name, PolicyName=policy_name)
        log.info("Successfully deleted policy %s for IAM group %s.", policy_name, group_name)
        return True
    except ClientError as exc:
        log.debug(exc)
        log.error("Failed to delete policy %s for IAM group %s.", policy_name, group_name)
        return False


def get_group_policy(group_name, policy_name, region=None, key=None, keyid=None, profile=None):
    """
    Retrieves the specified inline policy document for the specified group.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.get_group_policy mygroup policyname
    """
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    try:
        res = conn.get_group_policy(GroupName=group_name, PolicyName=policy_name)
        return _decode_policy_document(res.get("PolicyDocument"))
    except ClientError as exc:
        log.debug(exc)
        log.error("Failed to get IAM group %s info.", group_name)
        return False


def get_all_groups(path_prefix="/", region=None, key=None, keyid=None, profile=None):
    """
    Get and return all IAM group details, starting at the optional path.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_iam.get_all_groups
    """
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    groups = []
    marker = None
    while True:
        params = {"PathPrefix": path_prefix}
        if marker:
            params["Marker"] = marker
        res = conn.list_groups(**params)
        groups.extend(res.get("Groups", []))
        if res.get("IsTruncated"):
            marker = res.get("Marker")
        else:
            break
    return groups


def get_all_group_policies(group_name, region=None, key=None, keyid=None, profile=None):
    """
    Get a list of inline policy names from a group.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.get_all_group_policies mygroup
    """
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    try:
        res = conn.list_group_policies(GroupName=group_name)
        return res.get("PolicyNames", [])
    except ClientError as exc:
        log.debug(exc)
        return []


def delete_group(group_name, region=None, key=None, keyid=None, profile=None):
    """
    Delete a group.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.delete_group mygroup
    """
    if not get_group(group_name, region, key, keyid, profile):
        return True
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.delete_group(GroupName=group_name)
        log.info("Successfully deleted IAM group %s.", group_name)
        return True
    except ClientError as exc:
        log.debug(exc)
        log.error("Failed to delete IAM group %s.", group_name)
        return False


def create_login_profile(user_name, password, region=None, key=None, keyid=None, profile=None):
    """
    Creates a login profile for the specified user.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.create_login_profile user_name password
    """
    if not get_user(user_name, region, key, keyid, profile):
        log.error("IAM user %s does not exist", user_name)
        return False
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    try:
        res = conn.create_login_profile(UserName=user_name, Password=password)
        log.info("Created profile for IAM user %s.", user_name)
        return res
    except ClientError as exc:
        log.debug(exc)
        if _client_error_code(exc) == "EntityAlreadyExists":
            log.info("Profile already exists for IAM user %s.", user_name)
            return "Conflict"
        log.error("Failed to update profile for IAM user %s.", user_name)
        return False


def delete_login_profile(user_name, region=None, key=None, keyid=None, profile=None):
    """
    Deletes a login profile for the specified user.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.delete_login_profile user_name
    """
    if not get_user(user_name, region, key, keyid, profile):
        log.error("IAM user %s does not exist", user_name)
        return False
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.delete_login_profile(UserName=user_name)
        log.info("Deleted login profile for IAM user %s.", user_name)
        return True
    except ClientError as exc:
        log.debug(exc)
        if _is_not_found(exc):
            log.info("Login profile already deleted for IAM user %s.", user_name)
            return True
        log.error("Failed to delete login profile for IAM user %s.", user_name)
        return False


def get_all_mfa_devices(user_name, region=None, key=None, keyid=None, profile=None):
    """
    Get all MFA devices associated with an IAM user.

    Returns a list of dicts with PascalCase keys (e.g. ``SerialNumber``).

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.get_all_mfa_devices user_name
    """
    if not get_user(user_name, region, key, keyid, profile):
        log.error("IAM user %s does not exist", user_name)
        return False
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    try:
        res = conn.list_mfa_devices(UserName=user_name)
        return res.get("MFADevices", [])
    except ClientError as exc:
        log.debug(exc)
        if _is_not_found(exc):
            log.info("Could not find IAM user %s.", user_name)
            return []
        log.error("Failed to get all MFA devices for IAM user %s.", user_name)
        return False


def deactivate_mfa_device(user_name, serial, region=None, key=None, keyid=None, profile=None):
    """
    Deactivates the specified MFA device and removes it from association with
    the user.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.deactivate_mfa_device user_name serial_num
    """
    if not get_user(user_name, region, key, keyid, profile):
        log.error("IAM user %s does not exist", user_name)
        return False
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.deactivate_mfa_device(UserName=user_name, SerialNumber=serial)
        log.info("Deactivated MFA device %s for IAM user %s.", serial, user_name)
        return True
    except ClientError as exc:
        log.debug(exc)
        if _is_not_found(exc):
            log.info("MFA device %s not associated with IAM user %s.", serial, user_name)
            return True
        log.error("Failed to deactivate MFA device %s for IAM user %s.", serial, user_name)
        return False


def delete_virtual_mfa_device(serial, region=None, key=None, keyid=None, profile=None):
    """
    Deletes the specified virtual MFA device.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.delete_virtual_mfa_device serial_num
    """
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.delete_virtual_mfa_device(SerialNumber=serial)
        log.info("Deleted virtual MFA device %s.", serial)
        return True
    except ClientError as exc:
        log.debug(exc)
        if _is_not_found(exc):
            log.info("Virtual MFA device %s not found.", serial)
            return True
        log.error("Failed to delete virtual MFA device %s.", serial)
        return False


def update_account_password_policy(
    allow_users_to_change_password=None,
    hard_expiry=None,
    max_password_age=None,
    minimum_password_length=None,
    password_reuse_prevention=None,
    require_lowercase_characters=None,
    require_numbers=None,
    require_symbols=None,
    require_uppercase_characters=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Update the password policy for the AWS account.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.update_account_password_policy True
    """
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    params = {}
    if allow_users_to_change_password is not None:
        params["AllowUsersToChangePassword"] = allow_users_to_change_password
    if hard_expiry is not None:
        params["HardExpiry"] = hard_expiry
    if max_password_age is not None:
        params["MaxPasswordAge"] = max_password_age
    if minimum_password_length is not None:
        params["MinimumPasswordLength"] = minimum_password_length
    if password_reuse_prevention is not None:
        params["PasswordReusePrevention"] = password_reuse_prevention
    if require_lowercase_characters is not None:
        params["RequireLowercaseCharacters"] = require_lowercase_characters
    if require_numbers is not None:
        params["RequireNumbers"] = require_numbers
    if require_symbols is not None:
        params["RequireSymbols"] = require_symbols
    if require_uppercase_characters is not None:
        params["RequireUppercaseCharacters"] = require_uppercase_characters
    try:
        conn.update_account_password_policy(**params)
        log.info("The password policy has been updated.")
        return True
    except ClientError as exc:
        log.debug(exc)
        log.error("Failed to update the password policy")
        return False


def get_account_policy(region=None, key=None, keyid=None, profile=None):
    """
    Get account password policy for the AWS account.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.get_account_policy
    """
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    try:
        res = conn.get_account_password_policy()
        return res.get("PasswordPolicy")
    except ClientError as exc:
        log.debug(exc)
        log.error("Failed to get the password policy.")
        return False


def get_account_id(region=None, key=None, keyid=None, profile=None):
    """
    Get the AWS account id associated with the used credentials.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.get_account_id
    """
    cache_key = "boto3_iam.account_id"
    if cache_key not in __context__:
        try:
            sts = _get_conn("sts", region=region, key=key, keyid=keyid, profile=profile)
            __context__[cache_key] = sts.get_caller_identity()["Account"]
        except ClientError as exc:
            log.debug(exc)
            log.error("Failed to get account id.")
            return None
    return __context__[cache_key]


def upload_server_cert(
    cert_name,
    cert_body,
    private_key,
    cert_chain=None,
    path=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Upload a server certificate.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.upload_server_cert mycert_name crt priv_key
    """
    if get_server_certificate(cert_name, region, key, keyid, profile):
        return True
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    params = {
        "ServerCertificateName": cert_name,
        "CertificateBody": cert_body,
        "PrivateKey": private_key,
    }
    if cert_chain is not None:
        params["CertificateChain"] = cert_chain
    if path is not None:
        params["Path"] = path
    try:
        res = conn.upload_server_certificate(**params)
        log.info("Created certificate %s.", cert_name)
        return res
    except ClientError as exc:
        log.debug(exc)
        log.error("Failed to create certificate %s.", cert_name)
        return False


def get_server_certificate(cert_name, region=None, key=None, keyid=None, profile=None):
    """
    Returns certificate information for a server cert.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.get_server_certificate mycert_name
    """
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    try:
        res = conn.get_server_certificate(ServerCertificateName=cert_name)
        return res.get("ServerCertificate") or False
    except ClientError as exc:
        log.debug(exc)
        log.error("Failed to get certificate %s information.", cert_name)
        return False


def delete_server_cert(cert_name, region=None, key=None, keyid=None, profile=None):
    """
    Deletes a server certificate.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.delete_server_cert mycert_name
    """
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    try:
        return conn.delete_server_certificate(ServerCertificateName=cert_name)
    except ClientError as exc:
        log.debug(exc)
        log.error("Failed to delete certificate %s.", cert_name)
        return False


def export_users(path_prefix="/", region=None, key=None, keyid=None, profile=None):
    """
    Get all IAM user details as a yaml sls structure.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_iam.export_users --out=txt | sed "s/local: //" > iam_users.sls
    """
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    results = odict.OrderedDict()
    users = get_all_users(path_prefix, region, key, keyid, profile)
    for user in users:
        name = user["UserName"]
        res = conn.list_user_policies(UserName=name, MaxItems=100)
        policies = {}
        for policy_name in res.get("PolicyNames", []):
            policy_res = conn.get_user_policy(UserName=name, PolicyName=policy_name)
            policies[policy_name] = _decode_policy_document(policy_res.get("PolicyDocument"))
        user_sls = [
            {"name": name},
            {"policies": policies},
            {"path": user.get("Path")},
        ]
        results["manage user " + name] = {"boto3_iam.user_present": user_sls}
    return salt.utils.yaml.safe_dump(results, default_flow_style=False, indent=2)


def export_roles(path_prefix="/", region=None, key=None, keyid=None, profile=None):
    """
    Get all IAM role details as a yaml sls structure.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_iam.export_roles --out=txt | sed "s/local: //" > iam_roles.sls
    """
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    results = odict.OrderedDict()
    roles = get_all_roles(path_prefix, region, key, keyid, profile)
    for role in roles:
        name = role["RoleName"]
        res = conn.list_role_policies(RoleName=name, MaxItems=100)
        policies = {}
        for policy_name in res.get("PolicyNames", []):
            policy_res = conn.get_role_policy(RoleName=name, PolicyName=policy_name)
            policies[policy_name] = _decode_policy_document(policy_res.get("PolicyDocument"))
        role_sls = [
            {"name": name},
            {"policies": policies},
            {"policy_document": _decode_policy_document(role.get("AssumeRolePolicyDocument"))},
            {"path": role.get("Path")},
        ]
        results["manage role " + name] = {"boto3_iam_role.present": role_sls}
    return salt.utils.yaml.safe_dump(results, default_flow_style=False, indent=2)


def _get_policy_arn(name, region=None, key=None, keyid=None, profile=None):
    if name.startswith("arn:aws:iam:"):
        return name
    account_id = get_account_id(region=region, key=key, keyid=keyid, profile=profile)
    return f"arn:aws:iam::{account_id}:policy/{name}"


def policy_exists(policy_name, region=None, key=None, keyid=None, profile=None):
    """
    Check to see if a managed policy exists.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.policy_exists mypolicy
    """
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.get_policy(
            PolicyArn=_get_policy_arn(
                policy_name, region=region, key=key, keyid=keyid, profile=profile
            )
        )
        return True
    except ClientError:
        return False


def get_policy(policy_name, region=None, key=None, keyid=None, profile=None):
    """
    Get the managed policy info.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.get_policy mypolicy
    """
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    try:
        res = conn.get_policy(
            PolicyArn=_get_policy_arn(
                policy_name, region=region, key=key, keyid=keyid, profile=profile
            )
        )
        return res.get("Policy")
    except ClientError:
        return None


def create_policy(
    policy_name,
    policy_document,
    path=None,
    description=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Create a managed policy.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.create_policy mypolicy '{"Version": "2012-10-17", ...}'
    """
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    if not isinstance(policy_document, str):
        policy_document = salt.utils.json.dumps(policy_document)
    params = {
        "PolicyName": policy_name,
        "PolicyDocument": policy_document,
    }
    if path is not None:
        params["Path"] = path
    if description is not None:
        params["Description"] = description
    if policy_exists(policy_name, region, key, keyid, profile):
        return True
    try:
        conn.create_policy(**params)
        log.info("Created IAM policy %s.", policy_name)
        return True
    except ClientError as exc:
        log.debug(exc)
        log.error("Failed to create IAM policy %s.", policy_name)
        return False


def delete_policy(policy_name, region=None, key=None, keyid=None, profile=None):
    """
    Delete a managed policy.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.delete_policy mypolicy
    """
    policy_arn = _get_policy_arn(policy_name, region, key, keyid, profile)
    if not policy_exists(policy_arn, region, key, keyid, profile):
        return True
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.delete_policy(PolicyArn=policy_arn)
        log.info("Deleted %s policy.", policy_name)
        return True
    except ClientError as exc:
        log.debug(exc)
        log.error("Failed to delete %s policy.", policy_name)
        return False


def list_policies(region=None, key=None, keyid=None, profile=None):
    """
    List managed policies.

    Returns a list whose entries are the ``Policies`` list from each page.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.list_policies
    """
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    try:
        pages = []
        paginator = conn.get_paginator("list_policies")
        for page in paginator.paginate():
            pages.append(page.get("Policies", []))
        return pages
    except ClientError as exc:
        log.debug(exc)
        log.error("Failed to list policies.")
        return []


def policy_version_exists(policy_name, version_id, region=None, key=None, keyid=None, profile=None):
    """
    Check to see if a managed policy version exists.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.policy_version_exists mypolicy v1
    """
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.get_policy_version(
            PolicyArn=_get_policy_arn(policy_name, region, key, keyid, profile),
            VersionId=version_id,
        )
        return True
    except ClientError:
        return False


def get_policy_version(policy_name, version_id, region=None, key=None, keyid=None, profile=None):
    """
    Get a specific version of a managed policy.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.get_policy_version mypolicy v1
    """
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    try:
        res = conn.get_policy_version(
            PolicyArn=_get_policy_arn(
                policy_name, region=region, key=key, keyid=keyid, profile=profile
            ),
            VersionId=version_id,
        )
        pv = res.get("PolicyVersion") or {}
        doc = pv.get("Document")
        if isinstance(doc, str):
            pv["Document"] = urllib.parse.unquote(doc)
        return {"policy_version": pv}
    except ClientError:
        return None


def create_policy_version(
    policy_name,
    policy_document,
    set_as_default=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Create a version of a managed policy.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.create_policy_version mypolicy '{...}'
    """
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    if not isinstance(policy_document, str):
        policy_document = salt.utils.json.dumps(policy_document)
    policy_arn = _get_policy_arn(policy_name, region, key, keyid, profile)
    params = {"PolicyArn": policy_arn, "PolicyDocument": policy_document}
    if set_as_default is not None:
        params["SetAsDefault"] = set_as_default
    try:
        res = conn.create_policy_version(**params)
        vid = res.get("PolicyVersion", {}).get("VersionId")
        log.info("Created IAM policy %s version %s.", policy_name, vid)
        return {"created": True, "version_id": vid}
    except ClientError as exc:
        log.debug(exc)
        log.error("Failed to create IAM policy %s version.", policy_name)
        return {
            "created": False,
            "error": {
                "code": _client_error_code(exc),
                "message": str(exc),
            },
        }


def delete_policy_version(policy_name, version_id, region=None, key=None, keyid=None, profile=None):
    """
    Delete a version of a managed policy.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.delete_policy_version mypolicy v1
    """
    policy_arn = _get_policy_arn(policy_name, region, key, keyid, profile)
    if not policy_version_exists(policy_arn, version_id, region, key, keyid, profile):
        return True
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.delete_policy_version(PolicyArn=policy_arn, VersionId=version_id)
        log.info("Deleted IAM policy %s version %s.", policy_name, version_id)
        return True
    except ClientError as exc:
        log.debug(exc)
        log.error("Failed to delete IAM policy %s version %s.", policy_name, version_id)
        return False


def list_policy_versions(policy_name, region=None, key=None, keyid=None, profile=None):
    """
    List versions of a managed policy.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.list_policy_versions mypolicy
    """
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    policy_arn = _get_policy_arn(policy_name, region, key, keyid, profile)
    try:
        res = conn.list_policy_versions(PolicyArn=policy_arn)
        return res.get("Versions", [])
    except ClientError as exc:
        log.debug(exc)
        log.error("Failed to list versions for IAM policy %s.", policy_name)
        return []


def set_default_policy_version(
    policy_name, version_id, region=None, key=None, keyid=None, profile=None
):
    """
    Set the default version of a managed policy.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.set_default_policy_version mypolicy v1
    """
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    policy_arn = _get_policy_arn(policy_name, region, key, keyid, profile)
    try:
        conn.set_default_policy_version(PolicyArn=policy_arn, VersionId=version_id)
        log.info("Set %s policy to version %s.", policy_name, version_id)
        return True
    except ClientError as exc:
        log.debug(exc)
        log.error("Failed to set %s policy to version %s.", policy_name, version_id)
        return False


def attach_user_policy(policy_name, user_name, region=None, key=None, keyid=None, profile=None):
    """
    Attach a managed policy to a user.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.attach_user_policy mypolicy myuser
    """
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    policy_arn = _get_policy_arn(policy_name, region, key, keyid, profile)
    try:
        conn.attach_user_policy(PolicyArn=policy_arn, UserName=user_name)
        log.info("Attached policy %s to IAM user %s.", policy_name, user_name)
        return True
    except ClientError as exc:
        log.debug(exc)
        log.error("Failed to attach %s policy to IAM user %s.", policy_name, user_name)
        return False


def detach_user_policy(policy_name, user_name, region=None, key=None, keyid=None, profile=None):
    """
    Detach a managed policy from a user.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.detach_user_policy mypolicy myuser
    """
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    policy_arn = _get_policy_arn(policy_name, region, key, keyid, profile)
    try:
        conn.detach_user_policy(PolicyArn=policy_arn, UserName=user_name)
        log.info("Detached %s policy from IAM user %s.", policy_name, user_name)
        return True
    except ClientError as exc:
        log.debug(exc)
        log.error("Failed to detach %s policy from IAM user %s.", policy_name, user_name)
        return False


def attach_group_policy(policy_name, group_name, region=None, key=None, keyid=None, profile=None):
    """
    Attach a managed policy to a group.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.attach_group_policy mypolicy mygroup
    """
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    policy_arn = _get_policy_arn(policy_name, region, key, keyid, profile)
    try:
        conn.attach_group_policy(PolicyArn=policy_arn, GroupName=group_name)
        log.info("Attached policy %s to IAM group %s.", policy_name, group_name)
        return True
    except ClientError as exc:
        log.debug(exc)
        log.error("Failed to attach policy %s to IAM group %s.", policy_name, group_name)
        return False


def detach_group_policy(policy_name, group_name, region=None, key=None, keyid=None, profile=None):
    """
    Detach a managed policy from a group.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.detach_group_policy mypolicy mygroup
    """
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    policy_arn = _get_policy_arn(policy_name, region, key, keyid, profile)
    try:
        conn.detach_group_policy(PolicyArn=policy_arn, GroupName=group_name)
        log.info("Detached policy %s from IAM group %s.", policy_name, group_name)
        return True
    except ClientError as exc:
        log.debug(exc)
        log.error("Failed to detach policy %s from IAM group %s.", policy_name, group_name)
        return False


def attach_role_policy(policy_name, role_name, region=None, key=None, keyid=None, profile=None):
    """
    Attach a managed policy to a role.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.attach_role_policy mypolicy myrole
    """
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    policy_arn = _get_policy_arn(policy_name, region, key, keyid, profile)
    try:
        conn.attach_role_policy(PolicyArn=policy_arn, RoleName=role_name)
        log.info("Attached policy %s to IAM role %s.", policy_name, role_name)
        return True
    except ClientError as exc:
        log.debug(exc)
        log.error("Failed to attach policy %s to IAM role %s.", policy_name, role_name)
        return False


def detach_role_policy(policy_name, role_name, region=None, key=None, keyid=None, profile=None):
    """
    Detach a managed policy from a role.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.detach_role_policy mypolicy myrole
    """
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    policy_arn = _get_policy_arn(policy_name, region, key, keyid, profile)
    try:
        conn.detach_role_policy(PolicyArn=policy_arn, RoleName=role_name)
        log.info("Detached policy %s from IAM role %s.", policy_name, role_name)
        return True
    except ClientError as exc:
        log.debug(exc)
        log.error("Failed to detach policy %s from IAM role %s.", policy_name, role_name)
        return False


def list_entities_for_policy(
    policy_name,
    path_prefix=None,
    entity_filter=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    List entities (users, groups, roles) that a policy is attached to.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.list_entities_for_policy mypolicy
    """
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    retries = 30
    policy_arn = _get_policy_arn(policy_name, region, key, keyid, profile)
    base_params = {"PolicyArn": policy_arn}
    if path_prefix is not None:
        base_params["PathPrefix"] = path_prefix
    if entity_filter is not None:
        base_params["EntityFilter"] = entity_filter
    while retries:
        try:
            allret = {
                "policy_groups": [],
                "policy_users": [],
                "policy_roles": [],
            }
            paginator = conn.get_paginator("list_entities_for_policy")
            for page in paginator.paginate(**base_params):
                allret["policy_groups"].extend(page.get("PolicyGroups", []))
                allret["policy_users"].extend(page.get("PolicyUsers", []))
                allret["policy_roles"].extend(page.get("PolicyRoles", []))
            return allret
        except ClientError as exc:
            if _client_error_code(exc) == "Throttling":
                log.debug("Throttled by AWS API, will retry in 5 seconds...")
                time.sleep(5)
                retries -= 1
                continue
            log.error("Failed to list entities for IAM policy %s.", policy_name)
            return {}
    return {}


def list_attached_user_policies(
    user_name,
    path_prefix=None,
    entity_filter=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):  # pylint: disable=unused-argument
    """
    List managed policies attached to the given user.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.list_attached_user_policies myuser
    """
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    params = {"UserName": user_name}
    if path_prefix is not None:
        params["PathPrefix"] = path_prefix
    try:
        policies = []
        paginator = conn.get_paginator("list_attached_user_policies")
        for page in paginator.paginate(**params):
            policies.extend(page.get("AttachedPolicies", []))
        return policies
    except ClientError as exc:
        log.debug(exc)
        log.error("Failed to list attached policies for IAM user %s.", user_name)
        return []


def list_attached_group_policies(
    group_name,
    path_prefix=None,
    entity_filter=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):  # pylint: disable=unused-argument
    """
    List managed policies attached to the given group.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.list_attached_group_policies mygroup
    """
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    params = {"GroupName": group_name}
    if path_prefix is not None:
        params["PathPrefix"] = path_prefix
    try:
        policies = []
        paginator = conn.get_paginator("list_attached_group_policies")
        for page in paginator.paginate(**params):
            policies.extend(page.get("AttachedPolicies", []))
        return policies
    except ClientError as exc:
        log.debug(exc)
        log.error("Failed to list attached policies for IAM group %s.", group_name)
        return []


def list_attached_role_policies(
    role_name,
    path_prefix=None,
    entity_filter=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):  # pylint: disable=unused-argument
    """
    List managed policies attached to the given role.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.list_attached_role_policies myrole
    """
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    params = {"RoleName": role_name}
    if path_prefix is not None:
        params["PathPrefix"] = path_prefix
    try:
        policies = []
        paginator = conn.get_paginator("list_attached_role_policies")
        for page in paginator.paginate(**params):
            policies.extend(page.get("AttachedPolicies", []))
        return policies
    except ClientError as exc:
        log.debug(exc)
        log.error("Failed to list attached policies for IAM role %s.", role_name)
        return []


def create_saml_provider(
    name, saml_metadata_document, region=None, key=None, keyid=None, profile=None
):
    """
    Create SAML provider.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.create_saml_provider name saml_metadata_document
    """
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.create_saml_provider(Name=name, SAMLMetadataDocument=saml_metadata_document)
        log.info("Successfully created %s SAML provider.", name)
        return True
    except ClientError as exc:
        log.debug(exc)
        log.error("Failed to create SAML provider %s.", name)
        return False


def get_saml_provider_arn(name, region=None, key=None, keyid=None, profile=None):
    """
    Get SAML provider ARN.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.get_saml_provider_arn my_saml_provider_name
    """
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    try:
        res = conn.list_saml_providers()
        for sp in res.get("SAMLProviderList", []):
            if sp.get("Arn", "").endswith(":saml-provider/" + name):
                return sp["Arn"]
        return False
    except ClientError as exc:
        log.debug(exc)
        log.error("Failed to get ARN of SAML provider %s.", name)
        return False


def delete_saml_provider(name, region=None, key=None, keyid=None, profile=None):
    """
    Delete SAML provider.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.delete_saml_provider my_saml_provider_name
    """
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    try:
        arn = get_saml_provider_arn(name, region=region, key=key, keyid=keyid, profile=profile)
        if not arn:
            log.info("SAML provider %s not found.", name)
            return True
        conn.delete_saml_provider(SAMLProviderArn=arn)
        log.info("Successfully deleted SAML provider %s.", name)
        return True
    except ClientError as exc:
        log.debug(exc)
        log.error("Failed to delete SAML provider %s.", name)
        return False


def list_saml_providers(region=None, key=None, keyid=None, profile=None):
    """
    List SAML provider names.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.list_saml_providers
    """
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    try:
        providers = []
        res = conn.list_saml_providers()
        for sp in res.get("SAMLProviderList", []):
            arn = sp.get("Arn", "")
            if arn:
                providers.append(arn.rsplit("/", 1)[1])
        return providers
    except ClientError as exc:
        log.debug(exc)
        log.error("Failed to get list of SAML providers.")
        return False


def get_saml_provider(name, region=None, key=None, keyid=None, profile=None):
    """
    Get SAML provider metadata document.

    ``name`` may be a provider ARN.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.get_saml_provider arn
    """
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    try:
        res = conn.get_saml_provider(SAMLProviderArn=name)
        return res.get("SAMLMetadataDocument")
    except ClientError as exc:
        log.debug(exc)
        log.error("Failed to get SAML provider document %s.", name)
        return False


def update_saml_provider(
    name, saml_metadata_document, region=None, key=None, keyid=None, profile=None
):
    """
    Update SAML provider.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_iam.update_saml_provider name saml_metadata_document
    """
    conn = _get_conn("iam", region=region, key=key, keyid=keyid, profile=profile)
    try:
        arn = get_saml_provider_arn(name, region=region, key=key, keyid=keyid, profile=profile)
        if not arn:
            log.info("SAML provider %s not found.", name)
            return False
        conn.update_saml_provider(SAMLProviderArn=arn, SAMLMetadataDocument=saml_metadata_document)
        return True
    except ClientError as exc:
        log.debug(exc)
        log.error("Failed to update SAML provider %s.", name)
        return False
