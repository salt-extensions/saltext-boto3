"""
Connection module for Amazon KMS using boto3.
=============================================

    Renamed from ``boto_kms`` to ``boto3_kms`` and rewritten to use the
    boto3 ``kms`` client APIs directly via
    :py:mod:`saltext.boto3.utils.boto3mod`.  The legacy boto2 code path
    (object-style access, retry loops) has been removed.

:depends:
  - boto3 >= 1.28.0
  - botocore >= 1.31.0

:configuration: This module accepts explicit kms credentials but can
    also utilize IAM roles assigned to the instance through Instance Profiles.
    Dynamic credentials are then automatically obtained from AWS API and no
    further configuration is necessary. More Information available at:

    .. code-block:: text

        http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html

    If IAM roles are not used you need to specify them either in the minion's
    config file or as a profile. For example, to specify them in the minion's
    config file:

.. code-block:: yaml

    kms.keyid: GKTADJGHEIQSXMKKRBJ08H
    kms.key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs

A region may also be specified in the configuration:

.. code-block:: yaml

    kms.region: us-east-1

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

import salt.serializers.json
from salt.utils import odict

from saltext.boto3.utils import boto3mod

log = logging.getLogger(__name__)


try:
    from botocore.exceptions import ClientError

    logging.getLogger("boto3").setLevel(logging.CRITICAL)
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False


__virtualname__ = "boto3_kms"


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
    return (False, "The boto3_kms module could not be loaded: boto3 is not available.")


def _err(e):
    return boto3mod.get_error(e)


def _resolve_alias(key_id, region=None, key=None, keyid=None, profile=None):
    """
    If ``key_id`` is an alias, resolve it to a KeyId via describe_key.
    """
    if key_id.startswith("alias/"):
        r = describe_key(key_id, region=region, key=key, keyid=keyid, profile=profile)
        if "key_metadata" in r:
            return r["key_metadata"]["KeyId"]
    return key_id


def create_alias(alias_name, target_key_id, region=None, key=None, keyid=None, profile=None):
    """
    Create a display name for a key.

    alias_name (str):
        The display name for the key, must start with "alias/".

    target_key_id (str):
        The ID of the key for which to create the alias.

    region (str, optional):
        The AWS region where the KMS key is located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_kms.create_alias 'alias/mykey' key_id
    """
    conn = _get_conn("kms", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.create_alias(AliasName=alias_name, TargetKeyId=target_key_id)
        return {"result": True}
    except ClientError as e:
        return {"result": False, "error": _err(e)}


def create_grant(
    key_id,
    grantee_principal,
    retiring_principal=None,
    operations=None,
    constraints=None,
    grant_tokens=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Add a grant to a key.

    key_id (str):
        The ID or alias of the key to which the grant will be added.

    grantee_principal (str):
        The principal that will be given the permissions specified in the grant.

    retiring_principal (str, optional):
        The principal that can retire the grant.

    operations (list, optional):
        A list of operations that the grantee principal is allowed to perform.

    constraints (dict, optional):
        A dictionary specifying the conditions under which the grant is valid.

    grant_tokens (list, optional):
        A list of grant tokens.

    region (str, optional):
        The AWS region where the KMS key is located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_kms.create_grant 'alias/mykey' 'arn:aws:iam::1:role/r' operations='["Encrypt","Decrypt"]'
    """
    conn = _get_conn("kms", region=region, key=key, keyid=keyid, profile=profile)
    key_id = _resolve_alias(key_id, region, key, keyid, profile)
    kwargs = {"KeyId": key_id, "GranteePrincipal": grantee_principal}
    if retiring_principal is not None:
        kwargs["RetiringPrincipal"] = retiring_principal
    if operations is not None:
        kwargs["Operations"] = operations
    if constraints is not None:
        kwargs["Constraints"] = constraints
    if grant_tokens is not None:
        kwargs["GrantTokens"] = grant_tokens
    try:
        return {"grant": conn.create_grant(**kwargs)}
    except ClientError as e:
        return {"error": _err(e)}


def create_key(
    policy=None,
    description=None,
    key_usage=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Create a customer master key.

    policy (dict, optional):
        The key policy to attach to the new key.

    description (str, optional):
        A description for the new key.

    key_usage (str, optional):
        The intended use of the key. For example, "ENCRYPT_DECRYPT".

    region (str, optional):
        The AWS region where the KMS key is located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_kms.create_key '{"Statement":...}' "My master key"
    """
    conn = _get_conn("kms", region=region, key=key, keyid=keyid, profile=profile)
    kwargs = {}
    if policy is not None:
        kwargs["Policy"] = salt.serializers.json.serialize(policy)
    if description is not None:
        kwargs["Description"] = description
    if key_usage is not None:
        kwargs["KeyUsage"] = key_usage
    try:
        res = conn.create_key(**kwargs)
        return {"key_metadata": res["KeyMetadata"]}
    except ClientError as e:
        return {"error": _err(e)}


def decrypt(
    ciphertext_blob,
    encryption_context=None,
    grant_tokens=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Decrypt ciphertext.

    ciphertext_blob (bytes):
        The encrypted data to be decrypted.

    encryption_context (dict, optional):
        The encryption context used when encrypting the data.

    grant_tokens (list, optional):
        A list of grant tokens.

    region (str, optional):
        The AWS region where the KMS key is located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_kms.decrypt encrypted_ciphertext
    """
    conn = _get_conn("kms", region=region, key=key, keyid=keyid, profile=profile)
    kwargs = {"CiphertextBlob": ciphertext_blob}
    if encryption_context is not None:
        kwargs["EncryptionContext"] = encryption_context
    if grant_tokens is not None:
        kwargs["GrantTokens"] = grant_tokens
    try:
        res = conn.decrypt(**kwargs)
        return {"plaintext": res["Plaintext"]}
    except ClientError as e:
        return {"error": _err(e)}


def key_exists(key_id, region=None, key=None, keyid=None, profile=None):
    """
    Check whether a KMS key exists.

    key_id (str):
        The ID or alias of the key to check for existence.

    region (str, optional):
        The AWS region where the KMS key is located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_kms.key_exists 'alias/mykey'
    """
    conn = _get_conn("kms", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.describe_key(KeyId=key_id)
        return {"result": True}
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("NotFoundException", "NotFound"):
            return {"result": False}
        return {"error": _err(e)}


def describe_key(key_id, region=None, key=None, keyid=None, profile=None):
    """
    Get detailed information about a key.

    key_id (str):
        The ID or alias of the key to describe.

    region (str, optional):
        The AWS region where the KMS key is located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_kms.describe_key 'alias/mykey'
    """
    conn = _get_conn("kms", region=region, key=key, keyid=keyid, profile=profile)
    try:
        res = conn.describe_key(KeyId=key_id)
        return {"key_metadata": res["KeyMetadata"]}
    except ClientError as e:
        return {"error": _err(e)}


def disable_key(key_id, region=None, key=None, keyid=None, profile=None):
    """
    Mark a key as disabled.

    key_id (str):
        The ID or alias of the key to disable.

    region (str, optional):
        The AWS region where the KMS key is located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_kms.disable_key 'alias/mykey'
    """
    conn = _get_conn("kms", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.disable_key(KeyId=key_id)
        return {"result": True}
    except ClientError as e:
        return {"result": False, "error": _err(e)}


def disable_key_rotation(key_id, region=None, key=None, keyid=None, profile=None):
    """
    Disable key rotation for a key.

    key_id (str):
        The ID or alias of the key to disable key rotation.

    region (str, optional):
        The AWS region where the KMS key is located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_kms.disable_key_rotation 'alias/mykey'
    """
    conn = _get_conn("kms", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.disable_key_rotation(KeyId=key_id)
        return {"result": True}
    except ClientError as e:
        return {"result": False, "error": _err(e)}


def enable_key(key_id, region=None, key=None, keyid=None, profile=None):
    """
    Mark a key as enabled.

    key_id (str):
        The ID or alias of the key to enable.

    region (str, optional):
        The AWS region where the KMS key is located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_kms.enable_key 'alias/mykey'
    """
    conn = _get_conn("kms", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.enable_key(KeyId=key_id)
        return {"result": True}
    except ClientError as e:
        return {"result": False, "error": _err(e)}


def enable_key_rotation(key_id, region=None, key=None, keyid=None, profile=None):
    """
    Enable key rotation for a key.

    key_id (str):
        The ID or alias of the key to enable key rotation.

    region (str, optional):
        The AWS region where the KMS key is located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_kms.enable_key_rotation 'alias/mykey'
    """
    conn = _get_conn("kms", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.enable_key_rotation(KeyId=key_id)
        return {"result": True}
    except ClientError as e:
        return {"result": False, "error": _err(e)}


def encrypt(
    key_id,
    plaintext,
    encryption_context=None,
    grant_tokens=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Encrypt plaintext using a KMS key.

    key_id (str):
        The ID or alias of the key to use for encryption.

    plaintext (str):
        The plaintext data to encrypt.

    encryption_context (dict, optional):
        The encryption context to use for encryption.

    grant_tokens (list, optional):
        The grant tokens to use for encryption.

    region (str, optional):
        The AWS region where the KMS key is located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_kms.encrypt 'alias/mykey' 'myplaindata'
    """
    conn = _get_conn("kms", region=region, key=key, keyid=keyid, profile=profile)
    kwargs = {"KeyId": key_id, "Plaintext": plaintext}
    if encryption_context is not None:
        kwargs["EncryptionContext"] = encryption_context
    if grant_tokens is not None:
        kwargs["GrantTokens"] = grant_tokens
    try:
        res = conn.encrypt(**kwargs)
        return {"ciphertext": res["CiphertextBlob"]}
    except ClientError as e:
        return {"error": _err(e)}


def generate_data_key(
    key_id,
    encryption_context=None,
    number_of_bytes=None,
    key_spec=None,
    grant_tokens=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Generate a secure data key.

    key_id (str):
        The ID or alias of the key to use for generating the data key.

    encryption_context (dict, optional):
        The encryption context to use for generating the data key.

    number_of_bytes (int, optional):
        The length of the data key in bytes.

    key_spec (str, optional):
        The specification of the data key (e.g., AES_128, AES_256).

    grant_tokens (list, optional):
        The grant tokens to use for generating the data key.

    region (str, optional):
        The AWS region where the KMS key is located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_kms.generate_data_key 'alias/mykey' number_of_bytes=1024 key_spec=AES_128
    """
    conn = _get_conn("kms", region=region, key=key, keyid=keyid, profile=profile)
    kwargs = {"KeyId": key_id}
    if encryption_context is not None:
        kwargs["EncryptionContext"] = encryption_context
    if number_of_bytes is not None:
        kwargs["NumberOfBytes"] = number_of_bytes
    if key_spec is not None:
        kwargs["KeySpec"] = key_spec
    if grant_tokens is not None:
        kwargs["GrantTokens"] = grant_tokens
    try:
        return {"data_key": conn.generate_data_key(**kwargs)}
    except ClientError as e:
        return {"error": _err(e)}


def generate_data_key_without_plaintext(
    key_id,
    encryption_context=None,
    number_of_bytes=None,
    key_spec=None,
    grant_tokens=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Generate a secure data key without a plaintext copy.

    key_id (str):
        The ID or alias of the key to use for generating the data key.

    encryption_context (dict, optional):
        The encryption context to use for generating the data key.

    number_of_bytes (int, optional):
        The length of the data key in bytes.

    key_spec (str, optional):
        The specification of the data key (e.g., AES_128, AES_256).

    grant_tokens (list, optional):
        The grant tokens to use for generating the data key.

    region (str, optional):
        The AWS region where the KMS key is located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_kms.generate_data_key_without_plaintext 'alias/mykey' number_of_bytes=1024
    """
    conn = _get_conn("kms", region=region, key=key, keyid=keyid, profile=profile)
    kwargs = {"KeyId": key_id}
    if encryption_context is not None:
        kwargs["EncryptionContext"] = encryption_context
    if number_of_bytes is not None:
        kwargs["NumberOfBytes"] = number_of_bytes
    if key_spec is not None:
        kwargs["KeySpec"] = key_spec
    if grant_tokens is not None:
        kwargs["GrantTokens"] = grant_tokens
    try:
        return {"data_key": conn.generate_data_key_without_plaintext(**kwargs)}
    except ClientError as e:
        return {"error": _err(e)}


def generate_random(number_of_bytes=None, region=None, key=None, keyid=None, profile=None):
    """
    Generate cryptographically secure random bytes.

    number_of_bytes (int, optional):
        The length of the random data to generate.

    region (str, optional):
        The AWS region where the KMS key is located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_kms.generate_random number_of_bytes=1024
    """
    conn = _get_conn("kms", region=region, key=key, keyid=keyid, profile=profile)
    kwargs = {}
    if number_of_bytes is not None:
        kwargs["NumberOfBytes"] = number_of_bytes
    try:
        res = conn.generate_random(**kwargs)
        return {"random": res["Plaintext"]}
    except ClientError as e:
        return {"error": _err(e)}


def get_key_policy(key_id, policy_name, region=None, key=None, keyid=None, profile=None):
    """
    Get the policy for the specified key.

    key_id (str):
        The ID or alias of the key for which to retrieve the policy.

    policy_name (str):
        The name of the policy to retrieve.

    region (str, optional):
        The AWS region where the KMS key is located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_kms.get_key_policy 'alias/mykey' default
    """
    conn = _get_conn("kms", region=region, key=key, keyid=keyid, profile=profile)
    try:
        res = conn.get_key_policy(KeyId=key_id, PolicyName=policy_name)
        return {
            "key_policy": salt.serializers.json.deserialize(
                res["Policy"], object_pairs_hook=odict.OrderedDict
            )
        }
    except ClientError as e:
        return {"error": _err(e)}


def get_key_rotation_status(key_id, region=None, key=None, keyid=None, profile=None):
    """
    Return whether key rotation is enabled for the specified key.

    key_id (str):
        The ID or alias of the key for which to check the rotation status.

    region (str, optional):
        The AWS region where the KMS key is located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_kms.get_key_rotation_status 'alias/mykey'
    """
    conn = _get_conn("kms", region=region, key=key, keyid=keyid, profile=profile)
    try:
        res = conn.get_key_rotation_status(KeyId=key_id)
        return {"result": res["KeyRotationEnabled"]}
    except ClientError as e:
        return {"error": _err(e)}


def list_grants(key_id, limit=None, marker=None, region=None, key=None, keyid=None, profile=None):
    """
    List grants for the specified key.

    key_id (str):
        The ID or alias of the key for which to list grants.

    limit (int, optional):
        The maximum number of grants to return.

    marker (str, optional):
        The marker for paginated results.

    region (str, optional):
        The AWS region where the KMS key is located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_kms.list_grants 'alias/mykey'
    """
    conn = _get_conn("kms", region=region, key=key, keyid=keyid, profile=profile)
    key_id = _resolve_alias(key_id, region, key, keyid, profile)
    grants = []
    next_marker = marker
    try:
        while True:
            kwargs = {"KeyId": key_id}
            if limit is not None:
                kwargs["Limit"] = limit
            if next_marker is not None:
                kwargs["Marker"] = next_marker
            res = conn.list_grants(**kwargs)
            grants.extend(res.get("Grants", []))
            if "NextMarker" in res and res.get("Truncated"):
                next_marker = res["NextMarker"]
            else:
                break
        return {"grants": grants}
    except ClientError as e:
        return {"error": _err(e)}


def list_key_policies(
    key_id, limit=None, marker=None, region=None, key=None, keyid=None, profile=None
):
    """
    List key policies for the specified key.

    key_id (str):
        The ID or alias of the key for which to list key policies.

    limit (int, optional):
        The maximum number of key policies to return.

    marker (str, optional):
        The marker for paginated results.

    region (str, optional):
        The AWS region where the KMS key is located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_kms.list_key_policies 'alias/mykey'
    """
    conn = _get_conn("kms", region=region, key=key, keyid=keyid, profile=profile)
    key_id = _resolve_alias(key_id, region, key, keyid, profile)
    kwargs = {"KeyId": key_id}
    if limit is not None:
        kwargs["Limit"] = limit
    if marker is not None:
        kwargs["Marker"] = marker
    try:
        res = conn.list_key_policies(**kwargs)
        return {"key_policies": res["PolicyNames"]}
    except ClientError as e:
        return {"error": _err(e)}


def put_key_policy(key_id, policy_name, policy, region=None, key=None, keyid=None, profile=None):
    """
    Attach a key policy to the specified key.

    key_id (str):
        The ID or alias of the key to which to attach the policy.

    policy_name (str):
        The name of the policy to attach.

    policy (str):
        The policy document to attach.

    region (str, optional):
        The AWS region where the KMS key is located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_kms.put_key_policy 'alias/mykey' default '{"Statement":...}'
    """
    conn = _get_conn("kms", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.put_key_policy(
            KeyId=key_id,
            PolicyName=policy_name,
            Policy=salt.serializers.json.serialize(policy),
        )
        return {"result": True}
    except ClientError as e:
        return {"result": False, "error": _err(e)}


def re_encrypt(
    ciphertext_blob,
    destination_key_id,
    source_encryption_context=None,
    destination_encryption_context=None,
    grant_tokens=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Re-encrypt ciphertext with a new master key.

    ciphertext_blob (str):
        The encrypted data to re-encrypt.

    destination_key_id (str):
        The ID or alias of the new KMS key to use for re-encryption.

    source_encryption_context (dict, optional):
        The encryption context used when the ciphertext was originally encrypted.

    destination_encryption_context (dict, optional):
        The encryption context to use for the re-encrypted ciphertext.

    grant_tokens (list, optional):
        A list of grant tokens to use for the re-encryption operation.

    region (str, optional):
        The AWS region where the KMS key is located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_kms.re_encrypt 'encrypted_data' 'alias/mynewkey'
    """
    conn = _get_conn("kms", region=region, key=key, keyid=keyid, profile=profile)
    kwargs = {
        "CiphertextBlob": ciphertext_blob,
        "DestinationKeyId": destination_key_id,
    }
    if source_encryption_context is not None:
        kwargs["SourceEncryptionContext"] = source_encryption_context
    if destination_encryption_context is not None:
        kwargs["DestinationEncryptionContext"] = destination_encryption_context
    if grant_tokens is not None:
        kwargs["GrantTokens"] = grant_tokens
    try:
        return {"ciphertext": conn.re_encrypt(**kwargs)}
    except ClientError as e:
        return {"error": _err(e)}


def revoke_grant(key_id, grant_id, region=None, key=None, keyid=None, profile=None):
    """
    Revoke a grant from a key.

    key_id (str):
        The ID or alias of the key from which to revoke the grant.

    grant_id (str):
        The ID of the grant to revoke.

    region (str, optional):
        The AWS region where the KMS key is located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_kms.revoke_grant 'alias/mykey' 8u89hf-j09j...
    """
    conn = _get_conn("kms", region=region, key=key, keyid=keyid, profile=profile)
    key_id = _resolve_alias(key_id, region, key, keyid, profile)
    try:
        conn.revoke_grant(KeyId=key_id, GrantId=grant_id)
        return {"result": True}
    except ClientError as e:
        return {"result": False, "error": _err(e)}


def update_key_description(key_id, description, region=None, key=None, keyid=None, profile=None):
    """
    Update a key's description.

    key_id (str):
        The ID or alias of the key to update.

    description (str):
        The new description for the key.

    region (str, optional):
        The AWS region where the KMS key is located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_kms.update_key_description 'alias/mykey' 'My key'
    """
    conn = _get_conn("kms", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.update_key_description(KeyId=key_id, Description=description)
        return {"result": True}
    except ClientError as e:
        return {"result": False, "error": _err(e)}
