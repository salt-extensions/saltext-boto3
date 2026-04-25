"""
Unit tests for the ``boto3_kms`` execution module.
"""

import pytest

from saltext.boto3.modules import boto3_kms

try:
    import botocore  # pylint: disable=unused-import

    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

pytestmark = [
    pytest.mark.skip_on_fips_enabled_platform,
    pytest.mark.skipif(HAS_BOTO3 is False, reason="The boto3 module must be installed."),
]


@pytest.fixture
def configure_loader_modules():
    return {
        boto3_kms: {
            "__opts__": {},
            "__context__": {},
            "__salt__": {},
        }
    }


@pytest.fixture
def conn(make_conn):
    with make_conn(boto3_kms) as client:
        yield client


def test_create_alias(conn):
    r = boto3_kms.create_alias("alias/mykey", "key-id")
    assert r == {"result": True}
    conn.create_alias.assert_called_once_with(AliasName="alias/mykey", TargetKeyId="key-id")


def test_create_alias_error(conn, client_error):
    conn.create_alias.side_effect = client_error("InternalFailure", "CreateAlias")
    r = boto3_kms.create_alias("alias/mykey", "key-id")
    assert r["result"] is False
    assert "error" in r


def test_create_key_minimal(conn):
    conn.create_key.return_value = {"KeyMetadata": {"KeyId": "abc"}}
    r = boto3_kms.create_key()
    assert r == {"key_metadata": {"KeyId": "abc"}}
    conn.create_key.assert_called_once_with()


def test_create_key_with_args(conn):
    conn.create_key.return_value = {"KeyMetadata": {"KeyId": "abc"}}
    r = boto3_kms.create_key(policy={"a": 1}, description="d", key_usage="ENCRYPT_DECRYPT")
    assert r["key_metadata"] == {"KeyId": "abc"}
    kwargs = conn.create_key.call_args.kwargs
    assert kwargs["Description"] == "d"
    assert kwargs["KeyUsage"] == "ENCRYPT_DECRYPT"
    assert "Policy" in kwargs


def test_create_key_error(conn, client_error):
    conn.create_key.side_effect = client_error("InternalFailure", "CreateKey")
    r = boto3_kms.create_key()
    assert "error" in r


def test_decrypt(conn):
    conn.decrypt.return_value = {"Plaintext": b"plain"}
    r = boto3_kms.decrypt(b"cipher", encryption_context={"a": "b"}, grant_tokens=["t"])
    assert r == {"plaintext": b"plain"}
    kwargs = conn.decrypt.call_args.kwargs
    assert kwargs["CiphertextBlob"] == b"cipher"
    assert kwargs["EncryptionContext"] == {"a": "b"}
    assert kwargs["GrantTokens"] == ["t"]


def test_decrypt_error(conn, client_error):
    conn.decrypt.side_effect = client_error("InternalFailure", "Decrypt")
    r = boto3_kms.decrypt(b"cipher")
    assert "error" in r


def test_encrypt(conn):
    conn.encrypt.return_value = {"CiphertextBlob": b"cipher"}
    r = boto3_kms.encrypt("key-id", b"plain", encryption_context={"a": "b"})
    assert r == {"ciphertext": b"cipher"}


def test_encrypt_error(conn, client_error):
    conn.encrypt.side_effect = client_error("InternalFailure", "Encrypt")
    assert "error" in boto3_kms.encrypt("key-id", b"plain")


def test_re_encrypt(conn):
    conn.re_encrypt.return_value = {"CiphertextBlob": b"c"}
    r = boto3_kms.re_encrypt(
        b"cipher",
        "dest-key",
        source_encryption_context={"a": "b"},
        destination_encryption_context={"c": "d"},
        grant_tokens=["t"],
    )
    assert "ciphertext" in r


def test_re_encrypt_error(conn, client_error):
    conn.re_encrypt.side_effect = client_error("InternalFailure", "ReEncrypt")
    assert "error" in boto3_kms.re_encrypt(b"c", "dest")


def test_key_exists_true(conn):
    conn.describe_key.return_value = {"KeyMetadata": {"KeyId": "k"}}
    assert boto3_kms.key_exists("alias/k") == {"result": True}


def test_key_exists_false(conn, client_error):
    conn.describe_key.side_effect = client_error("NotFoundException", "DescribeKey")
    assert boto3_kms.key_exists("alias/k") == {"result": False}


def test_key_exists_error(conn, client_error):
    conn.describe_key.side_effect = client_error("AccessDenied", "DescribeKey")
    r = boto3_kms.key_exists("alias/k")
    assert "error" in r


def test_describe_key(conn):
    conn.describe_key.return_value = {"KeyMetadata": {"KeyId": "k"}}
    assert boto3_kms.describe_key("alias/k") == {"key_metadata": {"KeyId": "k"}}


def test_describe_key_error(conn, client_error):
    conn.describe_key.side_effect = client_error("InternalFailure", "DescribeKey")
    assert "error" in boto3_kms.describe_key("alias/k")


@pytest.mark.parametrize(
    "fn,api",
    [
        ("disable_key", "disable_key"),
        ("disable_key_rotation", "disable_key_rotation"),
        ("enable_key", "enable_key"),
        ("enable_key_rotation", "enable_key_rotation"),
    ],
)
def test_enable_disable(conn, fn, api):
    r = getattr(boto3_kms, fn)("key-id")
    assert r == {"result": True}
    getattr(conn, api).assert_called_once_with(KeyId="key-id")


@pytest.mark.parametrize(
    "fn", ["disable_key", "disable_key_rotation", "enable_key", "enable_key_rotation"]
)
def test_enable_disable_error(conn, client_error, fn):
    getattr(conn, fn).side_effect = client_error("InternalFailure", "Op")
    r = getattr(boto3_kms, fn)("key-id")
    assert r["result"] is False


def test_generate_data_key(conn):
    conn.generate_data_key.return_value = {"CiphertextBlob": b"c", "Plaintext": b"p"}
    r = boto3_kms.generate_data_key("key-id", number_of_bytes=32, key_spec="AES_256")
    assert "data_key" in r
    kwargs = conn.generate_data_key.call_args.kwargs
    assert kwargs["NumberOfBytes"] == 32
    assert kwargs["KeySpec"] == "AES_256"


def test_generate_data_key_error(conn, client_error):
    conn.generate_data_key.side_effect = client_error("InternalFailure", "GenerateDataKey")
    assert "error" in boto3_kms.generate_data_key("key-id")


def test_generate_data_key_without_plaintext(conn):
    conn.generate_data_key_without_plaintext.return_value = {"CiphertextBlob": b"c"}
    r = boto3_kms.generate_data_key_without_plaintext("key-id", key_spec="AES_256")
    assert "data_key" in r


def test_generate_data_key_without_plaintext_error(conn, client_error):
    conn.generate_data_key_without_plaintext.side_effect = client_error(
        "InternalFailure", "GenerateDataKeyWithoutPlaintext"
    )
    assert "error" in boto3_kms.generate_data_key_without_plaintext("key-id")


def test_generate_random(conn):
    conn.generate_random.return_value = {"Plaintext": b"rand"}
    r = boto3_kms.generate_random(number_of_bytes=16)
    assert r == {"random": b"rand"}
    conn.generate_random.assert_called_once_with(NumberOfBytes=16)


def test_generate_random_error(conn, client_error):
    conn.generate_random.side_effect = client_error("InternalFailure", "GenerateRandom")
    assert "error" in boto3_kms.generate_random()


def test_get_key_policy(conn):
    conn.get_key_policy.return_value = {"Policy": '{"Version":"2012-10-17","a":1}'}
    r = boto3_kms.get_key_policy("key-id", "default")
    assert r["key_policy"]["a"] == 1
    conn.get_key_policy.assert_called_once_with(KeyId="key-id", PolicyName="default")


def test_get_key_policy_error(conn, client_error):
    conn.get_key_policy.side_effect = client_error("InternalFailure", "GetKeyPolicy")
    assert "error" in boto3_kms.get_key_policy("key-id", "default")


def test_get_key_rotation_status(conn):
    conn.get_key_rotation_status.return_value = {"KeyRotationEnabled": True}
    assert boto3_kms.get_key_rotation_status("key-id") == {"result": True}


def test_get_key_rotation_status_error(conn, client_error):
    conn.get_key_rotation_status.side_effect = client_error(
        "InternalFailure", "GetKeyRotationStatus"
    )
    assert "error" in boto3_kms.get_key_rotation_status("key-id")


def test_list_key_policies(conn):
    conn.list_key_policies.return_value = {"PolicyNames": ["default"]}
    r = boto3_kms.list_key_policies("key-id", limit=10, marker="m")
    assert r == {"key_policies": ["default"]}
    kwargs = conn.list_key_policies.call_args.kwargs
    assert kwargs["Limit"] == 10
    assert kwargs["Marker"] == "m"


def test_list_key_policies_error(conn, client_error):
    conn.list_key_policies.side_effect = client_error("InternalFailure", "ListKeyPolicies")
    assert "error" in boto3_kms.list_key_policies("key-id")


def test_put_key_policy(conn):
    r = boto3_kms.put_key_policy("key-id", "default", {"Version": "2012-10-17"})
    assert r == {"result": True}
    kwargs = conn.put_key_policy.call_args.kwargs
    assert kwargs["KeyId"] == "key-id"
    assert kwargs["PolicyName"] == "default"
    assert isinstance(kwargs["Policy"], str)


def test_put_key_policy_error(conn, client_error):
    conn.put_key_policy.side_effect = client_error("InternalFailure", "PutKeyPolicy")
    r = boto3_kms.put_key_policy("key-id", "default", {})
    assert r["result"] is False


def test_create_grant_direct(conn):
    conn.create_grant.return_value = {"GrantId": "g1"}
    r = boto3_kms.create_grant(
        "key-id",
        "arn:aws:iam::1:role/r",
        operations=["Encrypt"],
        constraints={"EncryptionContextSubset": {"k": "v"}},
        grant_tokens=["t"],
        retiring_principal="arn:aws:iam::1:role/retire",
    )
    assert "grant" in r
    kwargs = conn.create_grant.call_args.kwargs
    assert kwargs["KeyId"] == "key-id"
    assert kwargs["Operations"] == ["Encrypt"]


def test_create_grant_alias_resolved(conn):
    conn.describe_key.return_value = {"KeyMetadata": {"KeyId": "resolved-id"}}
    conn.create_grant.return_value = {"GrantId": "g1"}
    r = boto3_kms.create_grant("alias/mykey", "principal")
    assert "grant" in r
    assert conn.create_grant.call_args.kwargs["KeyId"] == "resolved-id"


def test_create_grant_error(conn, client_error):
    conn.create_grant.side_effect = client_error("InternalFailure", "CreateGrant")
    assert "error" in boto3_kms.create_grant("key-id", "principal")


def test_list_grants_paginated(conn):
    conn.list_grants.side_effect = [
        {"Grants": [{"GrantId": "g1"}], "NextMarker": "m", "Truncated": True},
        {"Grants": [{"GrantId": "g2"}], "Truncated": False},
    ]
    r = boto3_kms.list_grants("key-id")
    assert [g["GrantId"] for g in r["grants"]] == ["g1", "g2"]


def test_list_grants_alias(conn):
    conn.describe_key.return_value = {"KeyMetadata": {"KeyId": "resolved"}}
    conn.list_grants.return_value = {"Grants": [], "Truncated": False}
    boto3_kms.list_grants("alias/mk")
    assert conn.list_grants.call_args.kwargs["KeyId"] == "resolved"


def test_list_grants_error(conn, client_error):
    conn.list_grants.side_effect = client_error("InternalFailure", "ListGrants")
    assert "error" in boto3_kms.list_grants("key-id")


def test_revoke_grant(conn):
    r = boto3_kms.revoke_grant("key-id", "grant-id")
    assert r == {"result": True}
    conn.revoke_grant.assert_called_once_with(KeyId="key-id", GrantId="grant-id")


def test_revoke_grant_alias(conn):
    conn.describe_key.return_value = {"KeyMetadata": {"KeyId": "resolved"}}
    boto3_kms.revoke_grant("alias/mk", "grant-id")
    assert conn.revoke_grant.call_args.kwargs["KeyId"] == "resolved"


def test_revoke_grant_error(conn, client_error):
    conn.revoke_grant.side_effect = client_error("InternalFailure", "RevokeGrant")
    r = boto3_kms.revoke_grant("key-id", "grant-id")
    assert r["result"] is False


def test_update_key_description(conn):
    r = boto3_kms.update_key_description("key-id", "desc")
    assert r == {"result": True}
    conn.update_key_description.assert_called_once_with(KeyId="key-id", Description="desc")


def test_update_key_description_error(conn, client_error):
    conn.update_key_description.side_effect = client_error(
        "InternalFailure", "UpdateKeyDescription"
    )
    r = boto3_kms.update_key_description("key-id", "desc")
    assert r["result"] is False
