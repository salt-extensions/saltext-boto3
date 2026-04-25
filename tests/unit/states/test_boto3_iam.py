"""
Unit tests for the ``boto3_iam`` state module.
"""

import pytest

from saltext.boto3.states import boto3_iam as iam_state

try:
    import botocore  # pylint: disable=unused-import

    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

pytestmark = [
    pytest.mark.skip_on_fips_enabled_platform,
    pytest.mark.skipif(HAS_BOTO3 is False, reason="botocore is required for these tests."),
]


@pytest.fixture
def configure_loader_modules():
    return {iam_state: {"__opts__": {"test": False}, "__salt__": {}}}


def test_virtual(mock_salt):
    with mock_salt(iam_state, {"boto3_iam.get_user": True}):
        assert iam_state.__virtual__() is True


def test_virtual_no_exec_module(mock_salt):
    with mock_salt(iam_state, {}):
        result = iam_state.__virtual__()
    assert result[0] is False


def test_user_absent_not_exists(mock_salt):
    with mock_salt(iam_state, {"boto3_iam.get_user": False}):
        ret = iam_state.user_absent("u")
    assert ret["result"] is True
    assert "does not exist" in ret["comment"]


def test_user_absent_delete_keys_test_mode(mock_salt):
    salt_map = {
        "boto3_iam.get_user": {"UserName": "u"},
        "boto3_iam.get_all_access_keys": {"AccessKeyMetadata": [{"AccessKeyId": "AK"}]},
        "boto3_iam.get_all_mfa_devices": [],
        "boto3_iam.delete_login_profile": True,
        "boto3_iam.delete_user": True,
    }
    with mock_salt(iam_state, salt_map, test=True):
        ret = iam_state.user_absent("u", delete_mfa_devices=False, delete_profile=False)
    assert ret["result"] is None
    assert "Key AK is set to be deleted." in ret["comment"]


def test_keys_absent_user_missing(mock_salt):
    with mock_salt(iam_state, {"boto3_iam.get_user": False}):
        ret = iam_state.keys_absent(["AK"], "u")
    assert ret["result"] is False


def test_keys_absent_test_mode(mock_salt):
    salt_map = {
        "boto3_iam.get_user": {"UserName": "u"},
        "boto3_iam.get_all_access_keys": {"AccessKeyMetadata": [{"AccessKeyId": "AK"}]},
    }
    with mock_salt(iam_state, salt_map, test=True):
        ret = iam_state.keys_absent(["AK"], "u")
    assert ret["result"] is None


def test_group_absent_missing(mock_salt):
    with mock_salt(iam_state, {"boto3_iam.get_group": False}):
        ret = iam_state.group_absent("g")
    assert ret["result"] is True
    assert "does not exist" in ret["comment"]


def test_group_absent_test_mode(mock_salt):
    salt_map = {
        "boto3_iam.get_group": {"GroupName": "g"},
        "boto3_iam.get_group_members": [],
        "boto3_iam.list_attached_group_policies": [],
        "boto3_iam.get_all_group_policies": [],
    }
    with mock_salt(iam_state, salt_map, test=True):
        ret = iam_state.group_absent("g")
    assert ret["result"] is None


def test_group_absent_delete(mock_salt):
    salt_map = {
        "boto3_iam.get_group": {"GroupName": "g"},
        "boto3_iam.get_group_members": [],
        "boto3_iam.list_attached_group_policies": [],
        "boto3_iam.get_all_group_policies": [],
        "boto3_iam.delete_group": True,
    }
    with mock_salt(iam_state, salt_map):
        ret = iam_state.group_absent("g")
    assert ret["result"] is True
    assert ret["changes"] == {"deleted": "g"}


def test_account_policy_unchanged(mock_salt):
    salt_map = {"boto3_iam.get_account_policy": {"MinimumPasswordLength": 14}}
    with mock_salt(iam_state, salt_map):
        ret = iam_state.account_policy(name="pw", minimum_password_length=14)
    assert ret["result"] is True
    assert ret["comment"] == "Account policy is not changed."


def test_account_policy_disabled(mock_salt):
    with mock_salt(iam_state, {"boto3_iam.get_account_policy": None}):
        ret = iam_state.account_policy(name="pw")
    assert ret["result"] is False


def test_account_policy_change_test_mode(mock_salt):
    salt_map = {"boto3_iam.get_account_policy": {"MinimumPasswordLength": 8}}
    with mock_salt(iam_state, salt_map, test=True):
        ret = iam_state.account_policy(name="pw", minimum_password_length=14)
    assert ret["result"] is None
    assert ret["changes"]["minimum_password_length"] == "14"


def test_account_policy_change_apply(mock_salt):
    salt_map = {
        "boto3_iam.get_account_policy": {"MinimumPasswordLength": 8},
        "boto3_iam.update_account_password_policy": True,
    }
    with mock_salt(iam_state, salt_map):
        ret = iam_state.account_policy(name="pw", minimum_password_length=14)
    assert ret["result"] is True


def test_server_cert_absent_missing(mock_salt):
    with mock_salt(iam_state, {"boto3_iam.get_server_certificate": False}):
        ret = iam_state.server_cert_absent("c")
    assert ret["result"] is True
    assert "does not exist" in ret["comment"]


def test_server_cert_absent_test_mode(mock_salt):
    salt_map = {"boto3_iam.get_server_certificate": {"x": 1}}
    with mock_salt(iam_state, salt_map, test=True):
        ret = iam_state.server_cert_absent("c")
    assert ret["result"] is None


def test_server_cert_absent_delete(mock_salt):
    salt_map = {
        "boto3_iam.get_server_certificate": {"x": 1},
        "boto3_iam.delete_server_cert": {"ResponseMetadata": {}},
    }
    with mock_salt(iam_state, salt_map):
        ret = iam_state.server_cert_absent("c")
    assert ret["result"] is True


def test_server_cert_absent_fail(mock_salt):
    salt_map = {
        "boto3_iam.get_server_certificate": {"x": 1},
        "boto3_iam.delete_server_cert": False,
    }
    with mock_salt(iam_state, salt_map):
        ret = iam_state.server_cert_absent("c")
    assert ret["result"] is False


def test_policy_absent_missing(mock_salt):
    with mock_salt(iam_state, {"boto3_iam.policy_exists": False}):
        ret = iam_state.policy_absent("p")
    assert ret["result"] is True


def test_policy_absent_test_mode(mock_salt):
    with mock_salt(iam_state, {"boto3_iam.policy_exists": True}, test=True):
        ret = iam_state.policy_absent("p")
    assert ret["result"] is None


def test_policy_absent_delete(mock_salt):
    salt_map = {
        "boto3_iam.policy_exists": True,
        "boto3_iam.list_policy_versions": [
            {"VersionId": "v1", "IsDefaultVersion": True},
            {"VersionId": "v2", "IsDefaultVersion": False},
        ],
        "boto3_iam.delete_policy_version": True,
        "boto3_iam.delete_policy": True,
    }
    with mock_salt(iam_state, salt_map) as salt_mocks:
        ret = iam_state.policy_absent("p")
    assert ret["result"] is True
    assert ret["changes"] == {"old": {"policy": "p"}, "new": {"policy": None}}
    salt_mocks["boto3_iam.delete_policy_version"].assert_called_once()


def test_policy_absent_delete_version_fail(mock_salt):
    salt_map = {
        "boto3_iam.policy_exists": True,
        "boto3_iam.list_policy_versions": [{"VersionId": "v2", "IsDefaultVersion": False}],
        "boto3_iam.delete_policy_version": False,
    }
    with mock_salt(iam_state, salt_map):
        ret = iam_state.policy_absent("p")
    assert ret["result"] is False


def test_policy_present_new(mock_salt):
    salt_map = {
        "boto3_iam.get_policy": None,
        "boto3_iam.create_policy": True,
    }
    with mock_salt(iam_state, salt_map):
        ret = iam_state.policy_present("p", policy_document={"x": 1})
    assert ret["result"] is True


def test_policy_present_test_mode_new(mock_salt):
    with mock_salt(iam_state, {"boto3_iam.get_policy": None}, test=True):
        ret = iam_state.policy_present("p", policy_document={"x": 1})
    assert ret["result"] is None


def test_policy_present_unchanged(mock_salt):
    policy_doc = {"Version": "2012-10-17"}
    salt_map = {
        "boto3_iam.get_policy": {"DefaultVersionId": "v1"},
        "boto3_iam.get_policy_version": {"policy_version": {"Document": policy_doc}},
    }
    with mock_salt(iam_state, salt_map):
        ret = iam_state.policy_present("p", policy_document=policy_doc)
    assert ret["result"] is True
    assert not ret["changes"]


def test_saml_provider_present_existing(mock_salt):
    with mock_salt(iam_state, {"boto3_iam.list_saml_providers": ["sp"]}):
        ret = iam_state.saml_provider_present("sp", "<xml/>")
    assert ret["result"] is True
    assert "is present" in ret["comment"]


def test_saml_provider_present_test_mode(mock_salt):
    with mock_salt(iam_state, {"boto3_iam.list_saml_providers": []}, test=True):
        ret = iam_state.saml_provider_present("sp", "<xml/>")
    assert ret["result"] is None


def test_saml_provider_present_create(mock_salt):
    salt_map = {
        "boto3_iam.list_saml_providers": [],
        "boto3_iam.create_saml_provider": True,
    }
    with mock_salt(iam_state, salt_map):
        ret = iam_state.saml_provider_present("sp", "<xml/>")
    assert ret["result"] is True
    assert ret["changes"]["new"] == "sp"


def test_saml_provider_present_create_fail(mock_salt):
    salt_map = {
        "boto3_iam.list_saml_providers": [],
        "boto3_iam.create_saml_provider": False,
    }
    with mock_salt(iam_state, salt_map):
        ret = iam_state.saml_provider_present("sp", "<xml/>")
    assert ret["result"] is False


def test_saml_provider_absent_missing(mock_salt):
    with mock_salt(iam_state, {"boto3_iam.list_saml_providers": []}):
        ret = iam_state.saml_provider_absent("sp")
    assert ret["result"] is True
    assert "absent" in ret["comment"]


def test_saml_provider_absent_test_mode(mock_salt):
    with mock_salt(iam_state, {"boto3_iam.list_saml_providers": ["sp"]}, test=True):
        ret = iam_state.saml_provider_absent("sp")
    assert ret["result"] is None


def test_saml_provider_absent_delete(mock_salt):
    salt_map = {
        "boto3_iam.list_saml_providers": ["sp"],
        "boto3_iam.delete_saml_provider": True,
    }
    with mock_salt(iam_state, salt_map):
        ret = iam_state.saml_provider_absent("sp")
    assert ret["result"] is True
    assert ret["changes"]["old"] == "sp"


def test_saml_provider_absent_delete_fail(mock_salt):
    salt_map = {
        "boto3_iam.list_saml_providers": ["sp"],
        "boto3_iam.delete_saml_provider": False,
    }
    with mock_salt(iam_state, salt_map):
        ret = iam_state.saml_provider_absent("sp")
    assert ret["result"] is False
