"""
Unit tests for the ``boto3_kms`` state module.
"""

import pytest
from salt.exceptions import SaltInvocationError

from saltext.boto3.states import boto3_kms

pytestmark = [
    pytest.mark.skip_on_fips_enabled_platform,
]


@pytest.fixture
def configure_loader_modules():
    return {
        boto3_kms: {
            "__opts__": {"test": False},
            "__context__": {},
            "__salt__": {},
        }
    }


def test_virtual(mock_salt):
    with mock_salt(boto3_kms, {"boto3_kms.describe_key": True}):
        assert boto3_kms.__virtual__() == "boto3_kms"


def test_virtual_no_exec_module(mock_salt):
    with mock_salt(boto3_kms, {}):
        result = boto3_kms.__virtual__()
    assert result[0] is False
    assert "boto3_kms execution module is unavailable" in result[1]


def test_key_present_requires_policy():
    with pytest.raises(SaltInvocationError):
        boto3_kms.key_present(name="mykey", policy=None)


def test_key_present_grants_must_be_list():
    with pytest.raises(SaltInvocationError):
        boto3_kms.key_present(name="mykey", policy={"a": 1}, grants="not-a-list")


def test_key_present_manage_grants_must_be_bool():
    with pytest.raises(SaltInvocationError):
        boto3_kms.key_present(name="mykey", policy={"a": 1}, manage_grants="yes")


def test_key_present_key_rotation_must_be_bool():
    with pytest.raises(SaltInvocationError):
        boto3_kms.key_present(name="mykey", policy={"a": 1}, key_rotation="yes")


def test_key_present_enabled_must_be_bool():
    with pytest.raises(SaltInvocationError):
        boto3_kms.key_present(name="mykey", policy={"a": 1}, enabled="yes")


def test_key_present_test_mode_create(mock_salt):
    salt_map = {"boto3_kms.key_exists": {"result": False}}
    with mock_salt(boto3_kms, salt_map, test=True):
        ret = boto3_kms.key_present(name="mykey", policy={"a": 1})
    assert ret["result"] is None
    assert "set to be created" in ret["comment"]


def test_key_present_create_success(mock_salt):
    salt_map = {
        "boto3_kms.key_exists": {"result": False},
        "boto3_kms.create_key": {
            "key_metadata": {"KeyId": "k1", "Enabled": True, "Description": "d"}
        },
        "boto3_kms.create_alias": {"result": True},
        "boto3_kms.get_key_rotation_status": {"result": False},
    }
    with mock_salt(boto3_kms, salt_map):
        ret = boto3_kms.key_present(name="mykey", policy={"a": 1})
    assert ret["result"] is True
    assert ret["changes"]["new"] == {"key": "mykey"}


def test_key_present_create_key_error(mock_salt):
    salt_map = {
        "boto3_kms.key_exists": {"result": False},
        "boto3_kms.create_key": {"error": {"message": "bad"}},
    }
    with mock_salt(boto3_kms, salt_map):
        ret = boto3_kms.key_present(name="mykey", policy={"a": 1})
    assert ret["result"] is False
    assert "Failed to create key" in ret["comment"]


def test_key_present_create_alias_error(mock_salt):
    salt_map = {
        "boto3_kms.key_exists": {"result": False},
        "boto3_kms.create_key": {"key_metadata": {"KeyId": "k1"}},
        "boto3_kms.create_alias": {"error": {"message": "bad"}},
    }
    with mock_salt(boto3_kms, salt_map):
        ret = boto3_kms.key_present(name="mykey", policy={"a": 1})
    assert ret["result"] is False
    assert "dangling" in ret["comment"]


def test_key_present_key_exists_lookup_error(mock_salt):
    salt_map = {"boto3_kms.key_exists": {"error": {"message": "bad"}}}
    with mock_salt(boto3_kms, salt_map):
        ret = boto3_kms.key_present(name="mykey", policy={"a": 1})
    assert ret["result"] is False
    assert "find key" in ret["comment"]


def test_key_present_existing_describe_error(mock_salt):
    salt_map = {
        "boto3_kms.key_exists": {"result": True},
        "boto3_kms.describe_key": {"error": {"message": "bad"}},
    }
    with mock_salt(boto3_kms, salt_map):
        ret = boto3_kms.key_present(name="mykey", policy={"a": 1})
    assert ret["result"] is False


def test_key_present_existing_noop(mock_salt):
    salt_map = {
        "boto3_kms.key_exists": {"result": True},
        "boto3_kms.describe_key": {
            "key_metadata": {"KeyId": "k1", "Enabled": True, "Description": "d"}
        },
        "boto3_kms.get_key_policy": {"key_policy": {"a": 1}},
        "boto3_kms.get_key_rotation_status": {"result": False},
    }
    with mock_salt(boto3_kms, salt_map):
        ret = boto3_kms.key_present(name="mykey", policy={"a": 1}, description="d")
    assert ret["result"] is True
    assert not ret["changes"]


def test_key_present_updates_description(mock_salt):
    salt_map = {
        "boto3_kms.key_exists": {"result": True},
        "boto3_kms.describe_key": {
            "key_metadata": {"KeyId": "k1", "Enabled": True, "Description": "old"}
        },
        "boto3_kms.update_key_description": {"result": True},
        "boto3_kms.get_key_policy": {"key_policy": {"a": 1}},
        "boto3_kms.get_key_rotation_status": {"result": False},
    }
    with mock_salt(boto3_kms, salt_map) as salt_mocks:
        ret = boto3_kms.key_present(name="mykey", policy={"a": 1}, description="new")
    assert ret["result"] is True
    assert "Updated key description." in ret["comment"]
    salt_mocks["boto3_kms.update_key_description"].assert_called_once()


def test_key_present_updates_description_error(mock_salt):
    salt_map = {
        "boto3_kms.key_exists": {"result": True},
        "boto3_kms.describe_key": {
            "key_metadata": {"KeyId": "k1", "Enabled": True, "Description": "old"}
        },
        "boto3_kms.update_key_description": {"error": {"message": "bad"}},
    }
    with mock_salt(boto3_kms, salt_map):
        ret = boto3_kms.key_present(name="mykey", policy={"a": 1}, description="new")
    assert ret["result"] is False


def test_key_present_updates_policy(mock_salt):
    salt_map = {
        "boto3_kms.key_exists": {"result": True},
        "boto3_kms.describe_key": {
            "key_metadata": {"KeyId": "k1", "Enabled": True, "Description": "d"}
        },
        "boto3_kms.get_key_policy": {"key_policy": {"a": 0}},
        "boto3_kms.put_key_policy": {"result": True},
        "boto3_kms.get_key_rotation_status": {"result": False},
    }
    with mock_salt(boto3_kms, salt_map) as salt_mocks:
        ret = boto3_kms.key_present(name="mykey", policy={"a": 1}, description="d")
    assert ret["result"] is True
    salt_mocks["boto3_kms.put_key_policy"].assert_called_once()


def test_key_present_updates_policy_error(mock_salt):
    salt_map = {
        "boto3_kms.key_exists": {"result": True},
        "boto3_kms.describe_key": {
            "key_metadata": {"KeyId": "k1", "Enabled": True, "Description": "d"}
        },
        "boto3_kms.get_key_policy": {"key_policy": {"a": 0}},
        "boto3_kms.put_key_policy": {"error": {"message": "bad"}},
    }
    with mock_salt(boto3_kms, salt_map):
        ret = boto3_kms.key_present(name="mykey", policy={"a": 1}, description="d")
    assert ret["result"] is False


def test_key_present_disables_key(mock_salt):
    salt_map = {
        "boto3_kms.key_exists": {"result": True},
        "boto3_kms.describe_key": {
            "key_metadata": {"KeyId": "k1", "Enabled": True, "Description": "d"}
        },
        "boto3_kms.get_key_policy": {"key_policy": {"a": 1}},
        "boto3_kms.disable_key": {"result": True},
        "boto3_kms.get_key_rotation_status": {"result": False},
    }
    with mock_salt(boto3_kms, salt_map) as salt_mocks:
        ret = boto3_kms.key_present(name="mykey", policy={"a": 1}, description="d", enabled=False)
    assert ret["result"] is True
    salt_mocks["boto3_kms.disable_key"].assert_called_once()


def test_key_present_enables_key(mock_salt):
    salt_map = {
        "boto3_kms.key_exists": {"result": True},
        "boto3_kms.describe_key": {
            "key_metadata": {"KeyId": "k1", "Enabled": False, "Description": "d"}
        },
        "boto3_kms.get_key_policy": {"key_policy": {"a": 1}},
        "boto3_kms.enable_key": {"result": True},
        "boto3_kms.get_key_rotation_status": {"result": False},
    }
    with mock_salt(boto3_kms, salt_map) as salt_mocks:
        ret = boto3_kms.key_present(name="mykey", policy={"a": 1}, description="d", enabled=True)
    assert ret["result"] is True
    salt_mocks["boto3_kms.enable_key"].assert_called_once()


def test_key_present_enable_error(mock_salt):
    salt_map = {
        "boto3_kms.key_exists": {"result": True},
        "boto3_kms.describe_key": {
            "key_metadata": {"KeyId": "k1", "Enabled": False, "Description": "d"}
        },
        "boto3_kms.get_key_policy": {"key_policy": {"a": 1}},
        "boto3_kms.enable_key": {"error": {"message": "bad"}},
    }
    with mock_salt(boto3_kms, salt_map):
        ret = boto3_kms.key_present(name="mykey", policy={"a": 1}, description="d")
    assert ret["result"] is False


def test_key_present_enables_rotation(mock_salt):
    salt_map = {
        "boto3_kms.key_exists": {"result": True},
        "boto3_kms.describe_key": {
            "key_metadata": {"KeyId": "k1", "Enabled": True, "Description": "d"}
        },
        "boto3_kms.get_key_policy": {"key_policy": {"a": 1}},
        "boto3_kms.get_key_rotation_status": {"result": False},
        "boto3_kms.enable_key_rotation": {"result": True},
    }
    with mock_salt(boto3_kms, salt_map) as salt_mocks:
        ret = boto3_kms.key_present(
            name="mykey", policy={"a": 1}, description="d", key_rotation=True
        )
    assert ret["result"] is True
    salt_mocks["boto3_kms.enable_key_rotation"].assert_called_once()


def test_key_present_disables_rotation(mock_salt):
    salt_map = {
        "boto3_kms.key_exists": {"result": True},
        "boto3_kms.describe_key": {
            "key_metadata": {"KeyId": "k1", "Enabled": True, "Description": "d"}
        },
        "boto3_kms.get_key_policy": {"key_policy": {"a": 1}},
        "boto3_kms.get_key_rotation_status": {"result": True},
        "boto3_kms.disable_key_rotation": {"result": True},
        "boto3_kms.enable_key_rotation": {"result": True},
    }
    with mock_salt(boto3_kms, salt_map) as salt_mocks:
        ret = boto3_kms.key_present(
            name="mykey", policy={"a": 1}, description="d", key_rotation=False
        )
    assert ret["result"] is True
    salt_mocks["boto3_kms.disable_key_rotation"].assert_called_once()
    salt_mocks["boto3_kms.enable_key_rotation"].assert_not_called()


def test_key_present_rotation_on_disabled_key(mock_salt):
    salt_map = {
        "boto3_kms.key_exists": {"result": True},
        "boto3_kms.describe_key": {
            "key_metadata": {"KeyId": "k1", "Enabled": False, "Description": "d"}
        },
        "boto3_kms.get_key_policy": {"key_policy": {"a": 1}},
        "boto3_kms.get_key_rotation_status": {"result": False},
    }
    with mock_salt(boto3_kms, salt_map):
        ret = boto3_kms.key_present(
            name="mykey",
            policy={"a": 1},
            description="d",
            enabled=False,
            key_rotation=True,
        )
    assert ret["result"] is None


def test_key_present_rotation_error(mock_salt):
    salt_map = {
        "boto3_kms.key_exists": {"result": True},
        "boto3_kms.describe_key": {
            "key_metadata": {"KeyId": "k1", "Enabled": True, "Description": "d"}
        },
        "boto3_kms.get_key_policy": {"key_policy": {"a": 1}},
        "boto3_kms.get_key_rotation_status": {"result": False},
        "boto3_kms.enable_key_rotation": {"error": {"message": "bad"}},
    }
    with mock_salt(boto3_kms, salt_map):
        ret = boto3_kms.key_present(
            name="mykey", policy={"a": 1}, description="d", key_rotation=True
        )
    assert ret["result"] is False


def test_key_present_rotation_key_is_disabled_msg(mock_salt):
    salt_map = {
        "boto3_kms.key_exists": {"result": True},
        "boto3_kms.describe_key": {
            "key_metadata": {"KeyId": "k1", "Enabled": True, "Description": "d"}
        },
        "boto3_kms.get_key_policy": {"key_policy": {"a": 1}},
        "boto3_kms.get_key_rotation_status": {"result": False},
        "boto3_kms.enable_key_rotation": {"error": {"message": "key is disabled"}},
    }
    with mock_salt(boto3_kms, salt_map):
        ret = boto3_kms.key_present(
            name="mykey", policy={"a": 1}, description="d", key_rotation=True
        )
    assert ret["result"] is None


def test_key_present_test_mode_update_description(mock_salt):
    salt_map = {
        "boto3_kms.key_exists": {"result": True},
        "boto3_kms.describe_key": {
            "key_metadata": {"KeyId": "k1", "Enabled": True, "Description": "old"}
        },
        "boto3_kms.get_key_policy": {"key_policy": {"a": 1}},
        "boto3_kms.get_key_rotation_status": {"result": False},
    }
    with mock_salt(boto3_kms, salt_map, test=True):
        ret = boto3_kms.key_present(name="mykey", policy={"a": 1}, description="new")
    assert ret["result"] is None
