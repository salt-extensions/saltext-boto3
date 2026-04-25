"""
Unit tests for the ``boto3_iot`` state module.
"""

from unittest.mock import patch

import pytest

from saltext.boto3.states import boto3_iot

pytestmark = [
    pytest.mark.skip_on_fips_enabled_platform,
]


@pytest.fixture
def configure_loader_modules():
    return {
        boto3_iot: {
            "__opts__": {"test": False},
            "__salt__": {},
            "__states__": {},
        }
    }


def test_virtual(mock_salt):
    with mock_salt(boto3_iot, {"boto3_iot.policy_exists": object()}):
        assert boto3_iot.__virtual__() == "boto3_iot"


def test_virtual_no_exec_module(mock_salt):
    with mock_salt(boto3_iot, {}):
        result = boto3_iot.__virtual__()
    assert result[0] is False


def test_thing_type_present_already_exists(mock_salt):
    salt_map = {"boto3_iot.thing_type_exists": {"exists": True}}
    with mock_salt(boto3_iot, salt_map):
        ret = boto3_iot.thing_type_present(
            "n", thingTypeName="tt", thingTypeDescription="d", searchableAttributesList=[]
        )
    assert ret["result"] is True
    assert "already exists" in ret["comment"]


def test_thing_type_present_test_mode(mock_salt):
    salt_map = {"boto3_iot.thing_type_exists": {"exists": False}}
    with mock_salt(boto3_iot, salt_map, test=True):
        ret = boto3_iot.thing_type_present(
            "n", thingTypeName="tt", thingTypeDescription="d", searchableAttributesList=[]
        )
    assert ret["result"] is None
    assert "set to be created" in ret["comment"]


def test_thing_type_present_creates(mock_salt):
    salt_map = {
        "boto3_iot.thing_type_exists": {"exists": False},
        "boto3_iot.create_thing_type": {"created": True, "thingTypeArn": "arn"},
        "boto3_iot.describe_thing_type": {"thing_type": {"thingTypeName": "tt"}},
    }
    with mock_salt(boto3_iot, salt_map):
        ret = boto3_iot.thing_type_present(
            "n", thingTypeName="tt", thingTypeDescription="d", searchableAttributesList=["a"]
        )
    assert ret["result"] is True
    assert "created" in ret["comment"]
    assert ret["changes"]["new"] == {"thing_type": {"thingTypeName": "tt"}}


def test_thing_type_present_exists_error(mock_salt):
    salt_map = {"boto3_iot.thing_type_exists": {"error": {"message": "boom"}}}
    with mock_salt(boto3_iot, salt_map):
        ret = boto3_iot.thing_type_present(
            "n", thingTypeName="tt", thingTypeDescription="d", searchableAttributesList=[]
        )
    assert ret["result"] is False


def test_thing_type_present_create_fails(mock_salt):
    salt_map = {
        "boto3_iot.thing_type_exists": {"exists": False},
        "boto3_iot.create_thing_type": {"created": False, "error": {"message": "boom"}},
    }
    with mock_salt(boto3_iot, salt_map):
        ret = boto3_iot.thing_type_present(
            "n", thingTypeName="tt", thingTypeDescription="d", searchableAttributesList=[]
        )
    assert ret["result"] is False


def test_thing_type_absent_does_not_exist(mock_salt):
    salt_map = {"boto3_iot.describe_thing_type": {"thing_type": None}}
    with mock_salt(boto3_iot, salt_map):
        ret = boto3_iot.thing_type_absent("n", thingTypeName="tt")
    assert ret["result"] is True
    assert "does not exist" in ret["comment"]


def test_thing_type_absent_describe_error(mock_salt):
    salt_map = {"boto3_iot.describe_thing_type": {"error": {"message": "boom"}}}
    with mock_salt(boto3_iot, salt_map):
        ret = boto3_iot.thing_type_absent("n", thingTypeName="tt")
    assert ret["result"] is False


def test_thing_type_absent_test_mode_not_deprecated(mock_salt):
    salt_map = {
        "boto3_iot.describe_thing_type": {
            "thing_type": {"thingTypeMetadata": {"deprecated": False}}
        }
    }
    with mock_salt(boto3_iot, salt_map, test=True):
        ret = boto3_iot.thing_type_absent("n", thingTypeName="tt")
    assert ret["result"] is None
    assert "deprecated and removed" in ret["comment"]


def test_thing_type_absent_deletes_already_deprecated_past_5m(mock_salt):
    salt_map = {
        "boto3_iot.describe_thing_type": {
            "thing_type": {
                "thingTypeMetadata": {
                    "deprecated": True,
                    "deprecationDate": "2001-01-01 00:00:00.000000",
                }
            }
        },
        "boto3_iot.delete_thing_type": {"deleted": True},
    }
    with patch.object(boto3_iot.time, "sleep") as sleep, mock_salt(boto3_iot, salt_map):
        ret = boto3_iot.thing_type_absent("n", thingTypeName="tt")
    assert ret["result"] is True
    assert "deleted" in ret["comment"]
    sleep.assert_not_called()


def test_thing_type_absent_deprecates_then_deletes(mock_salt):
    salt_map = {
        "boto3_iot.describe_thing_type": {
            "thing_type": {"thingTypeMetadata": {"deprecated": False}}
        },
        "boto3_iot.deprecate_thing_type": {"deprecated": True},
        "boto3_iot.delete_thing_type": {"deleted": True},
    }
    with patch.object(boto3_iot.time, "sleep"), mock_salt(boto3_iot, salt_map) as salt_mocks:
        ret = boto3_iot.thing_type_absent("n", thingTypeName="tt")
    assert ret["result"] is True
    salt_mocks["boto3_iot.deprecate_thing_type"].assert_called_once()


def test_thing_type_absent_deprecate_fails(mock_salt):
    salt_map = {
        "boto3_iot.describe_thing_type": {
            "thing_type": {"thingTypeMetadata": {"deprecated": False}}
        },
        "boto3_iot.deprecate_thing_type": {
            "deprecated": False,
            "error": {"message": "boom"},
        },
    }
    with mock_salt(boto3_iot, salt_map):
        ret = boto3_iot.thing_type_absent("n", thingTypeName="tt")
    assert ret["result"] is False


def test_thing_type_absent_delete_fails(mock_salt):
    salt_map = {
        "boto3_iot.describe_thing_type": {
            "thing_type": {
                "thingTypeMetadata": {
                    "deprecated": True,
                    "deprecationDate": "2001-01-01 00:00:00.000000",
                }
            }
        },
        "boto3_iot.delete_thing_type": {"deleted": False, "error": {"message": "boom"}},
    }
    with patch.object(boto3_iot.time, "sleep"), mock_salt(boto3_iot, salt_map):
        ret = boto3_iot.thing_type_absent("n", thingTypeName="tt")
    assert ret["result"] is False


def test_policy_present_exists_error(mock_salt):
    salt_map = {"boto3_iot.policy_exists": {"error": {"message": "boom"}}}
    with mock_salt(boto3_iot, salt_map):
        ret = boto3_iot.policy_present("n", policyName="p", policyDocument={})
    assert ret["result"] is False


def test_policy_present_test_mode_create(mock_salt):
    salt_map = {"boto3_iot.policy_exists": {"exists": False}}
    with mock_salt(boto3_iot, salt_map, test=True):
        ret = boto3_iot.policy_present("n", policyName="p", policyDocument={"a": 1})
    assert ret["result"] is None


def test_policy_present_creates(mock_salt):
    salt_map = {
        "boto3_iot.policy_exists": {"exists": False},
        "boto3_iot.create_policy": {"created": True, "versionId": "1"},
        "boto3_iot.describe_policy": {"policy": {"policyName": "p"}},
    }
    with mock_salt(boto3_iot, salt_map):
        ret = boto3_iot.policy_present("n", policyName="p", policyDocument={"a": 1})
    assert ret["result"] is True
    assert ret["changes"]["new"] == {"policy": {"policyName": "p"}}


def test_policy_present_create_fails(mock_salt):
    salt_map = {
        "boto3_iot.policy_exists": {"exists": False},
        "boto3_iot.create_policy": {"created": False, "error": {"message": "boom"}},
    }
    with mock_salt(boto3_iot, salt_map):
        ret = boto3_iot.policy_present("n", policyName="p", policyDocument={"a": 1})
    assert ret["result"] is False


def test_policy_present_matches_no_update(mock_salt):
    salt_map = {
        "boto3_iot.policy_exists": {"exists": True},
        "boto3_iot.describe_policy": {
            "policy": {"policyDocument": {"a": 1}, "defaultVersionId": "1"}
        },
    }
    with mock_salt(boto3_iot, salt_map):
        ret = boto3_iot.policy_present("n", policyName="p", policyDocument={"a": 1})
    assert ret["result"] is True
    assert not ret["changes"]


def test_policy_present_updates(mock_salt):
    salt_map = {
        "boto3_iot.policy_exists": {"exists": True},
        "boto3_iot.describe_policy": {
            "policy": {"policyDocument": {"a": 1}, "defaultVersionId": "1"}
        },
        "boto3_iot.create_policy_version": {"created": True, "name": "2"},
        "boto3_iot.delete_policy_version": {"deleted": True},
    }
    with mock_salt(boto3_iot, salt_map) as salt_mocks:
        ret = boto3_iot.policy_present("n", policyName="p", policyDocument={"a": 2})
    assert ret["result"] is True
    assert "new" in ret["changes"]
    salt_mocks["boto3_iot.delete_policy_version"].assert_called_once()


def test_policy_present_updates_test_mode(mock_salt):
    salt_map = {
        "boto3_iot.policy_exists": {"exists": True},
        "boto3_iot.describe_policy": {
            "policy": {"policyDocument": {"a": 1}, "defaultVersionId": "1"}
        },
    }
    with mock_salt(boto3_iot, salt_map, test=True):
        ret = boto3_iot.policy_present("n", policyName="p", policyDocument={"a": 2})
    assert ret["result"] is None


def test_policy_present_update_fails(mock_salt):
    salt_map = {
        "boto3_iot.policy_exists": {"exists": True},
        "boto3_iot.describe_policy": {
            "policy": {"policyDocument": {"a": 1}, "defaultVersionId": "1"}
        },
        "boto3_iot.create_policy_version": {
            "created": False,
            "error": {"message": "boom"},
        },
    }
    with mock_salt(boto3_iot, salt_map):
        ret = boto3_iot.policy_present("n", policyName="p", policyDocument={"a": 2})
    assert ret["result"] is False


def test_policy_absent_not_exists(mock_salt):
    salt_map = {"boto3_iot.policy_exists": {"exists": False}}
    with mock_salt(boto3_iot, salt_map):
        ret = boto3_iot.policy_absent("n", policyName="p")
    assert ret["result"] is True
    assert "does not exist" in ret["comment"]


def test_policy_absent_exists_error(mock_salt):
    salt_map = {"boto3_iot.policy_exists": {"error": {"message": "boom"}}}
    with mock_salt(boto3_iot, salt_map):
        ret = boto3_iot.policy_absent("n", policyName="p")
    assert ret["result"] is False


def test_policy_absent_test_mode(mock_salt):
    salt_map = {"boto3_iot.policy_exists": {"exists": True}}
    with mock_salt(boto3_iot, salt_map, test=True):
        ret = boto3_iot.policy_absent("n", policyName="p")
    assert ret["result"] is None


def test_policy_absent_deletes(mock_salt):
    salt_map = {
        "boto3_iot.policy_exists": {"exists": True},
        "boto3_iot.list_policy_versions": {
            "policyVersions": [
                {"versionId": "1", "isDefaultVersion": True},
                {"versionId": "2", "isDefaultVersion": False},
            ]
        },
        "boto3_iot.delete_policy_version": {"deleted": True},
        "boto3_iot.delete_policy": {"deleted": True},
    }
    with mock_salt(boto3_iot, salt_map) as salt_mocks:
        ret = boto3_iot.policy_absent("n", policyName="p")
    assert ret["result"] is True
    salt_mocks["boto3_iot.delete_policy_version"].assert_called_once()
    salt_mocks["boto3_iot.delete_policy"].assert_called_once()


def test_policy_absent_version_delete_fails(mock_salt):
    salt_map = {
        "boto3_iot.policy_exists": {"exists": True},
        "boto3_iot.list_policy_versions": {
            "policyVersions": [{"versionId": "2", "isDefaultVersion": False}]
        },
        "boto3_iot.delete_policy_version": {
            "deleted": False,
            "error": {"message": "boom"},
        },
    }
    with mock_salt(boto3_iot, salt_map):
        ret = boto3_iot.policy_absent("n", policyName="p")
    assert ret["result"] is False


def test_policy_absent_delete_fails(mock_salt):
    salt_map = {
        "boto3_iot.policy_exists": {"exists": True},
        "boto3_iot.list_policy_versions": {"policyVersions": []},
        "boto3_iot.delete_policy": {"deleted": False, "error": {"message": "boom"}},
    }
    with mock_salt(boto3_iot, salt_map):
        ret = boto3_iot.policy_absent("n", policyName="p")
    assert ret["result"] is False


def test_policy_attached_already(mock_salt):
    salt_map = {"boto3_iot.list_principal_policies": {"policies": [{"policyName": "p"}]}}
    with mock_salt(boto3_iot, salt_map):
        ret = boto3_iot.policy_attached("n", policyName="p", principal="pr")
    assert ret["result"] is True
    assert not ret["changes"]


def test_policy_attached_attaches(mock_salt):
    salt_map = {
        "boto3_iot.list_principal_policies": {"policies": []},
        "boto3_iot.attach_principal_policy": {"attached": True},
    }
    with mock_salt(boto3_iot, salt_map):
        ret = boto3_iot.policy_attached("n", policyName="p", principal="pr")
    assert ret["result"] is True
    assert ret["changes"]["new"] == {"attached": True}


def test_policy_attached_test_mode(mock_salt):
    salt_map = {"boto3_iot.list_principal_policies": {"policies": []}}
    with mock_salt(boto3_iot, salt_map, test=True):
        ret = boto3_iot.policy_attached("n", policyName="p", principal="pr")
    assert ret["result"] is None


def test_policy_attached_list_error(mock_salt):
    salt_map = {"boto3_iot.list_principal_policies": {"error": {"message": "boom"}}}
    with mock_salt(boto3_iot, salt_map):
        ret = boto3_iot.policy_attached("n", policyName="p", principal="pr")
    assert ret["result"] is False


def test_policy_attached_attach_fails(mock_salt):
    salt_map = {
        "boto3_iot.list_principal_policies": {"policies": []},
        "boto3_iot.attach_principal_policy": {
            "attached": False,
            "error": {"message": "boom"},
        },
    }
    with mock_salt(boto3_iot, salt_map):
        ret = boto3_iot.policy_attached("n", policyName="p", principal="pr")
    assert ret["result"] is False


def test_policy_detached_not_attached(mock_salt):
    salt_map = {"boto3_iot.list_principal_policies": {"policies": []}}
    with mock_salt(boto3_iot, salt_map):
        ret = boto3_iot.policy_detached("n", policyName="p", principal="pr")
    assert ret["result"] is True
    assert not ret["changes"]


def test_policy_detached_detaches(mock_salt):
    salt_map = {
        "boto3_iot.list_principal_policies": {"policies": [{"policyName": "p"}]},
        "boto3_iot.detach_principal_policy": {"detached": True},
    }
    with mock_salt(boto3_iot, salt_map):
        ret = boto3_iot.policy_detached("n", policyName="p", principal="pr")
    assert ret["result"] is True
    assert ret["changes"]["new"] == {"attached": False}


def test_policy_detached_list_error(mock_salt):
    salt_map = {"boto3_iot.list_principal_policies": {"error": {"message": "boom"}}}
    with mock_salt(boto3_iot, salt_map):
        ret = boto3_iot.policy_detached("n", policyName="p", principal="pr")
    assert ret["result"] is False


def test_policy_detached_detach_fails(mock_salt):
    salt_map = {
        "boto3_iot.list_principal_policies": {"policies": [{"policyName": "p"}]},
        "boto3_iot.detach_principal_policy": {
            "detached": False,
            "error": {"message": "boom"},
        },
    }
    with mock_salt(boto3_iot, salt_map):
        ret = boto3_iot.policy_detached("n", policyName="p", principal="pr")
    assert ret["result"] is False


def test_topic_rule_present_exists_error(mock_salt):
    salt_map = {"boto3_iot.topic_rule_exists": {"error": {"message": "boom"}}}
    with mock_salt(boto3_iot, salt_map):
        ret = boto3_iot.topic_rule_present("n", ruleName="r", sql="s", actions=[])
    assert ret["result"] is False


def test_topic_rule_present_test_mode_create(mock_salt):
    salt_map = {"boto3_iot.topic_rule_exists": {"exists": False}}
    with mock_salt(boto3_iot, salt_map, test=True):
        ret = boto3_iot.topic_rule_present("n", ruleName="r", sql="s", actions=[])
    assert ret["result"] is None


def test_topic_rule_present_creates(mock_salt):
    salt_map = {
        "boto3_iot.topic_rule_exists": {"exists": False},
        "boto3_iot.create_topic_rule": {"created": True},
        "boto3_iot.describe_topic_rule": {"rule": {"ruleName": "r"}},
    }
    with mock_salt(boto3_iot, salt_map):
        ret = boto3_iot.topic_rule_present("n", ruleName="r", sql="s", actions=[])
    assert ret["result"] is True
    assert ret["changes"]["new"] == {"rule": {"ruleName": "r"}}


def test_topic_rule_present_create_fails(mock_salt):
    salt_map = {
        "boto3_iot.topic_rule_exists": {"exists": False},
        "boto3_iot.create_topic_rule": {"created": False, "error": {"message": "boom"}},
    }
    with mock_salt(boto3_iot, salt_map):
        ret = boto3_iot.topic_rule_present("n", ruleName="r", sql="s", actions=[])
    assert ret["result"] is False


def test_topic_rule_present_matches_no_update(mock_salt):
    salt_map = {
        "boto3_iot.topic_rule_exists": {"exists": True},
        "boto3_iot.describe_topic_rule": {
            "rule": {
                "ruleName": "r",
                "sql": "s",
                "description": "",
                "actions": [],
                "ruleDisabled": False,
            }
        },
    }
    with mock_salt(boto3_iot, salt_map):
        ret = boto3_iot.topic_rule_present("n", ruleName="r", sql="s", actions=[])
    assert ret["result"] is True
    assert not ret["changes"]


def test_topic_rule_present_updates(mock_salt):
    salt_map = {
        "boto3_iot.topic_rule_exists": {"exists": True},
        "boto3_iot.describe_topic_rule": {
            "rule": {
                "ruleName": "r",
                "sql": "old",
                "description": "",
                "actions": [],
                "ruleDisabled": False,
            }
        },
        "boto3_iot.replace_topic_rule": {"replaced": True},
    }
    with mock_salt(boto3_iot, salt_map):
        ret = boto3_iot.topic_rule_present("n", ruleName="r", sql="new", actions=[])
    assert ret["result"] is True
    assert ret["changes"]["new"]["sql"] == "new"


def test_topic_rule_present_updates_test_mode(mock_salt):
    salt_map = {
        "boto3_iot.topic_rule_exists": {"exists": True},
        "boto3_iot.describe_topic_rule": {
            "rule": {
                "ruleName": "r",
                "sql": "old",
                "description": "",
                "actions": [],
                "ruleDisabled": False,
            }
        },
    }
    with mock_salt(boto3_iot, salt_map, test=True):
        ret = boto3_iot.topic_rule_present("n", ruleName="r", sql="new", actions=[])
    assert ret["result"] is None


def test_topic_rule_present_replace_fails(mock_salt):
    salt_map = {
        "boto3_iot.topic_rule_exists": {"exists": True},
        "boto3_iot.describe_topic_rule": {
            "rule": {
                "ruleName": "r",
                "sql": "old",
                "description": "",
                "actions": [],
                "ruleDisabled": False,
            }
        },
        "boto3_iot.replace_topic_rule": {"replaced": False, "error": {"message": "boom"}},
    }
    with mock_salt(boto3_iot, salt_map):
        ret = boto3_iot.topic_rule_present("n", ruleName="r", sql="new", actions=[])
    assert ret["result"] is False


def test_topic_rule_absent_not_exists(mock_salt):
    salt_map = {"boto3_iot.topic_rule_exists": {"exists": False}}
    with mock_salt(boto3_iot, salt_map):
        ret = boto3_iot.topic_rule_absent("n", ruleName="r")
    assert ret["result"] is True
    assert "does not exist" in ret["comment"]


def test_topic_rule_absent_exists_error(mock_salt):
    salt_map = {"boto3_iot.topic_rule_exists": {"error": {"message": "boom"}}}
    with mock_salt(boto3_iot, salt_map):
        ret = boto3_iot.topic_rule_absent("n", ruleName="r")
    assert ret["result"] is False


def test_topic_rule_absent_test_mode(mock_salt):
    salt_map = {"boto3_iot.topic_rule_exists": {"exists": True}}
    with mock_salt(boto3_iot, salt_map, test=True):
        ret = boto3_iot.topic_rule_absent("n", ruleName="r")
    assert ret["result"] is None


def test_topic_rule_absent_deletes(mock_salt):
    salt_map = {
        "boto3_iot.topic_rule_exists": {"exists": True},
        "boto3_iot.delete_topic_rule": {"deleted": True},
    }
    with mock_salt(boto3_iot, salt_map):
        ret = boto3_iot.topic_rule_absent("n", ruleName="r")
    assert ret["result"] is True


def test_topic_rule_absent_delete_fails(mock_salt):
    salt_map = {
        "boto3_iot.topic_rule_exists": {"exists": True},
        "boto3_iot.delete_topic_rule": {"deleted": False, "error": {"message": "boom"}},
    }
    with mock_salt(boto3_iot, salt_map):
        ret = boto3_iot.topic_rule_absent("n", ruleName="r")
    assert ret["result"] is False
