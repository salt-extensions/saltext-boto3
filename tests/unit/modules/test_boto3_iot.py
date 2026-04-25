"""
Unit tests for the ``boto3_iot`` execution module.
"""

import pytest

from saltext.boto3.modules import boto3_iot

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
        boto3_iot: {
            "__opts__": {},
            "__context__": {},
            "__salt__": {},
        }
    }


@pytest.fixture
def conn(make_conn):
    with make_conn(boto3_iot) as client:
        yield client


def test_thing_type_exists_true(conn):
    conn.describe_thing_type.return_value = {"thingTypeName": "t"}
    assert boto3_iot.thing_type_exists("t") == {"exists": True}


def test_thing_type_exists_empty_response(conn):
    conn.describe_thing_type.return_value = {}
    assert boto3_iot.thing_type_exists("t") == {"exists": False}


def test_thing_type_exists_not_found(conn, client_error):
    conn.describe_thing_type.side_effect = client_error(
        "ResourceNotFoundException", "DescribeThingType"
    )
    assert boto3_iot.thing_type_exists("t") == {"exists": False}


def test_thing_type_exists_other_error(conn, client_error):
    conn.describe_thing_type.side_effect = client_error("AccessDenied", "DescribeThingType")
    assert "error" in boto3_iot.thing_type_exists("t")


def test_describe_thing_type(conn):
    conn.describe_thing_type.return_value = {
        "thingTypeName": "t",
        "ResponseMetadata": {},
        "thingTypeMetadata": {},
    }
    r = boto3_iot.describe_thing_type("t")
    assert r["thing_type"]["thingTypeName"] == "t"
    assert "ResponseMetadata" not in r["thing_type"]


def test_describe_thing_type_not_found(conn, client_error):
    conn.describe_thing_type.side_effect = client_error(
        "ResourceNotFoundException", "DescribeThingType"
    )
    assert boto3_iot.describe_thing_type("t") == {"thing_type": None}


def test_describe_thing_type_error(conn, client_error):
    conn.describe_thing_type.side_effect = client_error("AccessDenied", "DescribeThingType")
    assert "error" in boto3_iot.describe_thing_type("t")


def test_create_thing_type(conn):
    conn.create_thing_type.return_value = {"thingTypeArn": "arn"}
    assert boto3_iot.create_thing_type("t", "desc", ["a"]) == {
        "created": True,
        "thingTypeArn": "arn",
    }


def test_create_thing_type_empty(conn):
    conn.create_thing_type.return_value = {}
    assert boto3_iot.create_thing_type("t", "desc", ["a"]) == {"created": False}


def test_create_thing_type_error(conn, client_error):
    conn.create_thing_type.side_effect = client_error("Boom", "CreateThingType")
    r = boto3_iot.create_thing_type("t", "d", [])
    assert r["created"] is False
    assert "error" in r


def test_deprecate_thing_type_default(conn):
    assert boto3_iot.deprecate_thing_type("t") == {"deprecated": True}


def test_deprecate_thing_type_undo(conn):
    assert boto3_iot.deprecate_thing_type("t", undoDeprecate=True) == {"deprecated": False}


def test_deprecate_thing_type_error(conn, client_error):
    conn.deprecate_thing_type.side_effect = client_error("Boom", "DeprecateThingType")
    r = boto3_iot.deprecate_thing_type("t")
    assert r["deprecated"] is False
    assert "error" in r


def test_delete_thing_type(conn):
    assert boto3_iot.delete_thing_type("t") == {"deleted": True}


def test_delete_thing_type_not_found_is_success(conn, client_error):
    conn.delete_thing_type.side_effect = client_error(
        "ResourceNotFoundException", "DeleteThingType"
    )
    assert boto3_iot.delete_thing_type("t") == {"deleted": True}


def test_delete_thing_type_error(conn, client_error):
    conn.delete_thing_type.side_effect = client_error("AccessDenied", "DeleteThingType")
    r = boto3_iot.delete_thing_type("t")
    assert r["deleted"] is False
    assert "error" in r


def test_policy_exists_true(conn):
    assert boto3_iot.policy_exists("p") == {"exists": True}


def test_policy_exists_false(conn, client_error):
    conn.get_policy.side_effect = client_error("ResourceNotFoundException", "GetPolicy")
    assert boto3_iot.policy_exists("p") == {"exists": False}


def test_policy_exists_error(conn, client_error):
    conn.get_policy.side_effect = client_error("AccessDenied", "GetPolicy")
    assert "error" in boto3_iot.policy_exists("p")


def test_create_policy_success(conn):
    conn.create_policy.return_value = {"policyVersionId": "1"}
    assert boto3_iot.create_policy("p", {"a": 1}) == {"created": True, "versionId": "1"}
    assert isinstance(conn.create_policy.call_args.kwargs["policyDocument"], str)


def test_create_policy_empty(conn):
    conn.create_policy.return_value = {}
    assert boto3_iot.create_policy("p", "{}") == {"created": False}


def test_create_policy_error(conn, client_error):
    conn.create_policy.side_effect = client_error("Boom", "CreatePolicy")
    r = boto3_iot.create_policy("p", "{}")
    assert r["created"] is False
    assert "error" in r


def test_delete_policy(conn):
    assert boto3_iot.delete_policy("p") == {"deleted": True}


def test_delete_policy_error(conn, client_error):
    conn.delete_policy.side_effect = client_error("Boom", "DeletePolicy")
    r = boto3_iot.delete_policy("p")
    assert r["deleted"] is False


def test_describe_policy(conn):
    conn.get_policy.return_value = {
        "policyName": "p",
        "policyArn": "arn",
        "policyDocument": "{}",
        "defaultVersionId": "1",
    }
    r = boto3_iot.describe_policy("p")
    assert r["policy"]["policyName"] == "p"


def test_describe_policy_not_found(conn, client_error):
    conn.get_policy.side_effect = client_error("ResourceNotFoundException", "GetPolicy")
    assert boto3_iot.describe_policy("p") == {"policy": None}


def test_describe_policy_error(conn, client_error):
    conn.get_policy.side_effect = client_error("AccessDenied", "GetPolicy")
    assert "error" in boto3_iot.describe_policy("p")


def test_policy_version_exists_true(conn):
    conn.get_policy_version.return_value = {"policyVersionId": "1"}
    assert boto3_iot.policy_version_exists("p", "1") == {"exists": True}


def test_policy_version_exists_not_found(conn, client_error):
    conn.get_policy_version.side_effect = client_error(
        "ResourceNotFoundException", "GetPolicyVersion"
    )
    assert boto3_iot.policy_version_exists("p", "1") == {"exists": False}


def test_policy_version_exists_error(conn, client_error):
    conn.get_policy_version.side_effect = client_error("AccessDenied", "GetPolicyVersion")
    assert "error" in boto3_iot.policy_version_exists("p", "1")


def test_create_policy_version(conn):
    conn.create_policy_version.return_value = {"policyVersionId": "2"}
    assert boto3_iot.create_policy_version("p", {"a": 1}) == {"created": True, "name": "2"}


def test_create_policy_version_empty(conn):
    conn.create_policy_version.return_value = {}
    assert boto3_iot.create_policy_version("p", "{}") == {"created": False}


def test_create_policy_version_error(conn, client_error):
    conn.create_policy_version.side_effect = client_error("Boom", "CreatePolicyVersion")
    r = boto3_iot.create_policy_version("p", "{}")
    assert r["created"] is False


def test_delete_policy_version(conn):
    assert boto3_iot.delete_policy_version("p", "1") == {"deleted": True}


def test_delete_policy_version_error(conn, client_error):
    conn.delete_policy_version.side_effect = client_error("Boom", "DeletePolicyVersion")
    r = boto3_iot.delete_policy_version("p", "1")
    assert r["deleted"] is False


def test_describe_policy_version(conn):
    conn.get_policy_version.return_value = {
        "policyName": "p",
        "policyArn": "arn",
        "policyDocument": "{}",
        "policyVersionId": "1",
        "isDefaultVersion": True,
    }
    assert boto3_iot.describe_policy_version("p", "1")["policy"]["policyVersionId"] == "1"


def test_describe_policy_version_not_found(conn, client_error):
    conn.get_policy_version.side_effect = client_error(
        "ResourceNotFoundException", "GetPolicyVersion"
    )
    assert boto3_iot.describe_policy_version("p", "1") == {"policy": None}


def test_describe_policy_version_error(conn, client_error):
    conn.get_policy_version.side_effect = client_error("AccessDenied", "GetPolicyVersion")
    assert "error" in boto3_iot.describe_policy_version("p", "1")


def test_list_policies(conn):
    conn.list_policies.return_value = {"policies": [{"policyName": "a"}]}
    r = boto3_iot.list_policies()
    assert r == {"policies": [{"policyName": "a"}]}


def test_list_policies_error(conn, client_error):
    conn.list_policies.side_effect = client_error("Boom", "ListPolicies")
    assert "error" in boto3_iot.list_policies()


def test_list_policy_versions(conn):
    conn.list_policy_versions.return_value = {"policyVersions": [{"versionId": "1"}]}
    r = boto3_iot.list_policy_versions("p")
    assert r == {"policyVersions": [{"versionId": "1"}]}


def test_list_policy_versions_error(conn, client_error):
    conn.list_policy_versions.side_effect = client_error("Boom", "ListPolicyVersions")
    assert "error" in boto3_iot.list_policy_versions("p")


def test_set_default_policy_version(conn):
    assert boto3_iot.set_default_policy_version("p", 1) == {"changed": True}
    conn.set_default_policy_version.assert_called_once_with(policyName="p", policyVersionId="1")


def test_set_default_policy_version_error(conn, client_error):
    conn.set_default_policy_version.side_effect = client_error("Boom", "SetDefaultPolicyVersion")
    r = boto3_iot.set_default_policy_version("p", 1)
    assert r["changed"] is False


def test_list_principal_policies(conn):
    conn.list_principal_policies.return_value = {"policies": [{"policyName": "a"}]}
    r = boto3_iot.list_principal_policies("princ")
    assert r == {"policies": [{"policyName": "a"}]}


def test_list_principal_policies_error(conn, client_error):
    conn.list_principal_policies.side_effect = client_error("Boom", "ListPrincipalPolicies")
    assert "error" in boto3_iot.list_principal_policies("p")


def test_attach_principal_policy(conn):
    assert boto3_iot.attach_principal_policy("p", "princ") == {"attached": True}


def test_attach_principal_policy_error(conn, client_error):
    conn.attach_principal_policy.side_effect = client_error("Boom", "AttachPrincipalPolicy")
    r = boto3_iot.attach_principal_policy("p", "princ")
    assert r["attached"] is False


def test_detach_principal_policy(conn):
    assert boto3_iot.detach_principal_policy("p", "princ") == {"detached": True}


def test_detach_principal_policy_error(conn, client_error):
    conn.detach_principal_policy.side_effect = client_error("Boom", "DetachPrincipalPolicy")
    r = boto3_iot.detach_principal_policy("p", "princ")
    assert r["detached"] is False


def test_topic_rule_exists_true(conn):
    assert boto3_iot.topic_rule_exists("r") == {"exists": True}


def test_topic_rule_exists_unauthorized_is_false(conn, client_error):
    conn.get_topic_rule.side_effect = client_error("UnauthorizedException", "GetTopicRule")
    assert boto3_iot.topic_rule_exists("r") == {"exists": False}


def test_topic_rule_exists_error(conn, client_error):
    conn.get_topic_rule.side_effect = client_error("AccessDenied", "GetTopicRule")
    assert "error" in boto3_iot.topic_rule_exists("r")


def test_create_topic_rule(conn):
    assert boto3_iot.create_topic_rule("r", "SELECT *", [], "d") == {"created": True}
    assert conn.create_topic_rule.call_args.kwargs["topicRulePayload"] == {
        "sql": "SELECT *",
        "description": "d",
        "actions": [],
        "ruleDisabled": False,
    }


def test_create_topic_rule_error(conn, client_error):
    conn.create_topic_rule.side_effect = client_error("Boom", "CreateTopicRule")
    r = boto3_iot.create_topic_rule("r", "", [], "")
    assert r["created"] is False


def test_replace_topic_rule(conn):
    assert boto3_iot.replace_topic_rule("r", "s", [], "d") == {"replaced": True}


def test_replace_topic_rule_error(conn, client_error):
    conn.replace_topic_rule.side_effect = client_error("Boom", "ReplaceTopicRule")
    r = boto3_iot.replace_topic_rule("r", "s", [], "d")
    assert r["replaced"] is False


def test_delete_topic_rule(conn):
    assert boto3_iot.delete_topic_rule("r") == {"deleted": True}


def test_delete_topic_rule_error(conn, client_error):
    conn.delete_topic_rule.side_effect = client_error("Boom", "DeleteTopicRule")
    r = boto3_iot.delete_topic_rule("r")
    assert r["deleted"] is False


def test_describe_topic_rule(conn):
    conn.get_topic_rule.return_value = {
        "rule": {
            "ruleName": "r",
            "sql": "s",
            "description": "d",
            "actions": [],
            "ruleDisabled": False,
        }
    }
    assert boto3_iot.describe_topic_rule("r")["rule"]["ruleName"] == "r"


def test_describe_topic_rule_empty(conn):
    conn.get_topic_rule.return_value = {}
    assert boto3_iot.describe_topic_rule("r") == {"rule": None}


def test_describe_topic_rule_error(conn, client_error):
    conn.get_topic_rule.side_effect = client_error("Boom", "GetTopicRule")
    assert "error" in boto3_iot.describe_topic_rule("r")


def test_list_topic_rules(conn):
    conn.list_topic_rules.return_value = {"rules": [{"ruleName": "r"}]}
    r = boto3_iot.list_topic_rules(topic="t", ruleDisabled=False)
    assert r == {"rules": [{"ruleName": "r"}]}
    call_kwargs = conn.list_topic_rules.call_args.kwargs
    assert call_kwargs["topic"] == "t"
    assert call_kwargs["ruleDisabled"] is False


def test_list_topic_rules_error(conn, client_error):
    conn.list_topic_rules.side_effect = client_error("Boom", "ListTopicRules")
    assert "error" in boto3_iot.list_topic_rules()
