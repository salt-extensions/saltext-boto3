"""
Unit tests for the ``boto3_apigateway`` execution module.
"""

import pytest

from saltext.boto3.modules import boto3_apigateway

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
        boto3_apigateway: {
            "__opts__": {},
            "__context__": {},
            "__salt__": {},
        }
    }


@pytest.fixture
def conn(make_conn):
    with make_conn(boto3_apigateway) as client:
        yield client


def test_describe_apis_no_filter(conn):
    conn.get_rest_apis.return_value = {
        "items": [
            {"id": "1", "name": "a", "description": "d1"},
            {"id": "2", "name": "b", "description": "d2"},
        ]
    }
    result = boto3_apigateway.describe_apis()
    assert [a["id"] for a in result["restapi"]] == ["1", "2"]


def test_describe_apis_filter_by_name(conn):
    conn.get_rest_apis.return_value = {
        "items": [
            {"id": "1", "name": "a", "description": "d1"},
            {"id": "2", "name": "b", "description": "d2"},
        ]
    }
    result = boto3_apigateway.describe_apis(name="b")
    assert [a["id"] for a in result["restapi"]] == ["2"]


def test_describe_apis_filter_by_description(conn):
    conn.get_rest_apis.return_value = {
        "items": [
            {"id": "1", "name": "a", "description": "d1"},
            {"id": "2", "name": "a", "description": "d2"},
        ]
    }
    result = boto3_apigateway.describe_apis(name="a", description="d2")
    assert [a["id"] for a in result["restapi"]] == ["2"]


def test_describe_apis_paginates(conn):
    conn.get_rest_apis.side_effect = [
        {"items": [{"id": "1", "name": "a", "description": ""}], "position": "p1"},
        {"items": [{"id": "2", "name": "b", "description": ""}]},
    ]
    result = boto3_apigateway.describe_apis()
    assert {a["id"] for a in result["restapi"]} == {"1", "2"}
    assert conn.get_rest_apis.call_count == 2


def test_describe_apis_client_error(conn, client_error):
    conn.get_rest_apis.side_effect = client_error("AuthFailure", "GetRestApis")
    result = boto3_apigateway.describe_apis()
    assert "error" in result


def test_api_exists_true(conn):
    conn.get_rest_apis.return_value = {"items": [{"id": "1", "name": "a", "description": "d"}]}
    assert boto3_apigateway.api_exists("a") == {"exists": True}


def test_api_exists_false(conn):
    conn.get_rest_apis.return_value = {"items": []}
    assert boto3_apigateway.api_exists("a") == {"exists": False}


def test_create_api_success(conn):
    conn.create_rest_api.return_value = {"id": "new-id", "name": "a", "description": "d"}
    result = boto3_apigateway.create_api("a", "d")
    assert result["created"] is True
    assert result["restapi"]["id"] == "new-id"
    conn.create_rest_api.assert_called_once_with(name="a", description="d")


def test_create_api_clone_from(conn):
    conn.create_rest_api.return_value = {"id": "new-id", "name": "a", "description": "d"}
    boto3_apigateway.create_api("a", "d", cloneFrom="src-id")
    conn.create_rest_api.assert_called_once_with(name="a", description="d", cloneFrom="src-id")


def test_create_api_client_error(conn, client_error):
    conn.create_rest_api.side_effect = client_error("AuthFailure", "CreateRestApi")
    result = boto3_apigateway.create_api("a", "d")
    assert result["created"] is False
    assert "error" in result


def test_delete_api_deletes_matches(conn):
    conn.get_rest_apis.return_value = {
        "items": [
            {"id": "1", "name": "a", "description": "d"},
            {"id": "2", "name": "a", "description": "d"},
        ]
    }
    result = boto3_apigateway.delete_api("a")
    assert result == {"deleted": True, "count": 2}
    assert conn.delete_rest_api.call_count == 2


def test_delete_api_none_found(conn):
    conn.get_rest_apis.return_value = {"items": []}
    assert boto3_apigateway.delete_api("a") == {"deleted": False}
    conn.delete_rest_api.assert_not_called()


def test_delete_api_client_error(conn, client_error):
    conn.get_rest_apis.side_effect = client_error("AuthFailure", "GetRestApis")
    # the lookup catches the ClientError internally and returns an error dict;
    # with no "restapi" key, delete_api reports not-deleted.
    result = boto3_apigateway.delete_api("a")
    assert result == {"deleted": False}


def test_describe_api_resources_sorted(conn):
    conn.get_resources.return_value = {
        "items": [{"id": "r2", "path": "/b"}, {"id": "r1", "path": "/a"}]
    }
    result = boto3_apigateway.describe_api_resources("api-id")
    assert [r["path"] for r in result["resources"]] == ["/a", "/b"]


def test_describe_api_resource_found(conn):
    conn.get_resources.return_value = {
        "items": [{"id": "r1", "path": "/a"}, {"id": "r2", "path": "/b"}]
    }
    result = boto3_apigateway.describe_api_resource("api-id", "/b")
    assert result == {"resource": {"id": "r2", "path": "/b"}}


def test_describe_api_resource_not_found(conn):
    conn.get_resources.return_value = {"items": [{"id": "r1", "path": "/a"}]}
    assert boto3_apigateway.describe_api_resource("api-id", "/missing") == {"resource": None}


def test_describe_api_key(conn):
    conn.get_api_key.return_value = {"id": "k1", "name": "n"}
    result = boto3_apigateway.describe_api_key("k1")
    assert result == {"apiKey": {"id": "k1", "name": "n"}}


def test_describe_api_keys(conn):
    conn.get_api_keys.return_value = {
        "items": [{"id": "k1", "name": "n1"}, {"id": "k2", "name": "n2"}]
    }
    result = boto3_apigateway.describe_api_keys()
    assert [k["id"] for k in result["apiKeys"]] == ["k1", "k2"]


def test_create_api_key_default_stagekeys(conn):
    conn.create_api_key.return_value = {"id": "k1"}
    result = boto3_apigateway.create_api_key("n", "d")
    assert result["created"] is True
    conn.create_api_key.assert_called_once_with(
        name="n", description="d", enabled=True, stageKeys=[]
    )


def test_create_api_key_client_error(conn, client_error):
    conn.create_api_key.side_effect = client_error("AuthFailure", "CreateApiKey")
    result = boto3_apigateway.create_api_key("n", "d")
    assert result["created"] is False
    assert "error" in result


def test_enable_api_key(conn):
    conn.update_api_key.return_value = {"id": "k1", "enabled": True}
    result = boto3_apigateway.enable_api_key("k1")
    assert result["apiKey"]["enabled"] is True
    conn.update_api_key.assert_called_once()
    kwargs = conn.update_api_key.call_args.kwargs
    assert kwargs["apiKey"] == "k1"
    assert kwargs["patchOperations"][0]["value"] == "True"


def test_disable_api_key(conn):
    conn.update_api_key.return_value = {"id": "k1", "enabled": False}
    boto3_apigateway.disable_api_key("k1")
    kwargs = conn.update_api_key.call_args.kwargs
    assert kwargs["patchOperations"][0]["value"] == "False"


def test_describe_api_stage(conn):
    conn.get_stage.return_value = {"stageName": "prod"}
    result = boto3_apigateway.describe_api_stage("api-id", "prod")
    assert result == {"stage": {"stageName": "prod"}}
    conn.get_stage.assert_called_once_with(restApiId="api-id", stageName="prod")


def test_describe_api_stages(conn):
    conn.get_stages.return_value = {"item": [{"stageName": "prod"}, {"stageName": "dev"}]}
    result = boto3_apigateway.describe_api_stages("api-id", "dep-id")
    assert [s["stageName"] for s in result["stages"]] == ["prod", "dev"]


def test_create_api_stage(conn):
    conn.create_stage.return_value = {"stageName": "prod"}
    result = boto3_apigateway.create_api_stage("api-id", "prod", "dep-id")
    assert result["created"] is True
    conn.create_stage.assert_called_once_with(
        restApiId="api-id",
        stageName="prod",
        deploymentId="dep-id",
        description="",
        cacheClusterEnabled=False,
        cacheClusterSize="0.5",
        variables={},
    )


def test_delete_api_stage(conn):
    conn.delete_stage.return_value = {}
    result = boto3_apigateway.delete_api_stage("api-id", "prod")
    assert result == {"deleted": True}
    conn.delete_stage.assert_called_once_with(restApiId="api-id", stageName="prod")


def test_delete_api_stage_client_error(conn, client_error):
    conn.delete_stage.side_effect = client_error("NotFound", "DeleteStage")
    result = boto3_apigateway.delete_api_stage("api-id", "prod")
    assert result["deleted"] is False
    assert "error" in result


def test_describe_usage_plans_by_name(conn):
    conn.get_usage_plans.return_value = {
        "items": [
            {"id": "p1", "name": "plan1"},
            {"id": "p2", "name": "plan2"},
        ]
    }
    result = boto3_apigateway.describe_usage_plans(name="plan2")
    assert [p["id"] for p in result["plans"]] == ["p2"]


def test_describe_usage_plans_by_id(conn):
    conn.get_usage_plans.return_value = {
        "items": [
            {"id": "p1", "name": "plan1"},
            {"id": "p2", "name": "plan2"},
        ]
    }
    result = boto3_apigateway.describe_usage_plans(plan_id="p1")
    assert [p["id"] for p in result["plans"]] == ["p1"]


def test_attach_usage_plan_to_apis(conn):
    conn.update_usage_plan.return_value = {"id": "p1"}
    apis = [{"apiId": "api-1", "stage": "prod"}]
    result = boto3_apigateway.attach_usage_plan_to_apis("p1", apis)
    assert result["success"] is True
    kwargs = conn.update_usage_plan.call_args.kwargs
    ops = kwargs["patchOperations"]
    assert ops[0]["op"] == "add"
    assert ops[0]["value"] == "api-1:prod"


def test_detach_usage_plan_from_apis(conn):
    conn.update_usage_plan.return_value = {"id": "p1"}
    apis = [{"apiId": "api-1", "stage": "prod"}]
    boto3_apigateway.detach_usage_plan_from_apis("p1", apis)
    ops = conn.update_usage_plan.call_args.kwargs["patchOperations"]
    assert ops[0]["op"] == "remove"


def test_update_usage_plan_empty_apis_noop(conn):
    result = boto3_apigateway.attach_usage_plan_to_apis("p1", [])
    assert result == {"success": True, "result": None}
    conn.update_usage_plan.assert_not_called()
