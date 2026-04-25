"""
Unit tests for the ``boto3_apigateway`` state module.

Focuses on the usage-plan and usage-plan-association state functions,
which are self-contained and dispatch to the boto3_apigateway execution
module via ``__salt__``. The ``present`` / ``absent`` state functions
relying on the ``_Swagger`` helper are exercised in integration tests.
"""

from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from saltext.boto3.states import boto3_apigateway as apigateway_state

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
    return {
        apigateway_state: {
            "__opts__": {"test": False},
            "__salt__": {},
            "__pillar__": {},
        }
    }


def test_virtual(mock_salt):
    with mock_salt(apigateway_state, {"boto3_apigateway.describe_apis": True}):
        assert apigateway_state.__virtual__() == "boto3_apigateway"


def test_virtual_no_exec_module(mock_salt):
    with mock_salt(apigateway_state, {}):
        result = apigateway_state.__virtual__()
    assert result[0] is False


def test_usage_plan_present_describe_error(mock_salt):
    salt_map = {"boto3_apigateway.describe_usage_plans": {"error": "boom"}}
    with mock_salt(apigateway_state, salt_map):
        ret = apigateway_state.usage_plan_present("state", "myplan")
    assert ret["result"] is False


def test_usage_plan_present_create_test_mode(mock_salt):
    salt_map = {"boto3_apigateway.describe_usage_plans": {"plans": []}}
    with mock_salt(apigateway_state, salt_map, test=True):
        ret = apigateway_state.usage_plan_present("state", "myplan")
    assert ret["result"] is None
    assert "would be created" in ret["comment"]


def test_usage_plan_present_create_success(mock_salt):
    describe = MagicMock(
        side_effect=[
            {"plans": []},
            {"plans": [{"id": "p1", "name": "myplan"}]},
        ]
    )
    salt_map = {
        "boto3_apigateway.describe_usage_plans": describe,
        "boto3_apigateway.create_usage_plan": {"plan": {"id": "p1"}},
    }
    with mock_salt(apigateway_state, salt_map):
        ret = apigateway_state.usage_plan_present("state", "myplan")
    assert ret["result"] is True
    assert ret["changes"]["new"] == {"plan": {"id": "p1", "name": "myplan"}}


def test_usage_plan_present_create_failure(mock_salt):
    salt_map = {
        "boto3_apigateway.describe_usage_plans": {"plans": []},
        "boto3_apigateway.create_usage_plan": {"error": "nope"},
    }
    with mock_salt(apigateway_state, salt_map):
        ret = apigateway_state.usage_plan_present("state", "myplan")
    assert ret["result"] is False
    assert "Failed to create" in ret["comment"]


def test_usage_plan_present_already_correct(mock_salt):
    existing = {
        "plans": [
            {
                "id": "p1",
                "name": "myplan",
                "throttle": {"rateLimit": 70, "burstLimit": 100},
                "quota": {"limit": 1000, "offset": 0, "period": "DAY"},
            }
        ]
    }
    salt_map = {"boto3_apigateway.describe_usage_plans": existing}
    with mock_salt(apigateway_state, salt_map):
        ret = apigateway_state.usage_plan_present(
            "state",
            "myplan",
            throttle={"rateLimit": 70, "burstLimit": 100},
            quota={"limit": 1000, "offset": 0, "period": "DAY"},
        )
    assert ret["result"] is True
    assert not ret["changes"]
    assert "already in a correct state" in ret["comment"]


def test_usage_plan_present_update_test_mode(mock_salt):
    existing = {
        "plans": [
            {
                "id": "p1",
                "name": "myplan",
                "throttle": {"rateLimit": 70, "burstLimit": 100},
            }
        ]
    }
    salt_map = {"boto3_apigateway.describe_usage_plans": existing}
    with mock_salt(apigateway_state, salt_map, test=True):
        ret = apigateway_state.usage_plan_present(
            "state", "myplan", throttle={"rateLimit": 200, "burstLimit": 400}
        )
    assert ret["result"] is None
    assert "would be updated" in ret["comment"]


def test_usage_plan_absent_missing_is_noop(mock_salt):
    salt_map = {"boto3_apigateway.describe_usage_plans": {"plans": []}}
    with mock_salt(apigateway_state, salt_map):
        ret = apigateway_state.usage_plan_absent("state", "myplan")
    assert ret["result"] is True
    assert not ret["changes"]
    assert "does not exist" in ret["comment"]


def test_usage_plan_absent_describe_error(mock_salt):
    salt_map = {"boto3_apigateway.describe_usage_plans": {"error": "boom"}}
    with mock_salt(apigateway_state, salt_map):
        ret = apigateway_state.usage_plan_absent("state", "myplan")
    assert ret["result"] is False


def test_usage_plan_absent_test_mode(mock_salt):
    salt_map = {
        "boto3_apigateway.describe_usage_plans": {"plans": [{"id": "p1", "name": "myplan"}]}
    }
    with mock_salt(apigateway_state, salt_map, test=True):
        ret = apigateway_state.usage_plan_absent("state", "myplan")
    assert ret["result"] is None
    assert "would be deleted" in ret["comment"]


def test_usage_plan_absent_delete_success(mock_salt):
    salt_map = {
        "boto3_apigateway.describe_usage_plans": {"plans": [{"id": "p1", "name": "myplan"}]},
        "boto3_apigateway.delete_usage_plan": {"deleted": True, "usagePlanId": "p1"},
    }
    with mock_salt(apigateway_state, salt_map):
        ret = apigateway_state.usage_plan_absent("state", "myplan")
    assert ret["result"] is True
    assert ret["changes"]["old"] == {"plan": {"id": "p1", "name": "myplan"}}
    assert ret["changes"]["new"] == {"plan": None}


def test_usage_plan_absent_delete_failure(mock_salt):
    salt_map = {
        "boto3_apigateway.describe_usage_plans": {"plans": [{"id": "p1", "name": "myplan"}]},
        "boto3_apigateway.delete_usage_plan": {"error": "nope"},
    }
    with mock_salt(apigateway_state, salt_map):
        ret = apigateway_state.usage_plan_absent("state", "myplan")
    assert ret["result"] is False
    assert "Failed to delete" in ret["comment"]


def test_usage_plan_association_present_plan_missing(mock_salt):
    salt_map = {"boto3_apigateway.describe_usage_plans": {"plans": []}}
    with mock_salt(apigateway_state, salt_map):
        ret = apigateway_state.usage_plan_association_present(
            "state", "myplan", [{"apiId": "a1", "stage": "prod"}]
        )
    assert ret["result"] is False


def test_usage_plan_association_present_already_attached(mock_salt):
    existing = {
        "plans": [
            {
                "id": "p1",
                "name": "myplan",
                "apiStages": [{"apiId": "a1", "stage": "prod"}],
            }
        ]
    }
    salt_map = {"boto3_apigateway.describe_usage_plans": existing}
    with mock_salt(apigateway_state, salt_map):
        ret = apigateway_state.usage_plan_association_present(
            "state", "myplan", [{"apiId": "a1", "stage": "prod"}]
        )
    assert ret["result"] is True
    assert not ret["changes"]


def test_usage_plan_association_present_attach(mock_salt):
    existing = {"plans": [{"id": "p1", "name": "myplan", "apiStages": []}]}
    attached_stages = [{"apiId": "a1", "stage": "prod"}]
    salt_map = {
        "boto3_apigateway.describe_usage_plans": existing,
        "boto3_apigateway.attach_usage_plan_to_apis": {
            "success": True,
            "result": {"apiStages": attached_stages},
        },
    }
    with mock_salt(apigateway_state, salt_map):
        ret = apigateway_state.usage_plan_association_present("state", "myplan", attached_stages)
    assert ret["result"] is True
    assert ret["changes"]["new"] == attached_stages


def test_usage_plan_association_absent_plan_missing(mock_salt):
    salt_map = {"boto3_apigateway.describe_usage_plans": {"plans": []}}
    with mock_salt(apigateway_state, salt_map):
        ret = apigateway_state.usage_plan_association_absent(
            "state", "myplan", [{"apiId": "a1", "stage": "prod"}]
        )
    assert ret["result"] is False


def test_usage_plan_association_absent_nothing_attached(mock_salt):
    existing = {"plans": [{"id": "p1", "name": "myplan", "apiStages": []}]}
    salt_map = {"boto3_apigateway.describe_usage_plans": existing}
    with mock_salt(apigateway_state, salt_map):
        ret = apigateway_state.usage_plan_association_absent(
            "state", "myplan", [{"apiId": "a1", "stage": "prod"}]
        )
    assert ret["result"] is True
    assert not ret["changes"]


def test_usage_plan_association_absent_detach(mock_salt):
    existing = {
        "plans": [
            {
                "id": "p1",
                "name": "myplan",
                "apiStages": [{"apiId": "a1", "stage": "prod"}],
            }
        ]
    }
    salt_map = {
        "boto3_apigateway.describe_usage_plans": existing,
        "boto3_apigateway.detach_usage_plan_from_apis": {"success": True},
    }
    with mock_salt(apigateway_state, salt_map):
        ret = apigateway_state.usage_plan_association_absent(
            "state", "myplan", [{"apiId": "a1", "stage": "prod"}]
        )
    assert ret["result"] is True
    assert ret["changes"]["old"] == [{"apiId": "a1", "stage": "prod"}]


def test_get_stage_variables_none():
    assert apigateway_state._get_stage_variables(None) == {}


def test_get_stage_variables_dict_passthrough():
    payload = {"foo": "bar"}
    assert apigateway_state._get_stage_variables(payload) == payload


def test_get_stage_variables_str_not_found():
    with (
        patch.dict(apigateway_state.__opts__, {}, clear=True),
        patch.dict(apigateway_state.__pillar__, {}, clear=True),
    ):
        assert apigateway_state._get_stage_variables("missing") == {}


def test_get_stage_variables_str_in_opts():
    with (
        patch.dict(apigateway_state.__opts__, {"vars": {"a": 1}}),
        patch.dict(apigateway_state.__pillar__, {}, clear=True),
    ):
        assert apigateway_state._get_stage_variables("vars") == {"a": 1}


def test_get_stage_variables_master_overrides_opts():
    with (
        patch.dict(apigateway_state.__opts__, {"vars": {"a": 1}}),
        patch.dict(apigateway_state.__pillar__, {"master": {"vars": {"b": 2}}}, clear=True),
    ):
        assert apigateway_state._get_stage_variables("vars") == {"b": 2}


def test_get_stage_variables_pillar_overrides_master():
    with (
        patch.dict(apigateway_state.__opts__, {"vars": {"a": 1}}),
        patch.dict(
            apigateway_state.__pillar__,
            {"master": {"vars": {"b": 2}}, "vars": {"c": 3}},
            clear=True,
        ),
    ):
        assert apigateway_state._get_stage_variables("vars") == {"c": 3}


def test_get_stage_variables_non_dict_value_returns_empty():
    with patch.dict(apigateway_state.__pillar__, {"vars": "not-a-dict"}, clear=True):
        assert apigateway_state._get_stage_variables("vars") == {}
