"""
Unit tests for the ``boto3_datapipeline`` state module.
"""

from unittest.mock import MagicMock

import pytest

from saltext.boto3.states import boto3_datapipeline as datapipeline_state

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
    return {datapipeline_state: {"__opts__": {"test": False}, "__salt__": {}}}


def _with_pillar(salt_map):
    salt_map.setdefault("pillar.get", {})
    return salt_map


def test_virtual(mock_salt):
    with mock_salt(datapipeline_state, {"boto3_datapipeline.create_pipeline": None}):
        assert datapipeline_state.__virtual__() == "boto3_datapipeline"


def test_virtual_no_exec_module(mock_salt):
    with mock_salt(datapipeline_state, {}):
        result = datapipeline_state.__virtual__()
    assert result[0] is False


def test_present_already_matches(mock_salt):
    salt_map = _with_pillar(
        {
            "boto3_datapipeline.pipeline_id_from_name": {"result": "pid"},
            "boto3_datapipeline.get_pipeline_definition": {
                "result": {
                    "pipelineObjects": [],
                    "parameterObjects": [],
                    "parameterValues": [],
                }
            },
        }
    )
    with mock_salt(datapipeline_state, salt_map):
        ret = datapipeline_state.present("p")
    assert ret["result"] is True
    assert "present" in ret["comment"]
    assert not ret["changes"]


def test_present_test_mode(mock_salt):
    salt_map = _with_pillar({"boto3_datapipeline.pipeline_id_from_name": {"error": "no pipe"}})
    with mock_salt(datapipeline_state, salt_map, test=True):
        ret = datapipeline_state.present("p")
    assert ret["result"] is None
    assert "set to be created" in ret["comment"]


def test_present_creates_pipeline(mock_salt):
    get_def = MagicMock(
        side_effect=[
            {"error": "not found"},
            {"result": {"pipelineObjects": [], "parameterObjects": [], "parameterValues": []}},
        ]
    )
    salt_map = _with_pillar(
        {
            "boto3_datapipeline.pipeline_id_from_name": {"error": "no pipe"},
            "boto3_datapipeline.create_pipeline": {"result": "pid"},
            "boto3_datapipeline.put_pipeline_definition": {"result": {"errored": False}},
            "boto3_datapipeline.activate_pipeline": {"result": True},
            "boto3_datapipeline.get_pipeline_definition": get_def,
        }
    )
    with mock_salt(datapipeline_state, salt_map):
        ret = datapipeline_state.present("p")
    assert ret["result"] is True
    assert ret["changes"]["new"] == "Pipeline created."
    assert "created" in ret["comment"]


def test_present_create_pipeline_error(mock_salt):
    salt_map = _with_pillar(
        {
            "boto3_datapipeline.pipeline_id_from_name": {"error": "no pipe"},
            "boto3_datapipeline.create_pipeline": {"error": "denied"},
        }
    )
    with mock_salt(datapipeline_state, salt_map):
        ret = datapipeline_state.present("p")
    assert ret["result"] is False
    assert "denied" in ret["comment"]


def test_present_put_definition_immutable_retries(mock_salt):
    immutable_error = {"error": [{"errors": ["this field can not be changed"]}]}
    create = MagicMock(side_effect=[{"result": "pid1"}, {"result": "pid2"}])
    put_def = MagicMock(side_effect=[immutable_error, {"result": {"errored": False}}])
    get_def = MagicMock(
        side_effect=[
            {"error": "not found"},
            {"result": {"pipelineObjects": [], "parameterObjects": [], "parameterValues": []}},
        ]
    )
    delete_pipeline = MagicMock(return_value={"result": True})
    salt_map = _with_pillar(
        {
            "boto3_datapipeline.pipeline_id_from_name": {"error": "no pipe"},
            "boto3_datapipeline.create_pipeline": create,
            "boto3_datapipeline.put_pipeline_definition": put_def,
            "boto3_datapipeline.delete_pipeline": delete_pipeline,
            "boto3_datapipeline.activate_pipeline": {"result": True},
            "boto3_datapipeline.get_pipeline_definition": get_def,
        }
    )
    with mock_salt(datapipeline_state, salt_map):
        ret = datapipeline_state.present("p")
    assert ret["result"] is True
    assert delete_pipeline.called
    assert create.call_count == 2


def test_absent_already_gone(mock_salt):
    salt_map = {"boto3_datapipeline.pipeline_id_from_name": {"error": "no pipe"}}
    with mock_salt(datapipeline_state, salt_map):
        ret = datapipeline_state.absent("p")
    assert ret["result"] is True
    assert "absent" in ret["comment"]
    assert not ret["changes"]


def test_absent_test_mode(mock_salt):
    salt_map = {"boto3_datapipeline.pipeline_id_from_name": {"result": "pid"}}
    with mock_salt(datapipeline_state, salt_map, test=True):
        ret = datapipeline_state.absent("p")
    assert ret["result"] is None
    assert "set to be deleted" in ret["comment"]


def test_absent_deletes(mock_salt):
    delete_mock = MagicMock(return_value={"result": True})
    salt_map = {
        "boto3_datapipeline.pipeline_id_from_name": {"result": "pid"},
        "boto3_datapipeline.delete_pipeline": delete_mock,
    }
    with mock_salt(datapipeline_state, salt_map):
        ret = datapipeline_state.absent("p")
    assert ret["result"] is True
    assert ret["changes"]["old"] == {"pipeline_id": "pid"}
    assert ret["changes"]["new"] is None
    delete_mock.assert_called_once()


def test_immutable_fields_error_helper():
    assert (
        datapipeline_state._immutable_fields_error(
            {"error": [{"errors": ["this field can not be changed"]}]}
        )
        is True
    )
    assert datapipeline_state._immutable_fields_error({"error": [{"errors": ["other"]}]}) is False


def test_properties_from_dict():
    result = datapipeline_state._properties_from_dict({"a": "1", "b": {"ref": "2"}})
    result_sorted = sorted(result, key=lambda x: x["key"])
    assert result_sorted == [
        {"key": "a", "stringValue": "1"},
        {"key": "b", "refValue": "2"},
    ]


def test_dict_to_list_ids():
    result = datapipeline_state._dict_to_list_ids({"a": {"x": 1}})
    assert result == [{"id": "a", "x": 1}]


def test_cleaned_strips_date():
    pipeline_objects = [
        {
            "id": "DefaultSchedule",
            "fields": [
                {"key": "startDateTime", "stringValue": "2020-01-02T03:04:05"},
            ],
        }
    ]
    cleaned = datapipeline_state._cleaned(pipeline_objects)
    assert cleaned[0]["fields"][0]["stringValue"] == "03:04:05"


def test_recursive_compare_list_mismatch():
    assert datapipeline_state._recursive_compare([1, 2], [1, 2, 3]) is False


def test_recursive_compare_equal_dicts():
    assert datapipeline_state._recursive_compare({"a": [1, 2]}, {"a": [2, 1]}) is True
