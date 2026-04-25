"""
Unit tests for the ``boto3_datapipeline`` execution module.
"""

from unittest.mock import MagicMock

import pytest

from saltext.boto3.modules import boto3_datapipeline

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
        boto3_datapipeline: {
            "__opts__": {},
            "__context__": {},
            "__salt__": {},
        }
    }


@pytest.fixture
def conn(make_conn):
    with make_conn(boto3_datapipeline) as client:
        yield client


def test_activate_pipeline(conn):
    assert boto3_datapipeline.activate_pipeline("pid") == {"result": True}
    conn.activate_pipeline.assert_called_once_with(pipelineId="pid")


def test_activate_pipeline_client_error(conn, client_error):
    conn.activate_pipeline.side_effect = client_error("Denied", "ActivatePipeline")
    assert "error" in boto3_datapipeline.activate_pipeline("pid")


def test_create_pipeline(conn):
    conn.create_pipeline.return_value = {"pipelineId": "pid1"}
    result = boto3_datapipeline.create_pipeline("n", "u", description="d")
    assert result == {"result": "pid1"}
    conn.create_pipeline.assert_called_once_with(name="n", uniqueId="u", description="d")


def test_create_pipeline_client_error(conn, client_error):
    conn.create_pipeline.side_effect = client_error("Denied", "CreatePipeline")
    assert "error" in boto3_datapipeline.create_pipeline("n", "u")


def test_delete_pipeline(conn):
    assert boto3_datapipeline.delete_pipeline("pid") == {"result": True}
    conn.delete_pipeline.assert_called_once_with(pipelineId="pid")


def test_delete_pipeline_client_error(conn, client_error):
    conn.delete_pipeline.side_effect = client_error("Denied", "DeletePipeline")
    assert "error" in boto3_datapipeline.delete_pipeline("pid")


def test_describe_pipelines(conn):
    conn.describe_pipelines.return_value = {"pipelineDescriptionList": []}
    result = boto3_datapipeline.describe_pipelines(["pid"])
    assert result["result"] == {"pipelineDescriptionList": []}
    conn.describe_pipelines.assert_called_once_with(pipelineIds=["pid"])


def test_describe_pipelines_client_error(conn, client_error):
    conn.describe_pipelines.side_effect = client_error("Denied", "DescribePipelines")
    assert "error" in boto3_datapipeline.describe_pipelines(["pid"])


def test_get_pipeline_definition(conn):
    conn.get_pipeline_definition.return_value = {"pipelineObjects": []}
    result = boto3_datapipeline.get_pipeline_definition("pid", version="active")
    assert result["result"] == {"pipelineObjects": []}
    conn.get_pipeline_definition.assert_called_once_with(pipelineId="pid", version="active")


def test_get_pipeline_definition_default_version(conn):
    conn.get_pipeline_definition.return_value = {}
    boto3_datapipeline.get_pipeline_definition("pid")
    assert conn.get_pipeline_definition.call_args.kwargs["version"] == "latest"


def test_get_pipeline_definition_client_error(conn, client_error):
    conn.get_pipeline_definition.side_effect = client_error("Denied", "GetPipelineDefinition")
    assert "error" in boto3_datapipeline.get_pipeline_definition("pid")


def test_list_pipelines(conn):
    paginator = MagicMock()
    paginator.paginate.return_value = [
        {"pipelineIdList": [{"id": "a", "name": "A"}]},
        {"pipelineIdList": [{"id": "b", "name": "B"}]},
    ]
    conn.get_paginator.return_value = paginator
    result = boto3_datapipeline.list_pipelines()
    assert result["result"] == [{"id": "a", "name": "A"}, {"id": "b", "name": "B"}]


def test_list_pipelines_client_error(conn, client_error):
    conn.get_paginator.side_effect = client_error("Denied", "ListPipelines")
    assert "error" in boto3_datapipeline.list_pipelines()


def test_pipeline_id_from_name(conn):
    paginator = MagicMock()
    paginator.paginate.return_value = [
        {"pipelineIdList": [{"id": "a", "name": "wrong"}, {"id": "b", "name": "target"}]}
    ]
    conn.get_paginator.return_value = paginator
    assert boto3_datapipeline.pipeline_id_from_name("target") == {"result": "b"}


def test_pipeline_id_from_name_not_found(conn):
    paginator = MagicMock()
    paginator.paginate.return_value = [{"pipelineIdList": []}]
    conn.get_paginator.return_value = paginator
    result = boto3_datapipeline.pipeline_id_from_name("target")
    assert "error" in result
    assert "target" in result["error"]


def test_pipeline_id_from_name_list_client_error(conn, client_error):
    conn.get_paginator.side_effect = client_error("Denied", "ListPipelines")
    assert "error" in boto3_datapipeline.pipeline_id_from_name("target")


def test_put_pipeline_definition(conn):
    conn.put_pipeline_definition.return_value = {"errored": False, "validationErrors": []}
    result = boto3_datapipeline.put_pipeline_definition("pid", [{"id": "a"}])
    assert result["result"]["errored"] is False
    kwargs = conn.put_pipeline_definition.call_args.kwargs
    assert kwargs["pipelineId"] == "pid"
    assert kwargs["pipelineObjects"] == [{"id": "a"}]
    assert kwargs["parameterObjects"] == []
    assert kwargs["parameterValues"] == []


def test_put_pipeline_definition_validation_errors(conn):
    conn.put_pipeline_definition.return_value = {
        "errored": True,
        "validationErrors": [{"errors": ["bad"]}],
    }
    result = boto3_datapipeline.put_pipeline_definition("pid", [])
    assert result["error"] == [{"errors": ["bad"]}]


def test_put_pipeline_definition_client_error(conn, client_error):
    conn.put_pipeline_definition.side_effect = client_error("Denied", "PutPipelineDefinition")
    assert "error" in boto3_datapipeline.put_pipeline_definition("pid", [])
