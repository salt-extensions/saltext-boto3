"""
Unit tests for the ``boto3_sqs`` state module.
"""

import textwrap
from unittest.mock import MagicMock

import pytest

from saltext.boto3.states import boto3_sqs as sqs_state

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
        sqs_state: {
            "__opts__": {"test": False},
            "__salt__": {},
        }
    }


def test_virtual(mock_salt):
    with mock_salt(sqs_state, {"boto3_sqs.exists": True}):
        assert sqs_state.__virtual__() == "boto3_sqs"


def test_virtual_no_exec_module(mock_salt):
    with mock_salt(sqs_state, {}):
        result = sqs_state.__virtual__()
    assert result[0] is False
    assert "boto3_sqs" in result[1]


def test_present_create_failure(mock_salt):
    salt_map = {
        "boto3_sqs.exists": MagicMock(return_value={"result": False}),
        "boto3_sqs.create": {"error": "create error"},
        "boto3_sqs.get_attributes": {"result": {}},
    }
    with mock_salt(sqs_state, salt_map, test=False):
        ret = sqs_state.present("mysqs")
    assert ret["result"] is False
    assert ret["name"] == "mysqs"
    assert ret["comment"] == ["Failed to create SQS queue mysqs: create error"]


def test_present_test_mode_create(mock_salt):
    salt_map = {
        "boto3_sqs.exists": MagicMock(return_value={"result": False}),
        "boto3_sqs.get_attributes": {"result": {}},
    }
    with mock_salt(sqs_state, salt_map, test=True):
        ret = sqs_state.present("mysqs")
    assert ret["result"] is None
    assert ret["comment"] == ["SQS queue mysqs is set to be created."]
    assert ret["changes"] == {"old": None, "new": "mysqs"}


def test_present_test_mode_attributes_diff(mock_salt):
    salt_map = {
        "boto3_sqs.exists": MagicMock(return_value={"result": True}),
        "boto3_sqs.get_attributes": {"result": {}},
    }
    with mock_salt(sqs_state, salt_map, test=True):
        ret = sqs_state.present("mysqs", {"DelaySeconds": 20})
    diff = textwrap.dedent("""\
        ---
        +++
        @@ -1 +1 @@
        -{}
        +DelaySeconds: 20

    """).splitlines()
    for idx in (0, 1):
        diff[idx] += " "
    diff = "\n".join(diff)
    assert ret["result"] is None
    assert ret["comment"] == [
        "SQS queue mysqs present.",
        f"Attribute(s) DelaySeconds set to be updated:\n{diff}",
    ]
    assert ret["changes"] == {"attributes": {"diff": diff}}


def test_present_already_present(mock_salt):
    salt_map = {
        "boto3_sqs.exists": MagicMock(return_value={"result": True}),
        "boto3_sqs.get_attributes": {"result": {}},
    }
    with mock_salt(sqs_state, salt_map, test=False):
        ret = sqs_state.present("mysqs")
    assert ret["result"] is True
    assert ret["comment"] == ["SQS queue mysqs present."]
    assert not ret["changes"]


def test_absent_not_present(mock_salt):
    salt_map = {"boto3_sqs.exists": MagicMock(return_value={"result": False})}
    with mock_salt(sqs_state, salt_map, test=False):
        ret = sqs_state.absent("test.example.com.")
    assert ret["result"] is True
    assert ret["comment"] == "SQS queue test.example.com. does not exist in None."


def test_absent_test_mode(mock_salt):
    salt_map = {"boto3_sqs.exists": MagicMock(return_value={"result": True})}
    with mock_salt(sqs_state, salt_map, test=True):
        ret = sqs_state.absent("test.example.com.")
    assert ret["result"] is None
    assert ret["comment"] == "SQS queue test.example.com. is set to be removed."
    assert ret["changes"] == {"old": "test.example.com.", "new": None}
