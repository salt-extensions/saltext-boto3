"""
Unit tests for the ``boto3_cfn`` state module.
"""

from unittest.mock import MagicMock

import pytest

from saltext.boto3.states import boto3_cfn as cfn_state

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
    return {cfn_state: {"__opts__": {"test": False}, "__salt__": {}}}


def _defaults(**overrides):
    salt_map = {
        "boto3_cfn.exists": False,
        "boto3_cfn.describe": {"stack": {}},
        "boto3_cfn.create": {"StackId": "sid"},
        "boto3_cfn.update_stack": {"StackId": "sid"},
        "boto3_cfn.delete": {"ResponseMetadata": {}},
        "boto3_cfn.get_template": {"TemplateBody": "{}"},
        "boto3_cfn.validate_template": {"Parameters": []},
        "cp.get_file_str": "{}",
    }
    salt_map.update(overrides)
    return salt_map


def test_virtual(mock_salt):
    with mock_salt(cfn_state, _defaults()):
        assert cfn_state.__virtual__() == "boto3_cfn"


def test_virtual_no_exec_module(mock_salt):
    with mock_salt(cfn_state, {}):
        result = cfn_state.__virtual__()
    assert result[0] is False
    assert "boto3_cfn" in result[1]


def test_present_validate_failure(mock_salt):
    salt_map = _defaults(**{"boto3_cfn.validate_template": "bad template"})
    with mock_salt(cfn_state, salt_map) as salt_mocks:
        ret = cfn_state.present("mystack", template_body="{}")
    assert ret["result"] is False
    assert "Template could not be validated" in ret["comment"]
    assert salt_mocks  # sanity


def test_present_creates_when_missing(mock_salt):
    with mock_salt(cfn_state, _defaults()) as salt_mocks:
        ret = cfn_state.present("mystack", template_body="{}")
    assert ret["result"] is True
    assert ret["changes"] == {"new": {"StackId": "sid"}}
    salt_mocks["boto3_cfn.create"].assert_called_once()


def test_present_create_test_mode(mock_salt):
    with mock_salt(cfn_state, _defaults(), test=True) as salt_mocks:
        ret = cfn_state.present("mystack", template_body="{}")
    assert ret["result"] is None
    assert "set to be created" in ret["comment"]
    salt_mocks["boto3_cfn.create"].assert_not_called()


def test_present_create_failure(mock_salt):
    salt_map = _defaults(**{"boto3_cfn.create": False})
    with mock_salt(cfn_state, salt_map):
        ret = cfn_state.present("mystack", template_body="{}")
    assert ret["result"] is False


def test_present_existing_same_template(mock_salt):
    salt_map = _defaults(
        **{
            "boto3_cfn.exists": True,
            "boto3_cfn.get_template": {"TemplateBody": {"a": 1}},
        }
    )
    with mock_salt(cfn_state, salt_map) as salt_mocks:
        ret = cfn_state.present("mystack", template_body='{"a": 1}')
    assert ret["result"] is True
    assert not ret["changes"]
    assert "exists" in ret["comment"]
    salt_mocks["boto3_cfn.update_stack"].assert_not_called()


def test_present_existing_update(mock_salt):
    salt_map = _defaults(
        **{
            "boto3_cfn.exists": True,
            "boto3_cfn.get_template": {"TemplateBody": {"a": 1}},
        }
    )
    with mock_salt(cfn_state, salt_map) as salt_mocks:
        ret = cfn_state.present("mystack", template_body='{"a": 2}')
    assert ret["result"] is True
    assert ret["changes"] == {"new": {"StackId": "sid"}}
    salt_mocks["boto3_cfn.update_stack"].assert_called_once()


def test_present_existing_update_test_mode(mock_salt):
    salt_map = _defaults(
        **{
            "boto3_cfn.exists": True,
            "boto3_cfn.get_template": {"TemplateBody": {"a": 1}},
        }
    )
    with mock_salt(cfn_state, salt_map, test=True) as salt_mocks:
        ret = cfn_state.present("mystack", template_body='{"a": 2}')
    assert ret["result"] is None
    assert "set to be updated" in ret["comment"]
    salt_mocks["boto3_cfn.update_stack"].assert_not_called()


def test_present_update_failure(mock_salt):
    salt_map = _defaults(
        **{
            "boto3_cfn.exists": True,
            "boto3_cfn.get_template": {"TemplateBody": {"a": 1}},
            "boto3_cfn.update_stack": "update failed",
        }
    )
    with mock_salt(cfn_state, salt_map):
        ret = cfn_state.present("mystack", template_body='{"a": 2}')
    assert ret["result"] is False
    assert "could not be updated" in ret["comment"]


def test_present_get_template_error(mock_salt):
    salt_map = _defaults(
        **{
            "boto3_cfn.exists": True,
            "boto3_cfn.get_template": "error msg",
        }
    )
    with mock_salt(cfn_state, salt_map):
        ret = cfn_state.present("mystack", template_body='{"a": 2}')
    assert ret["result"] is False
    assert "Could not retrieve stack template" in ret["comment"]


def test_absent_when_missing(mock_salt):
    with mock_salt(cfn_state, _defaults()) as salt_mocks:
        ret = cfn_state.absent("mystack")
    assert ret["result"] is True
    assert "does not exist" in ret["comment"]
    salt_mocks["boto3_cfn.delete"].assert_not_called()


def test_absent_test_mode(mock_salt):
    salt_map = _defaults(**{"boto3_cfn.exists": True})
    with mock_salt(cfn_state, salt_map, test=True) as salt_mocks:
        ret = cfn_state.absent("mystack")
    assert ret["result"] is None
    assert "set to be deleted" in ret["comment"]
    salt_mocks["boto3_cfn.delete"].assert_not_called()


def test_absent_delete_success(mock_salt):
    salt_map = _defaults(**{"boto3_cfn.exists": True})
    with mock_salt(cfn_state, salt_map):
        ret = cfn_state.absent("mystack")
    assert ret["result"] is True
    assert ret["changes"] == {"deleted": "mystack"}


def test_absent_delete_failure_str(mock_salt):
    salt_map = _defaults(**{"boto3_cfn.exists": True, "boto3_cfn.delete": "delete failed"})
    with mock_salt(cfn_state, salt_map):
        ret = cfn_state.absent("mystack")
    assert ret["result"] is False
    assert "could not be deleted" in ret["comment"]


def test_get_template_helper_salt_url(mock_salt):
    cp_mock = MagicMock(return_value='{"a": 1}')
    with mock_salt(cfn_state, {"cp.get_file_str": cp_mock}):
        result = cfn_state._get_template("salt://my.json", "mystack")
    assert result == '{"a": 1}'
    cp_mock.assert_called_once_with("salt://my.json")


def test_get_template_helper_passthrough():
    assert cfn_state._get_template('{"a": 1}', "mystack") == '{"a": 1}'
    assert cfn_state._get_template(None, "mystack") is None
