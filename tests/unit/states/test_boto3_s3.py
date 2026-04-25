"""
Unit tests for the ``boto3_s3`` state module.
"""

import hashlib
from unittest.mock import MagicMock

import pytest

from saltext.boto3.states import boto3_s3 as s3_state

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
        s3_state: {
            "__opts__": {"test": False, "hash_type": "sha256"},
            "__salt__": {},
        }
    }


@pytest.fixture
def source_file(tmp_path):
    f = tmp_path / "src.txt"
    f.write_bytes(b"hello world")
    return str(f)


@pytest.fixture
def digest(source_file):
    with open(source_file, "rb") as fh:
        return hashlib.sha256(fh.read()).hexdigest()


def test_virtual(mock_salt):
    with mock_salt(s3_state, {"boto3_s3.get_object_metadata": MagicMock()}):
        assert s3_state.__virtual__() == "boto3_s3"


def test_virtual_no_exec_module(mock_salt):
    with mock_salt(s3_state, {}):
        result = s3_state.__virtual__()
    assert result[0] is False


def test_object_present_already_present(mock_salt, source_file, digest):
    metadata = {"Metadata": {"salt_managed_content_hash": digest}}
    salt_map = {
        "config.option": {},
        "boto3_s3.get_object_metadata": {"result": metadata},
        "boto3_s3.upload_file": MagicMock(),
    }
    with mock_salt(s3_state, salt_map) as salt:
        ret = s3_state.object_present("bucket/key", source=source_file)
    assert ret["result"] is True
    assert "is present" in ret["comment"]
    salt["boto3_s3.upload_file"].assert_not_called()


def test_object_present_create(mock_salt, source_file):
    salt_map = {
        "config.option": {},
        "boto3_s3.get_object_metadata": {"result": None},
        "boto3_s3.upload_file": {"result": True},
    }
    with mock_salt(s3_state, salt_map) as salt:
        ret = s3_state.object_present("bucket/key", source=source_file)
    assert ret["result"] is True
    assert "created" in ret["comment"]
    salt["boto3_s3.upload_file"].assert_called_once()


def test_object_present_test_mode(mock_salt, source_file):
    salt_map = {
        "config.option": {},
        "boto3_s3.get_object_metadata": {"result": None},
        "boto3_s3.upload_file": MagicMock(),
    }
    with mock_salt(s3_state, salt_map, test=True) as salt:
        ret = s3_state.object_present("bucket/key", source=source_file)
    assert ret["result"] is None
    assert "set to be create" in ret["comment"]
    salt["boto3_s3.upload_file"].assert_not_called()


def test_object_present_upload_error(mock_salt, source_file):
    salt_map = {
        "config.option": {},
        "boto3_s3.get_object_metadata": {"result": None},
        "boto3_s3.upload_file": {"error": "boom"},
    }
    with mock_salt(s3_state, salt_map):
        ret = s3_state.object_present("bucket/key", source=source_file)
    assert ret["result"] is False
    assert "Failed" in ret["comment"]


def test_object_present_metadata_error(mock_salt, source_file):
    salt_map = {
        "config.option": {},
        "boto3_s3.get_object_metadata": {"error": "denied"},
    }
    with mock_salt(s3_state, salt_map):
        ret = s3_state.object_present("bucket/key", source=source_file)
    assert ret["result"] is False
    assert "Failed to check" in ret["comment"]


def test_object_present_invalid_extra_arg(mock_salt, source_file):
    with mock_salt(s3_state, {"config.option": {}}):
        ret = s3_state.object_present(
            "bucket/key", source=source_file, extra_args={"ACL": "private"}
        )
    assert "error" in ret
    assert "ACL" in ret["error"]


def test_object_present_update(mock_salt, source_file, digest):
    metadata = {
        "Metadata": {"salt_managed_content_hash": digest},
        "ContentType": "text/html",
    }
    salt_map = {
        "config.option": {},
        "boto3_s3.get_object_metadata": {"result": metadata},
        "boto3_s3.upload_file": {"result": True},
    }
    with mock_salt(s3_state, salt_map):
        ret = s3_state.object_present(
            "bucket/key",
            source=source_file,
            extra_args={"ContentType": "text/plain"},
        )
    assert ret["result"] is True
    assert "updated" in ret["comment"]


def test_object_present_source_missing(mock_salt):
    with mock_salt(s3_state, {"config.option": {}}):
        ret = s3_state.object_present("bucket/key", source="/does/not/exist")
    assert ret["result"] is False
    assert "Could not read" in ret["comment"]
