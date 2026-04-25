"""
Unit tests for the ``boto3_s3_bucket`` state module.
"""

import pytest

from saltext.boto3.states import boto3_s3_bucket as s3_bucket_state

try:
    import botocore  # pylint: disable=unused-import

    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

pytestmark = [
    pytest.mark.skip_on_fips_enabled_platform,
    pytest.mark.skipif(HAS_BOTO3 is False, reason="botocore is required for these tests."),
]


ERR = {"error": {"message": "boom"}}

BASIC_DESCRIBE = {
    "bucket": {
        "ACL": {"Owner": {"ID": "o"}, "Grants": []},
        "Location": {"LocationConstraint": None},
        "Logging": {},
        "Versioning": {},
        "RequestPayment": {"Payer": "BucketOwner"},
        "NotificationConfiguration": {},
    }
}


@pytest.fixture
def configure_loader_modules():
    return {s3_bucket_state: {"__opts__": {"test": False}, "__salt__": {}}}


def test_virtual(mock_salt):
    with mock_salt(s3_bucket_state, {"boto3_s3_bucket.exists": True}):
        assert s3_bucket_state.__virtual__() == "boto3_s3_bucket"


def test_virtual_no_exec_module(mock_salt):
    with mock_salt(s3_bucket_state, {}):
        result = s3_bucket_state.__virtual__()
    assert result[0] is False


def test_present_exists_error(mock_salt):
    with mock_salt(s3_bucket_state, {"boto3_s3_bucket.exists": ERR}):
        ret = s3_bucket_state.present("foo", Bucket="b")
    assert ret["result"] is False
    assert "Failed to create bucket" in ret["comment"]


def test_present_create_test_mode(mock_salt):
    salt_map = {"boto3_s3_bucket.exists": {"exists": False}}
    with mock_salt(s3_bucket_state, salt_map, test=True):
        ret = s3_bucket_state.present("foo", Bucket="b")
    assert ret["result"] is None
    assert "set to be created" in ret["comment"]


def test_present_create_happy_path(mock_salt):
    salt_map = {
        "boto3_s3_bucket.exists": {"exists": False},
        "boto3_s3_bucket.create": {"created": True},
        "boto3_s3_bucket.put_acl": {"updated": True},
        "boto3_s3_bucket.put_notification_configuration": {"updated": True},
        "boto3_s3_bucket.put_request_payment": {"updated": True},
        "boto3_s3_bucket.describe": BASIC_DESCRIBE,
    }
    with mock_salt(s3_bucket_state, salt_map):
        ret = s3_bucket_state.present("foo", Bucket="b")
    assert ret["result"] is True
    assert "created" in ret["comment"]
    assert ret["changes"]["old"] == {"bucket": None}


def test_present_create_failure(mock_salt):
    salt_map = {
        "boto3_s3_bucket.exists": {"exists": False},
        "boto3_s3_bucket.create": {"created": False, "error": {"message": "oops"}},
    }
    with mock_salt(s3_bucket_state, salt_map):
        ret = s3_bucket_state.present("foo", Bucket="b")
    assert ret["result"] is False
    assert "Failed to create bucket" in ret["comment"]


def test_present_create_setter_failure(mock_salt):
    salt_map = {
        "boto3_s3_bucket.exists": {"exists": False},
        "boto3_s3_bucket.create": {"created": True},
        "boto3_s3_bucket.put_acl": {"updated": True},
        "boto3_s3_bucket.put_cors": {"updated": False, "error": {"message": "bad"}},
        "boto3_s3_bucket.put_notification_configuration": {"updated": True},
        "boto3_s3_bucket.put_request_payment": {"updated": True},
    }
    with mock_salt(s3_bucket_state, salt_map):
        ret = s3_bucket_state.present("foo", Bucket="b", CORSRules=[{"AllowedMethods": ["GET"]}])
    assert ret["result"] is False


def test_present_exists_noop(mock_salt):
    salt_map = {
        "boto3_s3_bucket.exists": {"exists": True},
        "boto3_s3_bucket.describe": BASIC_DESCRIBE,
        "boto3_s3_bucket.list": {"Owner": {"ID": "o"}},
        "boto3_s3_bucket.put_acl": {"updated": True},
    }
    with mock_salt(s3_bucket_state, salt_map):
        ret = s3_bucket_state.present("foo", Bucket="b")
    assert ret["result"] is True
    assert "is present" in ret["comment"]


def test_present_describe_error(mock_salt):
    salt_map = {
        "boto3_s3_bucket.exists": {"exists": True},
        "boto3_s3_bucket.describe": ERR,
    }
    with mock_salt(s3_bucket_state, salt_map):
        ret = s3_bucket_state.present("foo", Bucket="b")
    assert ret["result"] is False
    assert "Failed to update bucket" in ret["comment"]


def test_present_location_mismatch(mock_salt):
    described = {
        "bucket": {
            "ACL": {"Owner": {"ID": "o"}, "Grants": []},
            "Location": {"LocationConstraint": "us-east-1"},
            "Logging": {},
            "Versioning": {},
            "RequestPayment": {"Payer": "BucketOwner"},
            "NotificationConfiguration": {},
        }
    }
    salt_map = {
        "boto3_s3_bucket.exists": {"exists": True},
        "boto3_s3_bucket.describe": described,
        "boto3_s3_bucket.list": {"Owner": {"ID": "o"}},
        "boto3_s3_bucket.put_acl": {"updated": True},
    }
    with mock_salt(s3_bucket_state, salt_map):
        ret = s3_bucket_state.present("foo", Bucket="b", LocationConstraint="EU")
    assert ret["result"] is False
    assert "cannot be changed" in ret["comment"]


def test_present_update_test_mode(mock_salt):
    salt_map = {
        "boto3_s3_bucket.exists": {"exists": True},
        "boto3_s3_bucket.describe": BASIC_DESCRIBE,
        "boto3_s3_bucket.list": {"Owner": {"ID": "o"}},
    }
    with mock_salt(s3_bucket_state, salt_map, test=True):
        ret = s3_bucket_state.present("foo", Bucket="b", Tagging={"Env": "prod"})
    assert ret["result"] is None
    assert "set to be modified" in ret["comment"]


def test_present_update_apply_change(mock_salt):
    salt_map = {
        "boto3_s3_bucket.exists": {"exists": True},
        "boto3_s3_bucket.describe": BASIC_DESCRIBE,
        "boto3_s3_bucket.list": {"Owner": {"ID": "o"}},
        "boto3_s3_bucket.put_acl": {"updated": True},
        "boto3_s3_bucket.put_tagging": {"updated": True},
    }
    with mock_salt(s3_bucket_state, salt_map) as salt_mocks:
        ret = s3_bucket_state.present("foo", Bucket="b", Tagging={"Env": "prod"})
    assert ret["result"] is True
    salt_mocks["boto3_s3_bucket.put_tagging"].assert_called_once()


def test_present_update_setter_fails(mock_salt):
    salt_map = {
        "boto3_s3_bucket.exists": {"exists": True},
        "boto3_s3_bucket.describe": BASIC_DESCRIBE,
        "boto3_s3_bucket.list": {"Owner": {"ID": "o"}},
        "boto3_s3_bucket.put_acl": {"updated": True},
        "boto3_s3_bucket.put_tagging": {"updated": False, "error": {"message": "bad"}},
    }
    with mock_salt(s3_bucket_state, salt_map):
        ret = s3_bucket_state.present("foo", Bucket="b", Tagging={"Env": "prod"})
    assert ret["result"] is False


def test_absent_not_there(mock_salt):
    with mock_salt(s3_bucket_state, {"boto3_s3_bucket.exists": {"exists": False}}):
        ret = s3_bucket_state.absent("foo", Bucket="b")
    assert ret["result"] is True
    assert "does not exist" in ret["comment"]


def test_absent_exists_error(mock_salt):
    with mock_salt(s3_bucket_state, {"boto3_s3_bucket.exists": ERR}):
        ret = s3_bucket_state.absent("foo", Bucket="b")
    assert ret["result"] is False


def test_absent_test_mode(mock_salt):
    salt_map = {"boto3_s3_bucket.exists": {"exists": True}}
    with mock_salt(s3_bucket_state, salt_map, test=True):
        ret = s3_bucket_state.absent("foo", Bucket="b")
    assert ret["result"] is None
    assert "set to be removed" in ret["comment"]


def test_absent_delete(mock_salt):
    salt_map = {
        "boto3_s3_bucket.exists": {"exists": True},
        "boto3_s3_bucket.delete": {"deleted": True},
    }
    with mock_salt(s3_bucket_state, salt_map):
        ret = s3_bucket_state.absent("foo", Bucket="b")
    assert ret["result"] is True
    assert "deleted" in ret["comment"]


def test_absent_delete_error(mock_salt):
    salt_map = {
        "boto3_s3_bucket.exists": {"exists": True},
        "boto3_s3_bucket.delete": {"deleted": False, "error": {"message": "bad"}},
    }
    with mock_salt(s3_bucket_state, salt_map):
        ret = s3_bucket_state.absent("foo", Bucket="b")
    assert ret["result"] is False
