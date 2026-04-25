"""
Unit tests for the ``boto3_s3`` execution module.
"""

import pytest

from saltext.boto3.modules import boto3_s3

try:
    import boto3
    import botocore.exceptions  # pylint: disable=unused-import

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
        boto3_s3: {
            "__opts__": {},
            "__context__": {},
            "__salt__": {},
        }
    }


@pytest.fixture
def conn(make_conn):
    with make_conn(boto3_s3) as client:
        yield client


def test_get_object_metadata(conn):
    conn.head_object.return_value = {"ContentLength": 123, "ETag": "abc"}
    ret = boto3_s3.get_object_metadata("bucket/path/to/obj")
    assert ret == {"result": {"ContentLength": 123, "ETag": "abc"}}
    conn.head_object.assert_called_once_with(Bucket="bucket", Key="path/to/obj")


def test_get_object_metadata_extra_args(conn):
    conn.head_object.return_value = {}
    boto3_s3.get_object_metadata("bucket/key", extra_args={"RequestPayer": "requester"})
    conn.head_object.assert_called_once_with(Bucket="bucket", Key="key", RequestPayer="requester")


def test_get_object_metadata_missing(conn, client_error):
    conn.head_object.side_effect = client_error("404", "HeadObject", message="Not Found")
    assert boto3_s3.get_object_metadata("bucket/key") == {"result": None}


def test_get_object_metadata_error(conn, client_error):
    conn.head_object.side_effect = client_error("AccessDenied", "HeadObject")
    ret = boto3_s3.get_object_metadata("bucket/key")
    assert "error" in ret


def test_upload_file(conn):
    assert boto3_s3.upload_file("/tmp/src", "bucket/key") == {"result": True}
    conn.upload_file.assert_called_once_with("/tmp/src", "bucket", "key", ExtraArgs=None)


def test_upload_file_with_extra_args(conn):
    boto3_s3.upload_file("/tmp/src", "bucket/key", extra_args={"ContentType": "text/plain"})
    conn.upload_file.assert_called_once_with(
        "/tmp/src", "bucket", "key", ExtraArgs={"ContentType": "text/plain"}
    )


def test_upload_file_error(conn):
    conn.upload_file.side_effect = boto3.exceptions.S3UploadFailedError("oops")
    ret = boto3_s3.upload_file("/tmp/src", "bucket/key")
    assert "error" in ret
