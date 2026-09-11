"""
Unit tests for the ``boto3_s3`` execution module.
"""

from unittest.mock import MagicMock

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


def test_get_object(conn):
    body = MagicMock()
    body.read.return_value = b"payload"
    conn.get_object.return_value = {"Body": body}
    ret = boto3_s3.get_object("bucket/key")
    assert ret == {"result": b"payload"}
    conn.get_object.assert_called_once_with(Bucket="bucket", Key="key")


def test_get_object_with_extra_args(conn):
    body = MagicMock()
    body.read.return_value = b"payload"
    conn.get_object.return_value = {"Body": body}
    boto3_s3.get_object("bucket/key", extra_args={"RequestPayer": "requester"})
    conn.get_object.assert_called_once_with(Bucket="bucket", Key="key", RequestPayer="requester")


def test_get_object_invalid_name(conn):
    ret = boto3_s3.get_object("bucket-only")
    assert ret == {"error": "name must be in bucket/key format"}
    conn.get_object.assert_not_called()


def test_get_object_error(conn, client_error):
    conn.get_object.side_effect = client_error("AccessDenied", "GetObject")
    ret = boto3_s3.get_object("bucket/key")
    assert "error" in ret


def test_put_object(conn):
    ret = boto3_s3.put_object("bucket/key", "payload")
    assert ret == {"result": True}
    conn.put_object.assert_called_once_with(Bucket="bucket", Key="key", Body="payload")


def test_put_object_with_extra_args(conn):
    ret = boto3_s3.put_object("bucket/key", b"payload", extra_args={"ContentType": "text/plain"})
    assert ret == {"result": True}
    conn.put_object.assert_called_once_with(
        Bucket="bucket", Key="key", Body=b"payload", ContentType="text/plain"
    )


def test_put_object_invalid_name(conn):
    ret = boto3_s3.put_object("bucket-only", "payload")
    assert ret == {"error": "name must be in bucket/key format"}
    conn.put_object.assert_not_called()


def test_put_object_error(conn, client_error):
    conn.put_object.side_effect = client_error("AccessDenied", "PutObject")
    ret = boto3_s3.put_object("bucket/key", "payload")
    assert "error" in ret


def test_delete_object(conn):
    ret = boto3_s3.delete_object("bucket/key")
    assert ret == {"result": True}
    conn.delete_object.assert_called_once_with(Bucket="bucket", Key="key")


def test_delete_object_with_extra_args(conn):
    ret = boto3_s3.delete_object("bucket/key", extra_args={"VersionId": "v1"})
    assert ret == {"result": True}
    conn.delete_object.assert_called_once_with(Bucket="bucket", Key="key", VersionId="v1")


def test_delete_object_invalid_name(conn):
    ret = boto3_s3.delete_object("bucket-only")
    assert ret == {"error": "name must be in bucket/key format"}
    conn.delete_object.assert_not_called()


def test_delete_object_error(conn, client_error):
    conn.delete_object.side_effect = client_error("AccessDenied", "DeleteObject")
    ret = boto3_s3.delete_object("bucket/key")
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


def test_download_file(conn):
    assert boto3_s3.download_file("bucket/key", "/tmp/dst") == {"result": True}
    conn.download_file.assert_called_once_with("bucket", "key", "/tmp/dst", ExtraArgs=None)


def test_download_file_with_extra_args(conn):
    boto3_s3.download_file("bucket/key", "/tmp/dst", extra_args={"RequestPayer": "requester"})
    conn.download_file.assert_called_once_with(
        "bucket", "key", "/tmp/dst", ExtraArgs={"RequestPayer": "requester"}
    )


def test_download_file_invalid_name(conn):
    ret = boto3_s3.download_file("bucket-only", "/tmp/dst")
    assert ret == {"error": "name must be in bucket/key format"}
    conn.download_file.assert_not_called()


def test_download_file_error(conn, client_error):
    conn.download_file.side_effect = client_error("AccessDenied", "GetObject")
    ret = boto3_s3.download_file("bucket/key", "/tmp/dst")
    assert "error" in ret


def test_generate_presigned_url(conn):
    conn.generate_presigned_url.return_value = "https://example.test/presigned"
    ret = boto3_s3.generate_presigned_url("bucket/key", expiration=300)
    assert ret == {"result": "https://example.test/presigned"}
    conn.generate_presigned_url.assert_called_once_with(
        "get_object", Params={"Bucket": "bucket", "Key": "key"}, ExpiresIn=300
    )


def test_generate_presigned_url_with_extra_args(conn):
    conn.generate_presigned_url.return_value = "https://example.test/presigned"
    ret = boto3_s3.generate_presigned_url(
        "bucket/key", extra_args={"ResponseContentType": "text/plain"}
    )
    assert ret == {"result": "https://example.test/presigned"}
    conn.generate_presigned_url.assert_called_once_with(
        "get_object",
        Params={"Bucket": "bucket", "Key": "key", "ResponseContentType": "text/plain"},
        ExpiresIn=900,
    )


def test_generate_presigned_url_invalid_name(conn):
    ret = boto3_s3.generate_presigned_url("bucket-only")
    assert ret == {"error": "name must be in bucket/key format"}
    conn.generate_presigned_url.assert_not_called()


def test_generate_presigned_url_error(conn, client_error):
    conn.generate_presigned_url.side_effect = client_error("AccessDenied", "GetObject")
    ret = boto3_s3.generate_presigned_url("bucket/key")
    assert "error" in ret
