"""
Unit tests for saltext.boto3.modules.boto3_s3_bucket.
"""

from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from salt.exceptions import SaltInvocationError

from saltext.boto3.modules import boto3_s3_bucket

try:
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
        boto3_s3_bucket: {
            "__opts__": {},
            "__context__": {},
            "__salt__": {},
        }
    }


@pytest.fixture
def conn(make_conn):
    with make_conn(boto3_s3_bucket) as client:
        yield client


def test_exists_true(conn):
    conn.head_bucket.return_value = {}
    assert boto3_s3_bucket.exists("b") == {"exists": True}


def test_exists_404(conn, client_error):
    conn.head_bucket.side_effect = client_error("404", "S3Op")
    assert boto3_s3_bucket.exists("b") == {"exists": False}


def test_exists_error(conn, client_error):
    conn.head_bucket.side_effect = client_error("AccessDenied", "S3Op")
    ret = boto3_s3_bucket.exists("b")
    assert "error" in ret


def test_create(conn):
    conn.create_bucket.return_value = {"Location": "/mybucket"}
    ret = boto3_s3_bucket.create("mybucket")
    assert ret["created"] is True
    assert ret["name"] == "mybucket"


def test_create_with_location(conn):
    conn.create_bucket.return_value = {"Location": "/mybucket"}
    boto3_s3_bucket.create("mybucket", LocationConstraint="EU")
    kw = conn.create_bucket.call_args.kwargs
    assert kw["CreateBucketConfiguration"] == {"LocationConstraint": "EU"}


def test_create_no_location(conn):
    conn.create_bucket.return_value = None
    ret = boto3_s3_bucket.create("mybucket")
    assert ret == {"created": False}


def test_create_error(conn, client_error):
    conn.create_bucket.side_effect = client_error("AlreadyExists", "S3Op")
    ret = boto3_s3_bucket.create("mybucket")
    assert ret["created"] is False
    assert "error" in ret


def test_delete(conn):
    assert boto3_s3_bucket.delete("b") == {"deleted": True}
    conn.delete_bucket.assert_called_once_with(Bucket="b")


def test_delete_error(conn, client_error):
    conn.delete_bucket.side_effect = client_error("BucketNotEmpty", "S3Op")
    ret = boto3_s3_bucket.delete("b")
    assert ret["deleted"] is False


def test_delete_force(conn):
    conn.list_object_versions.return_value = {
        "Versions": [{"Key": "a", "VersionId": "v1"}],
        "DeleteMarkers": [],
    }
    conn.delete_objects.return_value = {"Errors": []}
    ret = boto3_s3_bucket.delete("b", Force=True)
    assert ret == {"deleted": True}
    conn.delete_objects.assert_called_once()


def test_delete_objects(conn):
    conn.delete_objects.return_value = {"Errors": []}
    ret = boto3_s3_bucket.delete_objects("b", {"Objects": [{"Key": "a"}]})
    assert ret == {"deleted": True}


def test_delete_objects_json_str(conn):
    conn.delete_objects.return_value = {"Errors": []}
    ret = boto3_s3_bucket.delete_objects("b", '{"Objects": [{"Key": "a"}]}')
    assert ret == {"deleted": True}


def test_delete_objects_with_failures(conn):
    conn.delete_objects.return_value = {"Errors": [{"Key": "bad", "Code": "X"}]}
    ret = boto3_s3_bucket.delete_objects("b", {"Objects": [{"Key": "bad"}]})
    assert ret["deleted"] is False
    assert ret["failed"]


def test_delete_objects_bad_input():
    # Non-dict (after JSON parse) and dict-without-Objects both raise.
    with pytest.raises(SaltInvocationError):
        boto3_s3_bucket.delete_objects("b", "[1, 2]")
    with pytest.raises(SaltInvocationError):
        boto3_s3_bucket.delete_objects("b", {"nokey": []})


def test_delete_objects_error(conn, client_error):
    conn.delete_objects.side_effect = client_error("Oops", "S3Op")
    ret = boto3_s3_bucket.delete_objects("b", {"Objects": [{"Key": "a"}]})
    assert ret["deleted"] is False
    assert "error" in ret


def test_describe(conn):
    conn.get_bucket_acl.return_value = {"Owner": {"ID": "x"}, "Grants": []}
    for name in (
        "get_bucket_cors",
        "get_bucket_lifecycle_configuration",
        "get_bucket_location",
        "get_bucket_logging",
        "get_bucket_notification_configuration",
        "get_bucket_policy",
        "get_bucket_replication",
        "get_bucket_request_payment",
        "get_bucket_versioning",
        "get_bucket_website",
    ):
        getattr(conn, name).return_value = {}
    conn.get_bucket_tagging.return_value = {"TagSet": [{"Key": "k", "Value": "v"}]}
    ret = boto3_s3_bucket.describe("b")
    assert "bucket" in ret
    assert ret["bucket"]["Tagging"] == {"k": "v"}


def test_describe_missing_bucket(conn, client_error):
    conn.get_bucket_acl.side_effect = client_error("NoSuchBucket", "S3Op")
    ret = boto3_s3_bucket.describe("b")
    assert ret == {"bucket": None}


def test_describe_skips_missing_subresources(conn, client_error):
    conn.get_bucket_acl.return_value = {"Owner": {"ID": "x"}, "Grants": []}
    conn.get_bucket_cors.side_effect = client_error("NoSuchCORSConfiguration", "S3Op")
    conn.get_bucket_lifecycle_configuration.return_value = {}
    conn.get_bucket_location.return_value = {}
    conn.get_bucket_logging.return_value = {}
    conn.get_bucket_notification_configuration.return_value = {}
    conn.get_bucket_policy.side_effect = client_error("NoSuchBucketPolicy", "S3Op")
    conn.get_bucket_replication.side_effect = client_error(
        "ReplicationConfigurationNotFoundError", "S3Op"
    )
    conn.get_bucket_request_payment.return_value = {}
    conn.get_bucket_versioning.return_value = {}
    conn.get_bucket_website.side_effect = client_error("NoSuchWebsiteConfiguration", "S3Op")
    conn.get_bucket_tagging.side_effect = client_error("NoSuchTagSet", "S3Op")
    ret = boto3_s3_bucket.describe("b")
    assert "CORS" not in ret["bucket"]
    assert "Policy" not in ret["bucket"]


def test_list(conn):
    conn.list_buckets.return_value = {
        "Buckets": [{"Name": "a"}],
        "Owner": {"ID": "x"},
        "ResponseMetadata": {"drop": "me"},
    }
    ret = boto3_s3_bucket.list()
    assert "ResponseMetadata" not in ret
    assert ret["Buckets"] == [{"Name": "a"}]


def test_list_error(conn, client_error):
    conn.list_buckets.side_effect = client_error("Oops", "S3Op")
    ret = boto3_s3_bucket.list()
    assert "error" in ret


def test_empty(conn):
    conn.list_object_versions.return_value = {
        "Versions": [{"Key": "a", "VersionId": "v1"}],
        "DeleteMarkers": [{"Key": "d", "VersionId": "v2"}],
    }
    conn.delete_objects.return_value = {"Errors": []}
    ret = boto3_s3_bucket.empty("b")
    assert ret == {"deleted": True}


def test_empty_already_empty(conn):
    conn.list_object_versions.return_value = {"Versions": [], "DeleteMarkers": []}
    ret = boto3_s3_bucket.empty("b")
    assert ret == {"deleted": True}


def test_list_object_versions(conn):
    conn.list_object_versions.side_effect = [
        {
            "Versions": [{"Key": "a"}],
            "DeleteMarkers": [],
            "IsTruncated": True,
            "NextKeyMarker": "k",
            "NextVersionIdMarker": "v",
        },
        {"Versions": [{"Key": "b"}], "DeleteMarkers": [], "IsTruncated": False},
    ]
    ret = boto3_s3_bucket.list_object_versions("b", Prefix="p")
    assert len(ret["Versions"]) == 2


def test_list_object_versions_error(conn, client_error):
    conn.list_object_versions.side_effect = client_error("Oops", "S3Op")
    ret = boto3_s3_bucket.list_object_versions("b")
    assert "error" in ret


def test_list_objects(conn):
    conn.list_objects_v2.side_effect = [
        {"Contents": [{"Key": "a"}], "IsTruncated": True, "NextContinuationToken": "t"},
        {"Contents": [{"Key": "b"}], "IsTruncated": False},
    ]
    ret = boto3_s3_bucket.list_objects("b")
    assert len(ret["Contents"]) == 2


def test_list_objects_error(conn, client_error):
    conn.list_objects_v2.side_effect = client_error("Oops", "S3Op")
    ret = boto3_s3_bucket.list_objects("b")
    assert "error" in ret


def test_put_acl(conn):
    ret = boto3_s3_bucket.put_acl("b", ACL="public-read")
    assert ret == {"updated": True, "name": "b"}
    conn.put_bucket_acl.assert_called_once()


def test_put_acl_with_policy_str(conn):
    boto3_s3_bucket.put_acl("b", AccessControlPolicy='{"Owner": {}}')
    kw = conn.put_bucket_acl.call_args.kwargs
    assert kw["AccessControlPolicy"] == {"Owner": {}}


def test_put_acl_error(conn, client_error):
    conn.put_bucket_acl.side_effect = client_error("Oops", "S3Op")
    ret = boto3_s3_bucket.put_acl("b", ACL="private")
    assert ret["updated"] is False


def test_put_cors(conn):
    ret = boto3_s3_bucket.put_cors("b", [{"AllowedMethods": ["GET"]}])
    assert ret == {"updated": True, "name": "b"}


def test_put_cors_json_str(conn):
    ret = boto3_s3_bucket.put_cors("b", '[{"AllowedMethods": ["GET"]}]')
    assert ret["updated"] is True


def test_put_cors_error(conn, client_error):
    conn.put_bucket_cors.side_effect = client_error("Oops", "S3Op")
    ret = boto3_s3_bucket.put_cors("b", [])
    assert ret["updated"] is False


def test_put_lifecycle_configuration(conn):
    ret = boto3_s3_bucket.put_lifecycle_configuration("b", [{"ID": "a"}])
    assert ret["updated"] is True


def test_put_lifecycle_configuration_error(conn, client_error):
    conn.put_bucket_lifecycle_configuration.side_effect = client_error("Oops", "S3Op")
    ret = boto3_s3_bucket.put_lifecycle_configuration("b", [])
    assert ret["updated"] is False


def test_put_logging(conn):
    ret = boto3_s3_bucket.put_logging("b", TargetBucket="log", TargetPrefix="p/", TargetGrants=[])
    assert ret["updated"] is True


def test_put_logging_disabled(conn):
    ret = boto3_s3_bucket.put_logging("b")
    assert ret["updated"] is True


def test_put_logging_error(conn, client_error):
    conn.put_bucket_logging.side_effect = client_error("Oops", "S3Op")
    ret = boto3_s3_bucket.put_logging("b", TargetBucket="log", TargetPrefix="p/")
    assert ret["updated"] is False


def test_put_notification_configuration(conn):
    ret = boto3_s3_bucket.put_notification_configuration("b", TopicConfigurations=[{"Id": "x"}])
    assert ret["updated"] is True


def test_put_notification_configuration_error(conn, client_error):
    conn.put_bucket_notification_configuration.side_effect = client_error("Oops", "S3Op")
    ret = boto3_s3_bucket.put_notification_configuration("b")
    assert ret["updated"] is False


def test_put_policy_dict(conn):
    ret = boto3_s3_bucket.put_policy("b", {"Version": "2012-10-17"})
    assert ret["updated"] is True


def test_put_policy_str(conn):
    ret = boto3_s3_bucket.put_policy("b", '{"Version": "2012-10-17"}')
    assert ret["updated"] is True


def test_put_policy_error(conn, client_error):
    conn.put_bucket_policy.side_effect = client_error("Oops", "S3Op")
    ret = boto3_s3_bucket.put_policy("b", "{}")
    assert ret["updated"] is False


def test_put_replication(conn):
    ret = boto3_s3_bucket.put_replication("b", Role="arn:aws:iam::1:role/x", Rules=[])
    assert ret["updated"] is True


def test_put_replication_error(conn, client_error):
    conn.put_bucket_replication.side_effect = client_error("Oops", "S3Op")
    ret = boto3_s3_bucket.put_replication("b", Role="arn:aws:iam::1:role/x", Rules=[])
    assert ret["updated"] is False


def test_put_replication_resolves_role_name(conn):
    with patch.dict(
        boto3_s3_bucket.__salt__,
        {"boto3_iam.get_account_id": MagicMock(return_value="123")},
    ):
        ret = boto3_s3_bucket.put_replication("b", Role="my-role", Rules=[])
    assert ret["updated"] is True
    kw = conn.put_bucket_replication.call_args.kwargs
    assert kw["ReplicationConfiguration"]["Role"].startswith("arn:aws:iam::123:role/")


def test_put_request_payment(conn):
    ret = boto3_s3_bucket.put_request_payment("b", Payer="Requester")
    assert ret["updated"] is True


def test_put_request_payment_error(conn, client_error):
    conn.put_bucket_request_payment.side_effect = client_error("Oops", "S3Op")
    ret = boto3_s3_bucket.put_request_payment("b", Payer="BucketOwner")
    assert ret["updated"] is False


def test_put_tagging(conn):
    ret = boto3_s3_bucket.put_tagging("b", Env="prod", Team="a")
    assert ret["updated"] is True
    kw = conn.put_bucket_tagging.call_args.kwargs
    tags = kw["Tagging"]["TagSet"]
    assert {"Key": "Env", "Value": "prod"} in tags


def test_put_tagging_error(conn, client_error):
    conn.put_bucket_tagging.side_effect = client_error("Oops", "S3Op")
    ret = boto3_s3_bucket.put_tagging("b", Env="prod")
    assert ret["updated"] is False


def test_put_versioning(conn):
    ret = boto3_s3_bucket.put_versioning("b", Status="Enabled")
    assert ret["updated"] is True


def test_put_versioning_with_mfa(conn):
    boto3_s3_bucket.put_versioning("b", Status="Enabled", MFADelete="Enabled", MFA="x")
    conn.put_bucket_versioning.assert_called_once()


def test_put_versioning_error(conn, client_error):
    conn.put_bucket_versioning.side_effect = client_error("Oops", "S3Op")
    ret = boto3_s3_bucket.put_versioning("b", Status="Enabled")
    assert ret["updated"] is False


def test_put_website(conn):
    ret = boto3_s3_bucket.put_website("b", IndexDocument={"Suffix": "index.html"})
    assert ret["updated"] is True


def test_put_website_error(conn, client_error):
    conn.put_bucket_website.side_effect = client_error("Oops", "S3Op")
    ret = boto3_s3_bucket.put_website("b")
    assert ret["updated"] is False


@pytest.mark.parametrize(
    "fn,client_method",
    [
        ("delete_cors", "delete_bucket_cors"),
        ("delete_lifecycle_configuration", "delete_bucket_lifecycle"),
        ("delete_policy", "delete_bucket_policy"),
        ("delete_replication", "delete_bucket_replication"),
        ("delete_tagging", "delete_bucket_tagging"),
        ("delete_website", "delete_bucket_website"),
    ],
)
def test_delete_subresource(conn, fn, client_method):
    ret = getattr(boto3_s3_bucket, fn)("b")
    assert ret["deleted"] is True
    getattr(conn, client_method).assert_called_once_with(Bucket="b")


@pytest.mark.parametrize(
    "fn,client_method",
    [
        ("delete_cors", "delete_bucket_cors"),
        ("delete_lifecycle_configuration", "delete_bucket_lifecycle"),
        ("delete_policy", "delete_bucket_policy"),
        ("delete_replication", "delete_bucket_replication"),
        ("delete_tagging", "delete_bucket_tagging"),
        ("delete_website", "delete_bucket_website"),
    ],
)
def test_delete_subresource_error(conn, client_error, fn, client_method):
    getattr(conn, client_method).side_effect = client_error("Oops", "S3Op")
    ret = getattr(boto3_s3_bucket, fn)("b")
    assert ret["deleted"] is False
