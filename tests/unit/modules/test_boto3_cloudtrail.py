"""
Unit tests for the ``boto3_cloudtrail`` execution module.

This file is the canonical template for execution-module unit tests in this
extension. New ``tests/unit/modules/test_boto3_*.py`` files should follow the
patterns demonstrated here:

- A module-level ``pytestmark`` skips on FIPS and when ``botocore`` is missing.
- ``configure_loader_modules`` initialises the loader dunders explicitly.
- A single ``conn`` fixture wraps the shared ``make_conn`` factory and yields a
  ``MagicMock`` bound to ``boto3mod.get_connection``.
- Per-behaviour fixtures (``trail_exists``, ``trail_missing`` ...) pre-bake the
  ``conn.<method>`` ``return_value`` or ``side_effect`` so individual tests stay
  short and one-behaviour-per-test.
- ``client_error`` (from the shared conftest) builds ``ClientError`` exceptions
  for error-branch coverage.
- Happy-path tests are named for what they verify (``test_create``); error and
  branch tests carry descriptive suffixes (``test_create_client_error``).
"""

from unittest.mock import patch

import pytest

from saltext.boto3.modules import boto3_cloudtrail

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
        boto3_cloudtrail: {
            "__opts__": {},
            "__context__": {},
            "__salt__": {},
        }
    }


@pytest.fixture
def conn(make_conn):
    with make_conn(boto3_cloudtrail) as client:
        yield client


@pytest.fixture
def trail_exists(conn):
    conn.get_trail_status.return_value = {"IsLogging": True}
    return conn


@pytest.fixture
def trail_missing(conn, client_error):
    conn.get_trail_status.side_effect = client_error("TrailNotFoundException", "GetTrailStatus")
    return conn


@pytest.fixture
def describe_returns_trail(conn):
    conn.describe_trails.return_value = {
        "trailList": [
            {
                "Name": "mytrail",
                "S3BucketName": "mybucket",
                "HomeRegion": "us-east-1",
                "TrailARN": "arn:aws:cloudtrail:us-east-1:111111111111:trail/mytrail",
            }
        ]
    }
    return conn


@pytest.fixture
def describe_empty(conn):
    conn.describe_trails.return_value = {"trailList": []}
    return conn


@pytest.fixture
def sts_account(conn):
    conn.get_caller_identity.return_value = {"Account": "111111111111"}
    return conn


def test_virtual():
    with patch.object(boto3_cloudtrail, "HAS_BOTO3", True):
        assert boto3_cloudtrail.__virtual__() == "boto3_cloudtrail"


def test_virtual_no_boto3():
    with patch.object(boto3_cloudtrail, "HAS_BOTO3", False):
        result = boto3_cloudtrail.__virtual__()
    assert result[0] is False
    assert "boto3" in result[1]


@pytest.mark.usefixtures("trail_exists")
def test_exists(conn):
    assert boto3_cloudtrail.exists("mytrail") == {"exists": True}
    conn.get_trail_status.assert_called_once_with(Name="mytrail")


@pytest.mark.usefixtures("trail_missing")
def test_exists_trail_not_found():
    assert boto3_cloudtrail.exists("mytrail") == {"exists": False}


def test_exists_client_error(conn, client_error):
    conn.get_trail_status.side_effect = client_error("AccessDenied", "GetTrailStatus")
    result = boto3_cloudtrail.exists("mytrail")
    assert "error" in result


def test_create(conn):
    conn.create_trail.return_value = {"Name": "mytrail"}
    result = boto3_cloudtrail.create("mytrail", "mybucket")
    conn.create_trail.assert_called_once_with(Name="mytrail", S3BucketName="mybucket")
    assert result == {"created": True, "name": "mytrail"}


def test_create_with_all_options(conn):
    conn.create_trail.return_value = {"Name": "mytrail"}
    boto3_cloudtrail.create(
        "mytrail",
        "mybucket",
        S3KeyPrefix="prefix",
        SnsTopicName="topic",
        IncludeGlobalServiceEvents=True,
        IsMultiRegionTrail=True,
        EnableLogFileValidation=True,
        CloudWatchLogsLogGroupArn="arn:aws:logs:::log-group:g",
        CloudWatchLogsRoleArn="arn:aws:iam::role/r",
        KmsKeyId="alias/aws/cloudtrail",
    )
    conn.create_trail.assert_called_once_with(
        Name="mytrail",
        S3BucketName="mybucket",
        S3KeyPrefix="prefix",
        SnsTopicName="topic",
        IncludeGlobalServiceEvents=True,
        IsMultiRegionTrail=True,
        EnableLogFileValidation=True,
        CloudWatchLogsLogGroupArn="arn:aws:logs:::log-group:g",
        CloudWatchLogsRoleArn="arn:aws:iam::role/r",
        KmsKeyId="alias/aws/cloudtrail",
    )


def test_create_returns_falsy_payload(conn):
    conn.create_trail.return_value = None
    assert boto3_cloudtrail.create("mytrail", "mybucket") == {"created": False}


def test_create_client_error(conn, client_error):
    conn.create_trail.side_effect = client_error("AccessDenied", "CreateTrail")
    result = boto3_cloudtrail.create("mytrail", "mybucket")
    assert result["created"] is False
    assert "error" in result


def test_delete(conn):
    assert boto3_cloudtrail.delete("mytrail") == {"deleted": True}
    conn.delete_trail.assert_called_once_with(Name="mytrail")


def test_delete_client_error(conn, client_error):
    conn.delete_trail.side_effect = client_error("AccessDenied", "DeleteTrail")
    result = boto3_cloudtrail.delete("mytrail")
    assert result["deleted"] is False
    assert "error" in result


@pytest.mark.usefixtures("describe_returns_trail")
def test_describe(conn):
    result = boto3_cloudtrail.describe("mytrail")
    assert result["trail"]["Name"] == "mytrail"
    assert result["trail"]["S3BucketName"] == "mybucket"
    conn.describe_trails.assert_called_once_with(trailNameList=["mytrail"])


@pytest.mark.usefixtures("describe_empty")
def test_describe_empty_trail_list():
    assert boto3_cloudtrail.describe("mytrail") == {"trail": None}


def test_describe_trail_not_found(conn, client_error):
    conn.describe_trails.side_effect = client_error("TrailNotFoundException", "DescribeTrails")
    assert boto3_cloudtrail.describe("mytrail") == {"trail": None}


def test_describe_client_error(conn, client_error):
    conn.describe_trails.side_effect = client_error("AccessDenied", "DescribeTrails")
    result = boto3_cloudtrail.describe("mytrail")
    assert "error" in result


def test_status(conn):
    conn.get_trail_status.return_value = {"IsLogging": True}
    result = boto3_cloudtrail.status("mytrail")
    assert result["trail"]["IsLogging"] is True
    conn.get_trail_status.assert_called_once_with(Name="mytrail")


@pytest.mark.usefixtures("trail_missing")
def test_status_trail_not_found():
    assert boto3_cloudtrail.status("mytrail") == {"trail": None}


def test_status_client_error(conn, client_error):
    conn.get_trail_status.side_effect = client_error("AccessDenied", "GetTrailStatus")
    result = boto3_cloudtrail.status("mytrail")
    assert "error" in result


def test_list_trails(conn):
    conn.describe_trails.return_value = {"trailList": [{"Name": "mytrail"}]}
    assert boto3_cloudtrail.list_trails() == {"trails": [{"Name": "mytrail"}]}


def test_list_trails_empty(conn):
    conn.describe_trails.return_value = {}
    assert boto3_cloudtrail.list_trails() == {"trails": []}


def test_list_trails_client_error(conn, client_error):
    conn.describe_trails.side_effect = client_error("AccessDenied", "DescribeTrails")
    result = boto3_cloudtrail.list_trails()
    assert "error" in result


def test_update(conn):
    conn.update_trail.return_value = {"Name": "mytrail"}
    result = boto3_cloudtrail.update("mytrail", "mybucket")
    conn.update_trail.assert_called_once_with(Name="mytrail", S3BucketName="mybucket")
    assert result == {"updated": True, "name": "mytrail"}


def test_update_with_all_options(conn):
    conn.update_trail.return_value = {"Name": "mytrail"}
    boto3_cloudtrail.update(
        "mytrail",
        "mybucket",
        S3KeyPrefix="prefix",
        IsMultiRegionTrail=False,
        KmsKeyId="alias/aws/cloudtrail",
    )
    conn.update_trail.assert_called_once_with(
        Name="mytrail",
        S3BucketName="mybucket",
        S3KeyPrefix="prefix",
        IsMultiRegionTrail=False,
        KmsKeyId="alias/aws/cloudtrail",
    )


def test_update_returns_falsy_payload(conn):
    conn.update_trail.return_value = None
    assert boto3_cloudtrail.update("mytrail", "mybucket") == {"updated": False}


def test_update_client_error(conn, client_error):
    conn.update_trail.side_effect = client_error("AccessDenied", "UpdateTrail")
    result = boto3_cloudtrail.update("mytrail", "mybucket")
    assert result["updated"] is False
    assert "error" in result


def test_start_logging(conn):
    assert boto3_cloudtrail.start_logging("mytrail") == {"started": True}
    conn.start_logging.assert_called_once_with(Name="mytrail")


def test_start_logging_client_error(conn, client_error):
    conn.start_logging.side_effect = client_error("AccessDenied", "StartLogging")
    result = boto3_cloudtrail.start_logging("mytrail")
    assert result["started"] is False
    assert "error" in result


def test_stop_logging(conn):
    assert boto3_cloudtrail.stop_logging("mytrail") == {"stopped": True}
    conn.stop_logging.assert_called_once_with(Name="mytrail")


def test_stop_logging_client_error(conn, client_error):
    conn.stop_logging.side_effect = client_error("AccessDenied", "StopLogging")
    result = boto3_cloudtrail.stop_logging("mytrail")
    assert result["stopped"] is False
    assert "error" in result


def test_get_trail_arn_passthrough_when_already_arn():
    arn = "arn:aws:cloudtrail:us-west-2:111111111111:trail/mytrail"
    assert boto3_cloudtrail._get_trail_arn(arn) == arn


@pytest.mark.usefixtures("sts_account")
def test_get_trail_arn_builds_from_sts():
    result = boto3_cloudtrail._get_trail_arn("mytrail", region="us-west-2")
    assert result == "arn:aws:cloudtrail:us-west-2:111111111111:trail/mytrail"


@pytest.mark.usefixtures("sts_account")
def test_get_trail_arn_defaults_to_us_east_1_when_no_region():
    result = boto3_cloudtrail._get_trail_arn("mytrail")
    assert result == "arn:aws:cloudtrail:us-east-1:111111111111:trail/mytrail"


@pytest.mark.usefixtures("sts_account")
def test_get_trail_arn_uses_profile_region():
    result = boto3_cloudtrail._get_trail_arn("mytrail", profile={"region": "eu-west-1"})
    assert result == "arn:aws:cloudtrail:eu-west-1:111111111111:trail/mytrail"


@pytest.mark.usefixtures("sts_account")
def test_add_tags(conn):
    result = boto3_cloudtrail.add_tags("mytrail", region="us-east-1", a="1", b="2")
    assert result == {"tagged": True}
    conn.add_tags.assert_called_once_with(
        ResourceId="arn:aws:cloudtrail:us-east-1:111111111111:trail/mytrail",
        TagsList=[{"Key": "a", "Value": "1"}, {"Key": "b", "Value": "2"}],
    )


@pytest.mark.usefixtures("sts_account")
def test_add_tags_skips_dunder_kwargs(conn):
    boto3_cloudtrail.add_tags("mytrail", region="us-east-1", a="1", __pub_user="root")
    _, kwargs = conn.add_tags.call_args
    assert kwargs["TagsList"] == [{"Key": "a", "Value": "1"}]


def test_add_tags_client_error(conn, client_error):
    arn = "arn:aws:cloudtrail:us-east-1:111111111111:trail/mytrail"
    conn.add_tags.side_effect = client_error("AccessDenied", "AddTags")
    result = boto3_cloudtrail.add_tags(arn, a="1")
    assert result["tagged"] is False
    assert "error" in result


@pytest.mark.usefixtures("sts_account")
def test_remove_tags(conn):
    result = boto3_cloudtrail.remove_tags("mytrail", region="us-east-1", a="1")
    assert result == {"tagged": True}
    conn.remove_tags.assert_called_once_with(
        ResourceId="arn:aws:cloudtrail:us-east-1:111111111111:trail/mytrail",
        TagsList=[{"Key": "a", "Value": "1"}],
    )


def test_remove_tags_client_error(conn, client_error):
    arn = "arn:aws:cloudtrail:us-east-1:111111111111:trail/mytrail"
    conn.remove_tags.side_effect = client_error("AccessDenied", "RemoveTags")
    result = boto3_cloudtrail.remove_tags(arn, a="1")
    assert result["tagged"] is False
    assert "error" in result


def test_list_tags(conn):
    arn = "arn:aws:cloudtrail:us-east-1:111111111111:trail/mytrail"
    conn.list_tags.return_value = {
        "ResourceTagList": [{"ResourceId": arn, "TagsList": [{"Key": "a", "Value": "1"}]}]
    }
    result = boto3_cloudtrail.list_tags(arn)
    assert result == {"tags": {"a": "1"}}
    conn.list_tags.assert_called_once_with(ResourceIdList=[arn])


def test_list_tags_client_error(conn, client_error):
    arn = "arn:aws:cloudtrail:us-east-1:111111111111:trail/mytrail"
    conn.list_tags.side_effect = client_error("AccessDenied", "ListTags")
    result = boto3_cloudtrail.list_tags(arn)
    assert "error" in result


def test_list_tags_propagates_sts_client_error(conn, client_error):
    """STS failures bubble up through ``_get_trail_arn`` and surface as errors."""
    conn.get_caller_identity.side_effect = client_error("AccessDenied", "GetCallerIdentity")
    result = boto3_cloudtrail.list_tags("mytrail")
    assert "error" in result
