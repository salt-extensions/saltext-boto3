"""
Unit tests for the ``boto3_elbv2`` execution module.
"""

import pytest

from saltext.boto3.modules import boto3_elbv2

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
        boto3_elbv2: {
            "__opts__": {},
            "__context__": {},
            "__salt__": {},
        }
    }


@pytest.fixture
def conn(make_conn):
    with make_conn(boto3_elbv2) as client:
        yield client


def test_create_target_group_when_already_exists(conn):
    conn.describe_target_groups.return_value = {"TargetGroups": [{"TargetGroupArn": "arn"}]}
    assert boto3_elbv2.create_target_group("tg", "HTTP", 80, "vpc-1") is True
    conn.create_target_group.assert_not_called()


def test_create_target_group_creates_when_missing(conn, client_error):
    conn.describe_target_groups.side_effect = client_error("TargetGroupNotFound", "Describe")
    conn.create_target_group.return_value = {"TargetGroups": [{"TargetGroupArn": "arn:tg/1"}]}
    assert boto3_elbv2.create_target_group("tg", "HTTP", 80, "vpc-1") is True
    conn.create_target_group.assert_called_once()


def test_create_target_group_handles_client_error(conn, client_error):
    conn.describe_target_groups.side_effect = client_error("TargetGroupNotFound", "Describe")
    conn.create_target_group.side_effect = client_error("Boom", "Create")
    assert boto3_elbv2.create_target_group("tg", "HTTP", 80, "vpc-1") is None


def test_delete_target_group_missing_returns_true(conn, client_error):
    conn.describe_target_groups.side_effect = client_error("NotFound", "Describe")
    assert boto3_elbv2.delete_target_group("tg") is True


def test_delete_target_group_by_name(conn):
    conn.describe_target_groups.side_effect = [
        {"TargetGroups": [{"TargetGroupArn": "arn:tg/1"}]},
        {"TargetGroups": [{"TargetGroupArn": "arn:tg/1"}]},
    ]
    assert boto3_elbv2.delete_target_group("tg") is True
    conn.delete_target_group.assert_called_once_with(TargetGroupArn="arn:tg/1")


def test_delete_target_group_by_arn(conn):
    arn = "arn:aws:elasticloadbalancing:us-west-2:1:targetgroup/tg/abc"
    conn.describe_target_groups.return_value = {"TargetGroups": [{"TargetGroupArn": arn}]}
    assert boto3_elbv2.delete_target_group(arn) is True
    conn.delete_target_group.assert_called_once_with(TargetGroupArn=arn)


def test_delete_target_group_client_error(conn, client_error):
    conn.describe_target_groups.return_value = {"TargetGroups": [{"TargetGroupArn": "arn"}]}
    conn.delete_target_group.side_effect = client_error("Boom", "Delete")
    assert boto3_elbv2.delete_target_group("tg") is False


def test_target_group_exists_true(conn):
    conn.describe_target_groups.return_value = {"TargetGroups": [{}]}
    assert boto3_elbv2.target_group_exists("tg") is True


def test_target_group_exists_false_on_error(conn, client_error):
    conn.describe_target_groups.side_effect = client_error("NotFound", "Describe")
    assert boto3_elbv2.target_group_exists("tg") is False


def test_describe_target_health_with_targets(conn):
    conn.describe_target_health.return_value = {
        "TargetHealthDescriptions": [
            {"Target": {"Id": "i-1"}, "TargetHealth": {"State": "healthy"}},
            {"Target": {"Id": "i-2"}, "TargetHealth": {"State": "unhealthy"}},
        ]
    }
    result = boto3_elbv2.describe_target_health("arn", targets=["i-1", "i-2"])
    assert result == {"i-1": "healthy", "i-2": "unhealthy"}


def test_describe_target_health_no_targets(conn):
    conn.describe_target_health.return_value = {"TargetHealthDescriptions": []}
    assert not boto3_elbv2.describe_target_health("arn")


def test_describe_target_health_client_error(conn, client_error):
    conn.describe_target_health.side_effect = client_error("Boom", "Describe")
    assert not boto3_elbv2.describe_target_health("arn")


def test_register_targets_with_string(conn):
    conn.register_targets.return_value = {"ok": True}
    assert boto3_elbv2.register_targets("arn", "i-1") is True
    conn.register_targets.assert_called_once_with(TargetGroupArn="arn", Targets=[{"Id": "i-1"}])


def test_register_targets_with_list(conn):
    conn.register_targets.return_value = {"ok": True}
    assert boto3_elbv2.register_targets("arn", ["i-1", "i-2"]) is True
    kwargs = conn.register_targets.call_args.kwargs
    assert kwargs["Targets"] == [{"Id": "i-1"}, {"Id": "i-2"}]


def test_register_targets_client_error(conn, client_error):
    conn.register_targets.side_effect = client_error("Boom", "Register")
    assert boto3_elbv2.register_targets("arn", "i-1") is False


def test_deregister_targets(conn):
    conn.deregister_targets.return_value = {"ok": True}
    assert boto3_elbv2.deregister_targets("arn", ["i-1"]) is True


def test_deregister_targets_client_error(conn, client_error):
    conn.deregister_targets.side_effect = client_error("Boom", "Deregister")
    assert boto3_elbv2.deregister_targets("arn", "i-1") is False
