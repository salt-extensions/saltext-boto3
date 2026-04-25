"""
Unit tests for the ``boto3_cloudwatch`` execution module.
"""

from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from saltext.boto3.modules import boto3_cloudwatch

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
        boto3_cloudwatch: {
            "__opts__": {},
            "__context__": {},
            "__salt__": {},
        }
    }


@pytest.fixture
def conn(make_conn):
    with make_conn(boto3_cloudwatch) as client:
        yield client


def test_get_alarm_found(conn):
    conn.describe_alarms.return_value = {
        "MetricAlarms": [
            {
                "AlarmName": "a",
                "MetricName": "m",
                "Namespace": "AWS/SQS",
                "Threshold": 1.0,
            }
        ]
    }
    result = boto3_cloudwatch.get_alarm("a")
    assert result["AlarmName"] == "a"
    assert result["MetricName"] == "m"
    conn.describe_alarms.assert_called_once_with(AlarmNames=["a"], AlarmTypes=["MetricAlarm"])


def test_get_alarm_none(conn):
    conn.describe_alarms.return_value = {"MetricAlarms": []}
    assert boto3_cloudwatch.get_alarm("a") is None


def test_get_alarm_error(conn, client_error):
    conn.describe_alarms.side_effect = client_error("Denied", "DescribeAlarms")
    result = boto3_cloudwatch.get_alarm("a")
    assert "error" in result


def test_get_all_alarms_no_prefix(conn):
    paginator = MagicMock()
    paginator.paginate.return_value = [{"MetricAlarms": [{"AlarmName": "a1", "MetricName": "m"}]}]
    conn.get_paginator.return_value = paginator
    result = boto3_cloudwatch.get_all_alarms()
    assert "manage alarm a1" in result


def test_get_all_alarms_prefix_skips_existing(conn):
    paginator = MagicMock()
    paginator.paginate.return_value = [
        {
            "MetricAlarms": [
                {"AlarmName": "PFX a1", "MetricName": "m"},
                {"AlarmName": "a2", "MetricName": "m"},
            ]
        }
    ]
    conn.get_paginator.return_value = paginator
    result = boto3_cloudwatch.get_all_alarms(prefix="PFX ")
    assert "manage alarm PFX a2" in result
    assert "manage alarm PFX a1" not in result


def test_create_or_update_alarm_minimal(conn):
    assert boto3_cloudwatch.create_or_update_alarm("a") is True
    conn.put_metric_alarm.assert_called_once_with(AlarmName="a", AlarmDescription="")


def test_create_or_update_alarm_all_kwargs(conn):
    boto3_cloudwatch.create_or_update_alarm(
        "a",
        MetricName="m",
        Namespace="AWS/SQS",
        Statistic="Average",
        ComparisonOperator="GreaterThanThreshold",
        Threshold="5",
        Period="60",
        EvaluationPeriods="1",
        Unit="Count",
        AlarmDescription="desc",
        Dimensions=[{"Name": "n", "Value": "v"}],
        AlarmActions=["arn:a"],
        InsufficientDataActions=["arn:b"],
        OKActions=["arn:c"],
    )
    kwargs = conn.put_metric_alarm.call_args.kwargs
    assert kwargs["Threshold"] == 5.0
    assert kwargs["Period"] == 60
    assert kwargs["EvaluationPeriods"] == 1
    assert kwargs["Dimensions"] == [{"Name": "n", "Value": "v"}]
    assert kwargs["AlarmActions"] == ["arn:a"]


def test_create_or_update_alarm_converts_dimensions_from_dict(conn):
    boto3_cloudwatch.create_or_update_alarm("a", Dimensions={"q": ["v1", "v2"]})
    kwargs = conn.put_metric_alarm.call_args.kwargs
    assert kwargs["Dimensions"] == [
        {"Name": "q", "Value": "v1"},
        {"Name": "q", "Value": "v2"},
    ]


def test_create_or_update_alarm_converts_dimensions_scalar(conn):
    boto3_cloudwatch.create_or_update_alarm("a", Dimensions={"q": "v"})
    kwargs = conn.put_metric_alarm.call_args.kwargs
    assert kwargs["Dimensions"] == [{"Name": "q", "Value": "v"}]


def test_create_or_update_alarm_parses_dimensions_json(conn):
    boto3_cloudwatch.create_or_update_alarm("a", Dimensions='{"q": "v"}')
    kwargs = conn.put_metric_alarm.call_args.kwargs
    assert kwargs["Dimensions"] == [{"Name": "q", "Value": "v"}]


def test_create_or_update_alarm_splits_action_strings(conn):
    boto3_cloudwatch.create_or_update_alarm(
        "a", AlarmActions="arn:a,arn:b", OKActions="arn:c", InsufficientDataActions="arn:d"
    )
    kwargs = conn.put_metric_alarm.call_args.kwargs
    assert kwargs["AlarmActions"] == ["arn:a", "arn:b"]
    assert kwargs["OKActions"] == ["arn:c"]
    assert kwargs["InsufficientDataActions"] == ["arn:d"]


def test_create_or_update_alarm_error(conn, client_error):
    conn.put_metric_alarm.side_effect = client_error("Denied", "PutMetricAlarm")
    assert boto3_cloudwatch.create_or_update_alarm("a") is False


def test_convert_to_arn_passthrough():
    assert boto3_cloudwatch.convert_to_arn(["arn:aws:...x"]) == ["arn:aws:...x"]


def test_convert_to_arn_scaling_policy(configure_loader_modules):
    lookup = MagicMock(return_value="arn:policy")
    with patch.dict(boto3_cloudwatch.__salt__, {"boto3_asg.get_scaling_policy_arn": lookup}):
        result = boto3_cloudwatch.convert_to_arn(["scaling_policy:my-asg:ScaleDown"])
    assert result == ["arn:policy"]
    lookup.assert_called_once_with("my-asg", "ScaleDown", None, None, None, None)


def test_convert_to_arn_scaling_policy_not_found(configure_loader_modules):
    with patch.dict(
        boto3_cloudwatch.__salt__,
        {"boto3_asg.get_scaling_policy_arn": MagicMock(return_value=None)},
    ):
        result = boto3_cloudwatch.convert_to_arn(["scaling_policy:my-asg:ScaleDown"])
    assert not result


def test_delete_alarm_success(conn):
    assert boto3_cloudwatch.delete_alarm("a") is True
    conn.delete_alarms.assert_called_once_with(AlarmNames=["a"])


def test_delete_alarm_error(conn, client_error):
    conn.delete_alarms.side_effect = client_error("Denied", "DeleteAlarms")
    assert boto3_cloudwatch.delete_alarm("a") is False
