"""
Unit tests for the ``boto3_kinesis`` execution module.
"""

from unittest.mock import patch

import pytest

from saltext.boto3.modules import boto3_kinesis

try:
    import botocore  # pylint: disable=unused-import

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
        boto3_kinesis: {
            "__opts__": {},
            "__context__": {},
            "__salt__": {},
        }
    }


@pytest.fixture(autouse=True)
def _no_sleep():
    """Short-circuit time.sleep used by retry/backoff helpers."""
    with patch.object(boto3_kinesis.time, "sleep", return_value=None):
        yield


@pytest.fixture
def conn(make_conn):
    with make_conn(boto3_kinesis) as client:
        yield client


def test_exists_true(conn):
    conn.describe_stream.return_value = {"StreamDescription": {"StreamName": "s"}}
    ret = boto3_kinesis.exists("s")
    assert ret["result"] is True


def test_exists_false(conn, client_error):
    conn.describe_stream.side_effect = client_error("ResourceNotFoundException", "DescribeStream")
    ret = boto3_kinesis.exists("s")
    assert ret["result"] is False
    assert "error" in ret


def test_create_stream(conn):
    conn.create_stream.return_value = {}
    ret = boto3_kinesis.create_stream("s", 2)
    assert ret["result"] is True
    conn.create_stream.assert_called_once_with(ShardCount=2, StreamName="s")


def test_create_stream_error(conn, client_error):
    conn.create_stream.side_effect = client_error("InvalidArgumentException", "CreateStream")
    ret = boto3_kinesis.create_stream("s", 2)
    assert "error" in ret
    assert "result" not in ret or ret["result"] is None


def test_delete_stream(conn):
    conn.delete_stream.return_value = {}
    ret = boto3_kinesis.delete_stream("s")
    assert ret["result"] is True
    conn.delete_stream.assert_called_once_with(StreamName="s")


def test_delete_stream_error(conn, client_error):
    conn.delete_stream.side_effect = client_error("ResourceNotFoundException", "DeleteStream")
    ret = boto3_kinesis.delete_stream("s")
    assert "error" in ret


def test_increase_stream_retention_period(conn):
    conn.increase_stream_retention_period.return_value = {}
    ret = boto3_kinesis.increase_stream_retention_period("s", 168)
    assert ret["result"] is True
    conn.increase_stream_retention_period.assert_called_once_with(
        StreamName="s", RetentionPeriodHours=168
    )


def test_decrease_stream_retention_period(conn):
    conn.decrease_stream_retention_period.return_value = {}
    ret = boto3_kinesis.decrease_stream_retention_period("s", 24)
    assert ret["result"] is True
    conn.decrease_stream_retention_period.assert_called_once_with(
        StreamName="s", RetentionPeriodHours=24
    )


def test_enable_enhanced_monitoring(conn):
    conn.enable_enhanced_monitoring.return_value = {}
    ret = boto3_kinesis.enable_enhanced_monitoring("s", ["IncomingBytes"])
    assert ret["result"] is True
    conn.enable_enhanced_monitoring.assert_called_once_with(
        StreamName="s", ShardLevelMetrics=["IncomingBytes"]
    )


def test_disable_enhanced_monitoring(conn):
    conn.disable_enhanced_monitoring.return_value = {}
    ret = boto3_kinesis.disable_enhanced_monitoring("s", ["IncomingBytes"])
    assert ret["result"] is True


def test_long_int():
    assert boto3_kinesis.long_int("123") == 123


def test_list_streams_paginated(conn):
    conn.list_streams.side_effect = [
        {"StreamNames": ["a", "b"], "HasMoreStreams": True},
        {"StreamNames": ["c"], "HasMoreStreams": False},
    ]
    ret = boto3_kinesis.list_streams()
    assert ret["result"] == ["a", "b", "c"]


def test_list_streams_error(conn, client_error):
    conn.list_streams.side_effect = client_error("ResourceNotFoundException", "ListStreams")
    ret = boto3_kinesis.list_streams()
    assert "error" in ret


def test_get_stream_when_active_active(conn):
    conn.describe_stream.return_value = {
        "StreamDescription": {"StreamStatus": "ACTIVE", "HasMoreShards": False}
    }
    ret = boto3_kinesis.get_stream_when_active("s")
    assert ret["result"]["StreamDescription"]["StreamStatus"] == "ACTIVE"


def test_get_stream_when_active_waits_then_active(conn):
    conn.describe_stream.side_effect = [
        {"StreamDescription": {"StreamStatus": "CREATING", "HasMoreShards": False}},
        {"StreamDescription": {"StreamStatus": "ACTIVE", "HasMoreShards": False}},
    ]
    ret = boto3_kinesis.get_stream_when_active("s")
    assert ret["result"]["StreamDescription"]["StreamStatus"] == "ACTIVE"


def test_get_stream_when_active_error(conn, client_error):
    conn.describe_stream.side_effect = client_error("ResourceNotFoundException", "DescribeStream")
    ret = boto3_kinesis.get_stream_when_active("s")
    assert "error" in ret


def test_get_full_stream_paginated(conn):
    conn.describe_stream.side_effect = [
        {
            "StreamDescription": {
                "HasMoreShards": True,
                "Shards": [{"ShardId": "shard-1"}],
            }
        },
        {
            "StreamDescription": {
                "HasMoreShards": False,
                "Shards": [{"ShardId": "shard-2"}],
            }
        },
    ]
    ret = boto3_kinesis._get_full_stream("s")
    shards = ret["result"]["StreamDescription"]["Shards"]
    assert [s["ShardId"] for s in shards] == ["shard-1", "shard-2"]


def test_get_info_for_reshard():
    stream_details = {
        "Shards": [
            {
                "ShardId": "a",
                "SequenceNumberRange": {"StartingSequenceNumber": "1"},
                "HashKeyRange": {"StartingHashKey": "0", "EndingHashKey": "100"},
            },
            {
                "ShardId": "b-closed",
                "SequenceNumberRange": {
                    "StartingSequenceNumber": "1",
                    "EndingSequenceNumber": "2",
                },
                "HashKeyRange": {"StartingHashKey": "101", "EndingHashKey": "200"},
            },
            {
                "ShardId": "c",
                "SequenceNumberRange": {"StartingSequenceNumber": "1"},
                "HashKeyRange": {"StartingHashKey": "201", "EndingHashKey": "300"},
            },
        ]
    }
    min_h, max_h, details = boto3_kinesis.get_info_for_reshard(stream_details)
    assert min_h == 0
    assert max_h == 300
    assert [s["ShardId"] for s in details["OpenShards"]] == ["a", "c"]


def test_execute_with_retries_success(conn):
    conn.describe_stream.return_value = {"ok": True}
    ret = boto3_kinesis._execute_with_retries(conn, "describe_stream", StreamName="s")
    assert ret["result"] == {"ok": True}


def test_execute_with_retries_retries_on_limit(conn, client_error):
    conn.describe_stream.side_effect = [
        client_error("LimitExceededException", "DescribeStream"),
        {"ok": True},
    ]
    ret = boto3_kinesis._execute_with_retries(conn, "describe_stream", StreamName="s")
    assert ret["result"] == {"ok": True}
    assert conn.describe_stream.call_count == 2


def test_execute_with_retries_fatal_error(conn, client_error):
    conn.describe_stream.side_effect = client_error("ResourceNotFoundException", "DescribeStream")
    ret = boto3_kinesis._execute_with_retries(conn, "describe_stream", StreamName="s")
    assert "error" in ret
    assert ret["result"] is None
    assert conn.describe_stream.call_count == 1


def test_execute_with_retries_exhausted(conn, client_error):
    conn.describe_stream.side_effect = client_error("LimitExceededException", "DescribeStream")
    ret = boto3_kinesis._execute_with_retries(conn, "describe_stream", StreamName="s")
    assert "error" in ret
    # max_attempts is 18
    assert conn.describe_stream.call_count == 18


def test_reshard_no_action_needed(conn):
    # 1 ACTIVE shard spanning full hash range; desired_size=1 so no action
    conn.describe_stream.return_value = {
        "StreamDescription": {
            "StreamStatus": "ACTIVE",
            "HasMoreShards": False,
            "Shards": [
                {
                    "ShardId": "shard-1",
                    "SequenceNumberRange": {"StartingSequenceNumber": "1"},
                    "HashKeyRange": {"StartingHashKey": "0", "EndingHashKey": "100"},
                }
            ],
        }
    }
    ret = boto3_kinesis.reshard("s", 1)
    assert ret["result"] is False


def test_reshard_dry_run_split(conn):
    # 1 open shard, desired_size=2 so split is expected; force=False -> dry run
    conn.describe_stream.return_value = {
        "StreamDescription": {
            "StreamStatus": "ACTIVE",
            "HasMoreShards": False,
            "Shards": [
                {
                    "ShardId": "shard-1",
                    "SequenceNumberRange": {"StartingSequenceNumber": "1"},
                    "HashKeyRange": {"StartingHashKey": "0", "EndingHashKey": "100"},
                }
            ],
        }
    }
    ret = boto3_kinesis.reshard("s", 2, force=False)
    assert ret["result"] is True
    conn.split_shard.assert_not_called()


def test_reshard_forced_split(conn):
    conn.describe_stream.return_value = {
        "StreamDescription": {
            "StreamStatus": "ACTIVE",
            "HasMoreShards": False,
            "Shards": [
                {
                    "ShardId": "shard-1",
                    "SequenceNumberRange": {"StartingSequenceNumber": "1"},
                    "HashKeyRange": {"StartingHashKey": "0", "EndingHashKey": "100"},
                }
            ],
        }
    }
    conn.split_shard.return_value = {}
    ret = boto3_kinesis.reshard("s", 2, force=True)
    assert ret["result"] is True
    conn.split_shard.assert_called_once()


def test_get_next_open_shard():
    stream_details = {
        "OpenShards": [
            {"ShardId": "s1"},
            {"ShardId": "s2"},
            {"ShardId": "s3"},
        ]
    }
    assert boto3_kinesis._get_next_open_shard(stream_details, "s1") == "s2"
    assert boto3_kinesis._get_next_open_shard(stream_details, "s2") == "s3"
    assert boto3_kinesis._get_next_open_shard(stream_details, "s3") is None


def test_jittered_backoff_bounded():
    for attempt in range(10):
        val = boto3_kinesis._jittered_backoff(attempt, 10)
        assert 0 <= val <= 10
