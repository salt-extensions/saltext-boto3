"""
Unit tests for the ``boto3_kinesis`` state module.
"""

import pytest

from saltext.boto3.states import boto3_kinesis

pytestmark = [
    pytest.mark.skip_on_fips_enabled_platform,
]


def _active_stream(num_shards=1, retention=24, enhanced=None):
    enhanced = enhanced if enhanced is not None else []
    shards = []
    chunk = 1000
    for i in range(num_shards):
        shards.append(
            {
                "ShardId": f"shard-{i}",
                "SequenceNumberRange": {"StartingSequenceNumber": "1"},
                "HashKeyRange": {
                    "StartingHashKey": str(i * chunk + (1 if i else 0)),
                    "EndingHashKey": str((i + 1) * chunk),
                },
            }
        )
    return {
        "result": {
            "StreamDescription": {
                "StreamStatus": "ACTIVE",
                "HasMoreShards": False,
                "Shards": shards,
                "RetentionPeriodHours": retention,
                "EnhancedMonitoring": [{"ShardLevelMetrics": enhanced}],
            }
        }
    }


_RESHARD_INFO = (0, 1000, {"OpenShards": [{"ShardId": "s0"}]})


def _base_map(**overrides):
    salt_map = {
        "boto3_kinesis.exists": {"result": True},
        "boto3_kinesis.create_stream": {"result": True},
        "boto3_kinesis.delete_stream": {"result": True},
        "boto3_kinesis.get_stream_when_active": _active_stream(),
        "boto3_kinesis.increase_stream_retention_period": {"result": True},
        "boto3_kinesis.decrease_stream_retention_period": {"result": True},
        "boto3_kinesis.enable_enhanced_monitoring": {"result": True},
        "boto3_kinesis.disable_enhanced_monitoring": {"result": True},
        "boto3_kinesis.get_info_for_reshard": _RESHARD_INFO,
        "boto3_kinesis.reshard": {"result": False},
    }
    salt_map.update(overrides)
    return salt_map


@pytest.fixture
def configure_loader_modules():
    return {boto3_kinesis: {"__opts__": {"test": False}, "__salt__": {}}}


# --- __virtual__ --------------------------------------------------------


def test_virtual(mock_salt):
    with mock_salt(boto3_kinesis, {"boto3_kinesis.exists": True}):
        assert boto3_kinesis.__virtual__() == "boto3_kinesis"


def test_virtual_no_exec_module(mock_salt):
    with mock_salt(boto3_kinesis, {}):
        result = boto3_kinesis.__virtual__()
    assert result[0] is False
    assert "boto3_kinesis execution module is unavailable" in result[1]


def test_present_creates_stream(mock_salt):
    salt_map = _base_map(**{"boto3_kinesis.exists": {"result": False}})
    with mock_salt(boto3_kinesis, salt_map) as salt_mocks:
        ret = boto3_kinesis.present("mystream", num_shards=1, do_reshard=False)
    assert ret["result"] is True
    salt_mocks["boto3_kinesis.create_stream"].assert_called_once()
    assert ret["changes"]["new"]["name"] == "mystream"


def test_present_create_test_mode(mock_salt):
    salt_map = _base_map(**{"boto3_kinesis.exists": {"result": False}})
    with mock_salt(boto3_kinesis, salt_map, test=True) as salt_mocks:
        ret = boto3_kinesis.present("mystream", num_shards=1)
    assert ret["result"] is None
    assert "would be created" in ret["comment"]
    salt_mocks["boto3_kinesis.create_stream"].assert_not_called()


def test_present_create_error(mock_salt):
    salt_map = _base_map(
        **{
            "boto3_kinesis.exists": {"result": False},
            "boto3_kinesis.create_stream": {"error": "boom"},
        }
    )
    with mock_salt(boto3_kinesis, salt_map):
        ret = boto3_kinesis.present("mystream", num_shards=1)
    assert ret["result"] is False
    assert "Failed to create" in ret["comment"]


def test_present_describe_error(mock_salt):
    salt_map = _base_map(**{"boto3_kinesis.get_stream_when_active": {"error": "boom"}})
    with mock_salt(boto3_kinesis, salt_map):
        ret = boto3_kinesis.present("mystream", num_shards=1)
    assert ret["result"] is False


def test_present_retention_increase(mock_salt):
    salt_map = _base_map(**{"boto3_kinesis.get_stream_when_active": _active_stream(retention=24)})
    with mock_salt(boto3_kinesis, salt_map) as salt_mocks:
        ret = boto3_kinesis.present("mystream", retention_hours=48, num_shards=1, do_reshard=False)
    assert ret["result"] is True
    salt_mocks["boto3_kinesis.increase_stream_retention_period"].assert_called_once()
    salt_mocks["boto3_kinesis.decrease_stream_retention_period"].assert_not_called()
    assert ret["changes"]["old"]["retention_hours"] == 24
    assert ret["changes"]["new"]["retention_hours"] == 48


def test_present_retention_decrease(mock_salt):
    salt_map = _base_map(**{"boto3_kinesis.get_stream_when_active": _active_stream(retention=168)})
    with mock_salt(boto3_kinesis, salt_map) as salt_mocks:
        ret = boto3_kinesis.present("mystream", retention_hours=24, num_shards=1, do_reshard=False)
    assert ret["result"] is True
    salt_mocks["boto3_kinesis.decrease_stream_retention_period"].assert_called_once()


def test_present_retention_test_mode(mock_salt):
    salt_map = _base_map(**{"boto3_kinesis.get_stream_when_active": _active_stream(retention=24)})
    with mock_salt(boto3_kinesis, salt_map, test=True) as salt_mocks:
        ret = boto3_kinesis.present("mystream", retention_hours=48, num_shards=1, do_reshard=False)
    assert ret["result"] is None
    salt_mocks["boto3_kinesis.increase_stream_retention_period"].assert_not_called()


def test_present_retention_update_error(mock_salt):
    salt_map = _base_map(
        **{
            "boto3_kinesis.get_stream_when_active": _active_stream(retention=24),
            "boto3_kinesis.increase_stream_retention_period": {"error": "boom"},
        }
    )
    with mock_salt(boto3_kinesis, salt_map):
        ret = boto3_kinesis.present("mystream", retention_hours=48)
    assert ret["result"] is False


def test_present_enhanced_monitoring_enable_all(mock_salt):
    salt_map = _base_map(**{"boto3_kinesis.get_stream_when_active": _active_stream(enhanced=[])})
    with mock_salt(boto3_kinesis, salt_map) as salt_mocks:
        ret = boto3_kinesis.present(
            "mystream", enhanced_monitoring=["ALL"], num_shards=1, do_reshard=False
        )
    assert ret["result"] is True
    salt_mocks["boto3_kinesis.enable_enhanced_monitoring"].assert_called_once()


def test_present_enhanced_monitoring_disable_all(mock_salt):
    salt_map = _base_map(
        **{"boto3_kinesis.get_stream_when_active": _active_stream(enhanced=["IncomingBytes"])}
    )
    with mock_salt(boto3_kinesis, salt_map) as salt_mocks:
        ret = boto3_kinesis.present(
            "mystream", enhanced_monitoring=False, num_shards=1, do_reshard=False
        )
    assert ret["result"] is True
    salt_mocks["boto3_kinesis.disable_enhanced_monitoring"].assert_called_once()


def test_present_enhanced_monitoring_no_change(mock_salt):
    salt_map = _base_map(
        **{"boto3_kinesis.get_stream_when_active": _active_stream(enhanced=["IncomingBytes"])}
    )
    with mock_salt(boto3_kinesis, salt_map) as salt_mocks:
        ret = boto3_kinesis.present(
            "mystream",
            enhanced_monitoring=["IncomingBytes"],
            num_shards=1,
            do_reshard=False,
        )
    assert ret["result"] is True
    salt_mocks["boto3_kinesis.enable_enhanced_monitoring"].assert_not_called()
    salt_mocks["boto3_kinesis.disable_enhanced_monitoring"].assert_not_called()


def test_present_enable_monitoring_error(mock_salt):
    salt_map = _base_map(
        **{
            "boto3_kinesis.get_stream_when_active": _active_stream(enhanced=[]),
            "boto3_kinesis.enable_enhanced_monitoring": {"error": "boom"},
        }
    )
    with mock_salt(boto3_kinesis, salt_map):
        ret = boto3_kinesis.present(
            "mystream",
            enhanced_monitoring=["IncomingBytes"],
            num_shards=1,
            do_reshard=False,
        )
    assert ret["result"] is False


def test_present_reshard_needed(mock_salt):
    from unittest.mock import MagicMock  # pylint: disable=import-outside-toplevel

    reshard_mock = MagicMock(side_effect=[{"result": True}, {"result": False}])
    salt_map = _base_map(
        **{
            "boto3_kinesis.get_stream_when_active": _active_stream(num_shards=1),
            "boto3_kinesis.reshard": reshard_mock,
        }
    )
    with mock_salt(boto3_kinesis, salt_map):
        ret = boto3_kinesis.present("mystream", num_shards=2, do_reshard=True)
    assert ret["result"] is True
    assert reshard_mock.call_count == 2
    assert ret["changes"]["new"]["num_shards"] == 2


def test_present_reshard_test_mode(mock_salt):
    salt_map = _base_map(**{"boto3_kinesis.get_stream_when_active": _active_stream(num_shards=1)})
    with mock_salt(boto3_kinesis, salt_map, test=True) as salt_mocks:
        ret = boto3_kinesis.present("mystream", num_shards=2, do_reshard=True)
    assert ret["result"] is None
    salt_mocks["boto3_kinesis.reshard"].assert_not_called()


def test_present_reshard_error(mock_salt):
    salt_map = _base_map(
        **{
            "boto3_kinesis.get_stream_when_active": _active_stream(num_shards=1),
            "boto3_kinesis.reshard": {"error": "boom"},
        }
    )
    with mock_salt(boto3_kinesis, salt_map):
        ret = boto3_kinesis.present("mystream", num_shards=2, do_reshard=True)
    assert ret["result"] is False


def test_present_no_reshard_when_disabled(mock_salt):
    salt_map = _base_map(**{"boto3_kinesis.get_stream_when_active": _active_stream(num_shards=1)})
    with mock_salt(boto3_kinesis, salt_map) as salt_mocks:
        ret = boto3_kinesis.present("mystream", num_shards=2, do_reshard=False)
    assert ret["result"] is True
    salt_mocks["boto3_kinesis.reshard"].assert_not_called()


def test_absent_not_exists(mock_salt):
    salt_map = _base_map(**{"boto3_kinesis.exists": {"result": False}})
    with mock_salt(boto3_kinesis, salt_map) as salt_mocks:
        ret = boto3_kinesis.absent("mystream")
    assert ret["result"] is True
    assert "does not exist" in ret["comment"]
    salt_mocks["boto3_kinesis.delete_stream"].assert_not_called()


def test_absent_test_mode(mock_salt):
    salt_map = _base_map()
    with mock_salt(boto3_kinesis, salt_map, test=True):
        ret = boto3_kinesis.absent("mystream")
    assert ret["result"] is None
    assert "would be deleted" in ret["comment"]


def test_absent_deletes(mock_salt):
    salt_map = _base_map()
    with mock_salt(boto3_kinesis, salt_map) as salt_mocks:
        ret = boto3_kinesis.absent("mystream")
    assert ret["result"] is True
    salt_mocks["boto3_kinesis.delete_stream"].assert_called_once()
    assert ret["changes"]["new"] == "Stream mystream deleted"


def test_absent_delete_error(mock_salt):
    salt_map = _base_map(**{"boto3_kinesis.delete_stream": {"error": "boom"}})
    with mock_salt(boto3_kinesis, salt_map):
        ret = boto3_kinesis.absent("mystream")
    assert ret["result"] is False
