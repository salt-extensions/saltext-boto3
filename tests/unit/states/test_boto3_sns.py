"""
Unit tests for the ``boto3_sns`` state module.

Mirrors the canonical state-module template in
``tests/unit/states/test_boto3_cloudtrail.py``: every test uses the shared
``mock_salt`` context manager from ``tests/unit/conftest.py`` and asserts the
explicit ``result``/``comment``/``changes`` contract.
"""

from unittest.mock import MagicMock

import pytest

from saltext.boto3.states import boto3_sns as sns_state

try:
    import botocore  # noqa: F401  # pylint: disable=unused-import

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
        sns_state: {
            "__opts__": {"test": False},
            "__salt__": {},
        }
    }


TOPIC_ARN = "arn:aws:sns:us-east-1:123456789012:mytopic"


@pytest.fixture
def topic_present_snapshot():
    return {
        "TopicArn": TOPIC_ARN,
        "Subscriptions": [],
        "Attributes": {"DisplayName": "old", "Policy": "{}", "DeliveryPolicy": "{}"},
    }


@pytest.fixture
def create_mocks():
    """No topic yet — exec mocks for the create-on-missing branch."""
    return {
        "boto3_sns.describe_topic": MagicMock(side_effect=[{}, {"TopicArn": TOPIC_ARN}]),
        "boto3_sns.create_topic": TOPIC_ARN,
        "boto3_sns.get_topic_attributes": {},
        "boto3_sns.subscribe": "arn:aws:sns:us-east-1:123:t:sub",
    }


@pytest.fixture
def already_present_mocks(topic_present_snapshot):
    """Topic exists with no pending changes."""
    return {
        "boto3_sns.describe_topic": topic_present_snapshot,
        "boto3_sns.get_topic_attributes": topic_present_snapshot["Attributes"],
    }


def test_virtual(mock_salt):
    with mock_salt(sns_state, {"boto3_sns.topic_exists": True}):
        assert sns_state.__virtual__() == "boto3_sns"


def test_virtual_no_exec_module(mock_salt):
    with mock_salt(sns_state, {}):
        result = sns_state.__virtual__()
    assert result[0] is False
    assert "boto3_sns" in result[1]


def test_topic_present_creates_topic(mock_salt, create_mocks):
    with mock_salt(sns_state, create_mocks) as salt:
        ret = sns_state.topic_present("mytopic")
    assert ret["result"] is True
    assert "created" in ret["comment"]
    assert ret["changes"]["old"] == {}
    assert ret["changes"]["new"] == {"TopicArn": TOPIC_ARN}
    salt["boto3_sns.create_topic"].assert_called_once()


def test_topic_present_test_mode_create(mock_salt):
    mocks = {"boto3_sns.describe_topic": {}}
    with mock_salt(sns_state, mocks, test=True) as salt:
        ret = sns_state.topic_present("mytopic")
    assert ret["result"] is None
    assert not ret["changes"]
    assert "would be created" in ret["comment"]
    salt["boto3_sns.describe_topic"].assert_called_once()


def test_topic_present_create_failure(mock_salt):
    mocks = {
        "boto3_sns.describe_topic": {},
        "boto3_sns.create_topic": None,
    }
    with mock_salt(sns_state, mocks):
        ret = sns_state.topic_present("mytopic")
    assert ret["result"] is False
    assert not ret["changes"]
    assert "Failed to create" in ret["comment"]


def test_topic_present_already_present_no_changes(mock_salt, already_present_mocks):
    with mock_salt(sns_state, already_present_mocks):
        ret = sns_state.topic_present("mytopic")
    assert ret["result"] is True
    assert not ret["changes"]
    assert "present" in ret["comment"]


def test_topic_present_updates_attribute(mock_salt, topic_present_snapshot):
    new_policy = {"Version": "2012-10-17", "Id": "new"}
    mocks = {
        "boto3_sns.describe_topic": MagicMock(
            side_effect=[
                topic_present_snapshot,
                {
                    **topic_present_snapshot,
                    "Attributes": {**topic_present_snapshot["Attributes"], "Policy": new_policy},
                },
            ]
        ),
        "boto3_sns.get_topic_attributes": topic_present_snapshot["Attributes"],
        "boto3_sns.set_topic_attributes": True,
    }
    with mock_salt(sns_state, mocks) as salt:
        ret = sns_state.topic_present("mytopic", attributes={"Policy": new_policy})
    assert ret["result"] is True
    assert "Policy set to" in ret["comment"]
    assert ret["changes"]["new"]["Attributes"]["Policy"] == new_policy
    salt["boto3_sns.set_topic_attributes"].assert_called_once()


def test_topic_present_test_mode_attribute_update(mock_salt, topic_present_snapshot):
    new_policy = {"Version": "2012-10-17", "Id": "new"}
    mocks = {
        "boto3_sns.describe_topic": topic_present_snapshot,
        "boto3_sns.get_topic_attributes": topic_present_snapshot["Attributes"],
    }
    with mock_salt(sns_state, mocks, test=True) as salt:
        ret = sns_state.topic_present("mytopic", attributes={"Policy": new_policy})
    assert ret["result"] is None
    assert not ret["changes"]
    assert "would be updated" in ret["comment"]
    assert (
        "boto3_sns.set_topic_attributes" not in salt
        or not salt["boto3_sns.set_topic_attributes"].called
    )


def test_topic_present_attribute_update_failure(mock_salt, topic_present_snapshot):
    new_policy = {"Version": "2012-10-17", "Id": "new"}
    mocks = {
        "boto3_sns.describe_topic": topic_present_snapshot,
        "boto3_sns.get_topic_attributes": topic_present_snapshot["Attributes"],
        "boto3_sns.set_topic_attributes": False,
    }
    with mock_salt(sns_state, mocks):
        ret = sns_state.topic_present("mytopic", attributes={"Policy": new_policy})
    assert ret["result"] is False
    assert not ret["changes"]
    assert "Failed to update" in ret["comment"]


def test_topic_present_subscribes_new_endpoint(mock_salt, topic_present_snapshot):
    new_arn = "arn:aws:sns:us-east-1:123:t:sub"
    mocks = {
        "boto3_sns.describe_topic": MagicMock(
            side_effect=[
                topic_present_snapshot,
                {
                    **topic_present_snapshot,
                    "Subscriptions": [
                        {
                            "Protocol": "https",
                            "Endpoint": "https://x",
                            "SubscriptionArn": new_arn,
                        }
                    ],
                },
            ]
        ),
        "boto3_sns.get_topic_attributes": topic_present_snapshot["Attributes"],
        "boto3_sns.subscribe": new_arn,
    }
    subs = [{"Protocol": "https", "Endpoint": "https://x"}]
    with mock_salt(sns_state, mocks) as salt:
        ret = sns_state.topic_present("mytopic", subscriptions=subs)
    assert ret["result"] is True
    assert "Subscription https:https://x set" in ret["comment"]
    assert ret["changes"]["new"]["Subscriptions"][0]["SubscriptionArn"] == new_arn
    salt["boto3_sns.subscribe"].assert_called_once()


def test_topic_present_subscribe_failure(mock_salt, topic_present_snapshot):
    mocks = {
        "boto3_sns.describe_topic": topic_present_snapshot,
        "boto3_sns.get_topic_attributes": topic_present_snapshot["Attributes"],
        "boto3_sns.subscribe": None,
    }
    subs = [{"Protocol": "https", "Endpoint": "https://x"}]
    with mock_salt(sns_state, mocks):
        ret = sns_state.topic_present("mytopic", subscriptions=subs)
    assert ret["result"] is False
    assert not ret["changes"]
    assert "Failed to set subscription" in ret["comment"]


def test_topic_present_unsubscribes_extra_endpoint(mock_salt, topic_present_snapshot):
    sub_arn = "arn:aws:sns:us-east-1:123:t:gone"
    snapshot = {
        **topic_present_snapshot,
        "Subscriptions": [
            {"Protocol": "https", "Endpoint": "https://gone", "SubscriptionArn": sub_arn}
        ],
    }
    mocks = {
        "boto3_sns.describe_topic": MagicMock(
            side_effect=[
                snapshot,
                {**topic_present_snapshot, "Subscriptions": []},
            ]
        ),
        "boto3_sns.get_topic_attributes": topic_present_snapshot["Attributes"],
        "boto3_sns.unsubscribe": True,
    }
    with mock_salt(sns_state, mocks) as salt:
        ret = sns_state.topic_present("mytopic", subscriptions=[])
    assert ret["result"] is True
    assert sub_arn in ret["comment"]
    assert "removed from topic" in ret["comment"]
    salt["boto3_sns.unsubscribe"].assert_called_once()


def test_topic_absent_when_missing(mock_salt):
    with mock_salt(sns_state, {"boto3_sns.describe_topic": {}}):
        ret = sns_state.topic_absent("mytopic")
    assert ret["result"] is True
    assert not ret["changes"]
    assert "absent" in ret["comment"]


def test_topic_absent_deletes_existing(mock_salt, topic_present_snapshot):
    mocks = {
        "boto3_sns.describe_topic": MagicMock(side_effect=[topic_present_snapshot, {}]),
        "boto3_sns.delete_topic": True,
    }
    with mock_salt(sns_state, mocks) as salt:
        ret = sns_state.topic_absent("mytopic")
    assert ret["result"] is True
    assert "deleted" in ret["comment"]
    assert ret["changes"]["old"] == topic_present_snapshot
    assert ret["changes"]["new"] == {}
    salt["boto3_sns.delete_topic"].assert_called_once()


def test_topic_absent_test_mode(mock_salt, topic_present_snapshot):
    mocks = {"boto3_sns.describe_topic": topic_present_snapshot}
    with mock_salt(sns_state, mocks, test=True):
        ret = sns_state.topic_absent("mytopic")
    assert ret["result"] is None
    assert not ret["changes"]
    assert "would be removed" in ret["comment"]


def test_topic_absent_unsubscribes_then_deletes(mock_salt, topic_present_snapshot):
    sub_arn = "arn:aws:sns:us-east-1:123:t:sub"
    snapshot = {
        **topic_present_snapshot,
        "Subscriptions": [
            {
                "Protocol": "https",
                "Endpoint": "https://x",
                "TopicArn": TOPIC_ARN,
                "SubscriptionArn": sub_arn,
            }
        ],
    }
    mocks = {
        "boto3_sns.describe_topic": MagicMock(side_effect=[snapshot, {}]),
        "boto3_sns.unsubscribe": True,
        "boto3_sns.delete_topic": True,
    }
    with mock_salt(sns_state, mocks, test=False) as salt:
        ret = sns_state.topic_absent("mytopic", unsubscribe=True)
    assert ret["result"] is True
    salt["boto3_sns.unsubscribe"].assert_called_once_with(
        sub_arn, region=None, key=None, keyid=None, profile=None
    )
    salt["boto3_sns.delete_topic"].assert_called_once()


def test_topic_absent_delete_failure(mock_salt, topic_present_snapshot):
    mocks = {
        "boto3_sns.describe_topic": topic_present_snapshot,
        "boto3_sns.delete_topic": False,
    }
    with mock_salt(sns_state, mocks):
        ret = sns_state.topic_absent("mytopic")
    assert ret["result"] is False
    assert not ret["changes"]
    assert "Failed to delete" in ret["comment"]


def test_topic_absent_unsubscribe_failure(mock_salt, topic_present_snapshot):
    sub_arn = "arn:aws:sns:us-east-1:123:t:sub"
    snapshot = {
        **topic_present_snapshot,
        "Subscriptions": [
            {
                "Protocol": "https",
                "Endpoint": "https://x",
                "TopicArn": TOPIC_ARN,
                "SubscriptionArn": sub_arn,
            }
        ],
    }
    mocks = {
        "boto3_sns.describe_topic": snapshot,
        "boto3_sns.unsubscribe": False,
    }
    with mock_salt(sns_state, mocks):
        ret = sns_state.topic_absent("mytopic", unsubscribe=True)
    assert ret["result"] is False
    assert not ret["changes"]
    assert "Failed to delete subscription" in ret["comment"]
