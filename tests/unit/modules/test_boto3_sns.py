"""
Unit tests for the ``boto3_sns`` execution module.

Mirrors the canonical execution-module template in
``tests/unit/modules/test_boto3_cloudtrail.py``: a ``conn`` fixture wraps the
shared ``make_conn`` factory, ``client_error`` builds botocore exceptions, and
per-behaviour fixtures keep individual tests short and one-behaviour-per-test.
"""

from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from saltext.boto3.modules import boto3_sns

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
        boto3_sns: {
            "__opts__": {},
            "__context__": {},
            "__salt__": {},
            "__utils__": {"boto3.assign_funcs": MagicMock()},
        }
    }


@pytest.fixture
def conn(make_conn):
    with make_conn(boto3_sns) as client:
        yield client


@pytest.fixture
def two_topics(conn):
    arn1 = "arn:aws:sns:us-east-1:123:alpha"
    arn2 = "arn:aws:sns:us-east-1:123:beta"
    conn.list_topics.return_value = {"Topics": [{"TopicArn": arn1}, {"TopicArn": arn2}]}
    return {"alpha": arn1, "beta": arn2}


@pytest.fixture
def empty_topics(conn):
    conn.list_topics.return_value = {"Topics": []}
    return conn


@pytest.fixture
def iam_account():
    salt_mocks = {
        "boto3_iam.get_account_id": MagicMock(return_value="123456789012"),
        "config.option": MagicMock(return_value=None),
    }
    with patch.dict(boto3_sns.__salt__, salt_mocks):
        yield salt_mocks


def test_list_topics_paginates(conn):
    arn1 = "arn:aws:sns:us-east-1:123:alpha"
    arn2 = "arn:aws:sns:us-east-1:123:beta"
    conn.list_topics.side_effect = [
        {"Topics": [{"TopicArn": arn1}], "NextToken": "tok"},
        {"Topics": [{"TopicArn": arn2}]},
    ]
    assert boto3_sns.list_topics() == {"alpha": arn1, "beta": arn2}
    assert conn.list_topics.call_count == 2


def test_list_topics_client_error(conn, client_error):
    conn.list_topics.side_effect = client_error("AccessDenied", "ListTopics")
    assert boto3_sns.list_topics() is None


def test_topic_exists_by_name(two_topics):
    assert boto3_sns.topic_exists("alpha") is True


def test_topic_exists_by_arn(two_topics):
    assert boto3_sns.topic_exists(two_topics["alpha"]) is True


@pytest.mark.usefixtures("empty_topics")
def test_topic_exists_false():
    assert boto3_sns.topic_exists("nope") is False


def test_topic_exists_list_topics_error(conn, client_error):
    conn.list_topics.side_effect = client_error("AccessDenied", "ListTopics")
    assert boto3_sns.topic_exists("anything") is False


def test_describe_topic_found(conn):
    arn = "arn:aws:sns:us-east-1:123:alpha"
    conn.list_topics.return_value = {"Topics": [{"TopicArn": arn}]}
    conn.list_subscriptions_by_topic.return_value = {"Subscriptions": []}
    conn.get_topic_attributes.return_value = {"Attributes": {"DisplayName": "A"}}
    assert boto3_sns.describe_topic("alpha") == {
        "TopicArn": arn,
        "Subscriptions": [],
        "Attributes": {"DisplayName": "A"},
    }


@pytest.mark.usefixtures("empty_topics")
def test_describe_topic_missing():
    assert not boto3_sns.describe_topic("nope")


def test_describe_topic_list_topics_error(conn, client_error):
    conn.list_topics.side_effect = client_error("AccessDenied", "ListTopics")
    assert not boto3_sns.describe_topic("anything")


def test_create_topic(conn):
    conn.create_topic.return_value = {"TopicArn": "arn:aws:sns:us-east-1:123:new"}
    assert boto3_sns.create_topic("new") == "arn:aws:sns:us-east-1:123:new"
    conn.create_topic.assert_called_once_with(Name="new")


def test_create_topic_client_error(conn, client_error):
    conn.create_topic.side_effect = client_error("AuthFailure", "CreateTopic")
    assert boto3_sns.create_topic("new") is None


def test_create_topic_missing_arn_in_response(conn):
    conn.create_topic.return_value = {}
    assert boto3_sns.create_topic("new") is None


def test_delete_topic(conn):
    assert boto3_sns.delete_topic("arn:aws:sns:us-east-1:123:t") is True
    conn.delete_topic.assert_called_once_with(TopicArn="arn:aws:sns:us-east-1:123:t")


def test_delete_topic_client_error(conn, client_error):
    conn.delete_topic.side_effect = client_error("NotFound", "DeleteTopic")
    assert boto3_sns.delete_topic("arn:aws:sns:us-east-1:123:t") is False


def test_get_topic_attributes(conn):
    conn.get_topic_attributes.return_value = {"Attributes": {"DisplayName": "hi"}}
    assert boto3_sns.get_topic_attributes("arn") == {"DisplayName": "hi"}


def test_get_topic_attributes_client_error(conn, client_error):
    conn.get_topic_attributes.side_effect = client_error("NotFound", "GetTopicAttributes")
    assert boto3_sns.get_topic_attributes("arn") is None


def test_set_topic_attributes(conn):
    assert boto3_sns.set_topic_attributes("arn", "DisplayName", "v") is True
    conn.set_topic_attributes.assert_called_once_with(
        TopicArn="arn", AttributeName="DisplayName", AttributeValue="v"
    )


def test_set_topic_attributes_client_error(conn, client_error):
    conn.set_topic_attributes.side_effect = client_error("AuthFailure", "SetTopicAttributes")
    assert boto3_sns.set_topic_attributes("arn", "k", "v") is False


def test_list_subscriptions_by_topic_paginates(conn):
    conn.list_subscriptions_by_topic.side_effect = [
        {"Subscriptions": [{"SubscriptionArn": "a"}], "NextToken": "tok"},
        {"Subscriptions": [{"SubscriptionArn": "b"}]},
    ]
    assert boto3_sns.list_subscriptions_by_topic("arn") == [
        {"SubscriptionArn": "a"},
        {"SubscriptionArn": "b"},
    ]


def test_list_subscriptions_by_topic_client_error(conn, client_error):
    conn.list_subscriptions_by_topic.side_effect = client_error(
        "NotFound", "ListSubscriptionsByTopic"
    )
    assert boto3_sns.list_subscriptions_by_topic("arn") is None


def test_list_subscriptions(conn):
    conn.list_subscriptions.return_value = {"Subscriptions": [{"SubscriptionArn": "a"}]}
    assert boto3_sns.list_subscriptions() == [{"SubscriptionArn": "a"}]


def test_list_subscriptions_client_error(conn, client_error):
    conn.list_subscriptions.side_effect = client_error("AuthFailure", "ListSubscriptions")
    assert boto3_sns.list_subscriptions() is None


def test_get_subscription_attributes(conn):
    conn.get_subscription_attributes.return_value = {"Attributes": {"RawMessageDelivery": "true"}}
    assert boto3_sns.get_subscription_attributes("sarn") == {"RawMessageDelivery": "true"}


def test_get_subscription_attributes_missing_key(conn):
    conn.get_subscription_attributes.return_value = {}
    assert boto3_sns.get_subscription_attributes("sarn") is None


def test_get_subscription_attributes_client_error(conn, client_error):
    conn.get_subscription_attributes.side_effect = client_error(
        "NotFound", "GetSubscriptionAttributes"
    )
    assert boto3_sns.get_subscription_attributes("sarn") is None


def test_set_subscription_attributes(conn):
    assert boto3_sns.set_subscription_attributes("sarn", "k", "v") is True
    conn.set_subscription_attributes.assert_called_once_with(
        SubscriptionArn="sarn", AttributeName="k", AttributeValue="v"
    )


def test_set_subscription_attributes_client_error(conn, client_error):
    conn.set_subscription_attributes.side_effect = client_error(
        "NotFound", "SetSubscriptionAttributes"
    )
    assert boto3_sns.set_subscription_attributes("sarn", "k", "v") is False


def test_subscribe(conn):
    conn.subscribe.return_value = {"SubscriptionArn": "arn:aws:sns:us-east-1:123:t:sub"}
    assert boto3_sns.subscribe("tarn", "https", "https://x") == "arn:aws:sns:us-east-1:123:t:sub"
    conn.subscribe.assert_called_once_with(TopicArn="tarn", Protocol="https", Endpoint="https://x")


def test_subscribe_missing_arn_in_response(conn):
    conn.subscribe.return_value = {}
    assert boto3_sns.subscribe("tarn", "https", "https://x") is None


def test_subscribe_client_error(conn, client_error):
    conn.subscribe.side_effect = client_error("NotFound", "Subscribe")
    assert boto3_sns.subscribe("tarn", "https", "https://x") is None


def test_unsubscribe_skips_pending(conn):
    assert boto3_sns.unsubscribe("PendingConfirmation") is True
    conn.unsubscribe.assert_not_called()


def test_unsubscribe_not_found_in_list(conn):
    conn.list_subscriptions.return_value = {"Subscriptions": []}
    assert boto3_sns.unsubscribe("arn:aws:sns:us-east-1:123:t:sub") is False


def test_unsubscribe(conn):
    sub_arn = "arn:aws:sns:us-east-1:123:t:sub"
    conn.list_subscriptions.return_value = {
        "Subscriptions": [{"SubscriptionArn": sub_arn, "TopicArn": "arn:aws:sns:us-east-1:123:t"}]
    }
    assert boto3_sns.unsubscribe(sub_arn) is True
    conn.unsubscribe.assert_called_once_with(SubscriptionArn=sub_arn)


def test_unsubscribe_client_error(conn, client_error):
    sub_arn = "arn:aws:sns:us-east-1:123:t:sub"
    conn.list_subscriptions.return_value = {
        "Subscriptions": [{"SubscriptionArn": sub_arn, "TopicArn": "arn:aws:sns:us-east-1:123:t"}]
    }
    conn.unsubscribe.side_effect = client_error("AuthFailure", "Unsubscribe")
    assert boto3_sns.unsubscribe(sub_arn) is False


def test_get_arn_passthrough_when_already_arn():
    arn = "arn:aws:sns:us-east-1:123456789012:mytopic"
    assert boto3_sns.get_arn(arn) == arn


@pytest.mark.usefixtures("iam_account")
def test_get_arn_builds_from_name():
    assert (
        boto3_sns.get_arn("mytopic", region="us-west-2")
        == "arn:aws:sns:us-west-2:123456789012:mytopic"
    )


@pytest.mark.usefixtures("iam_account")
def test_get_arn_default_region():
    assert boto3_sns.get_arn("mytopic") == "arn:aws:sns:us-east-1:123456789012:mytopic"


@pytest.mark.usefixtures("iam_account")
def test_get_arn_uses_profile_region():
    assert (
        boto3_sns.get_arn("mytopic", profile={"region": "eu-central-1"})
        == "arn:aws:sns:eu-central-1:123456789012:mytopic"
    )


def test_get_arn_uses_config_region():
    salt_mocks = {
        "boto3_iam.get_account_id": MagicMock(return_value="123456789012"),
        "config.option": MagicMock(return_value="ap-south-1"),
    }
    with patch.dict(boto3_sns.__salt__, salt_mocks):
        assert boto3_sns.get_arn("mytopic") == "arn:aws:sns:ap-south-1:123456789012:mytopic"
