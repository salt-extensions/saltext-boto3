"""
Unit tests for the ``boto3_sqs`` execution module.
"""

import pytest

from saltext.boto3.modules import boto3_sqs

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
        boto3_sqs: {
            "__opts__": {},
            "__context__": {},
            "__salt__": {},
        }
    }


@pytest.fixture
def conn(make_conn):
    with make_conn(boto3_sqs) as client:
        yield client


@pytest.fixture
def queue_url(conn):
    conn.get_queue_url.return_value = {"QueueUrl": "http://x/1/q"}
    return conn


@pytest.mark.usefixtures("queue_url")
def test_exists():
    assert boto3_sqs.exists("q") == {"result": True}


def test_exists_missing(conn, client_error):
    conn.get_queue_url.side_effect = client_error(
        "AWS.SimpleQueueService.NonExistentQueue", "GetQueueUrl"
    )
    assert boto3_sqs.exists("q") == {"result": False}


def test_exists_client_error(conn, client_error):
    conn.get_queue_url.side_effect = client_error("SomeOther", "GetQueueUrl")
    assert "error" in boto3_sqs.exists("q")


def test_create(conn):
    assert boto3_sqs.create("q") == {"result": True}
    conn.create_queue.assert_called_once_with(QueueName="q", Attributes={})


def test_create_with_attributes_dict(conn):
    boto3_sqs.create("q", attributes={"DelaySeconds": 5})
    conn.create_queue.assert_called_once_with(QueueName="q", Attributes={"DelaySeconds": 5})


def test_create_with_attributes_json_string(conn):
    boto3_sqs.create("q", attributes='{"DelaySeconds": 5}')
    conn.create_queue.assert_called_once_with(QueueName="q", Attributes={"DelaySeconds": 5})


def test_create_with_policy_dict_stringifies(conn):
    boto3_sqs.create("q", attributes={"Policy": {"Version": "2012-10-17"}})
    assert isinstance(conn.create_queue.call_args.kwargs["Attributes"]["Policy"], str)


def test_create_client_error(conn, client_error):
    conn.create_queue.side_effect = client_error("Oops", "CreateQueue")
    assert "error" in boto3_sqs.create("q")


@pytest.mark.usefixtures("queue_url")
def test_delete(conn):
    assert boto3_sqs.delete("q") == {"result": True}
    conn.delete_queue.assert_called_once_with(QueueUrl="http://x/1/q")


def test_delete_client_error(conn, client_error):
    conn.get_queue_url.side_effect = client_error("Oops", "GetQueueUrl")
    assert "error" in boto3_sqs.delete("q")


def test_list_empty(conn):
    conn.list_queues.return_value = {}
    assert boto3_sqs.list_() == {"result": []}


def test_list_with_prefix(conn):
    conn.list_queues.return_value = {
        "QueueUrls": [
            "https://sqs.us-east-1.amazonaws.com/123456789012/alpha",
            "https://sqs.us-east-1.amazonaws.com/123456789012/beta",
        ]
    }
    assert boto3_sqs.list_(prefix="a") == {"result": ["alpha", "beta"]}
    conn.list_queues.assert_called_once_with(QueueNamePrefix="a")


def test_list_client_error(conn, client_error):
    conn.list_queues.side_effect = client_error("Oops", "ListQueues")
    assert "error" in boto3_sqs.list_()


@pytest.mark.usefixtures("queue_url")
def test_get_attributes(conn):
    conn.get_queue_attributes.return_value = {"Attributes": {"DelaySeconds": "5"}}
    assert boto3_sqs.get_attributes("q") == {"result": {"DelaySeconds": "5"}}
    conn.get_queue_attributes.assert_called_once_with(
        QueueUrl="http://x/1/q", AttributeNames=["All"]
    )


def test_get_attributes_client_error(conn, client_error):
    conn.get_queue_url.side_effect = client_error("Oops", "GetQueueUrl")
    assert "error" in boto3_sqs.get_attributes("q")


@pytest.mark.usefixtures("queue_url")
def test_set_attributes(conn):
    assert boto3_sqs.set_attributes("q", {"DelaySeconds": 20}) == {"result": True}
    conn.set_queue_attributes.assert_called_once_with(
        QueueUrl="http://x/1/q", Attributes={"DelaySeconds": 20}
    )


def test_set_attributes_client_error(conn, client_error):
    conn.get_queue_url.side_effect = client_error("Oops", "GetQueueUrl")
    assert "error" in boto3_sqs.set_attributes("q", {"DelaySeconds": 20})
