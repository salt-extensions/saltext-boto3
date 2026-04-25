"""
Connection module for Amazon SQS using boto3.
=============================================

    Renamed from ``boto_sqs`` to ``boto3_sqs`` and rewritten to use the
    boto3 ``sqs`` client APIs directly via
    :py:mod:`saltext.boto3.utils.boto3mod`.  The legacy boto2 code path
    (object-style access, retry loops) has been removed.

:depends:
  - boto3 >= 1.28.0
  - botocore >= 1.31.0

:configuration: This module accepts explicit SQS credentials but can
    also utilize IAM roles assigned to the instance through Instance Profiles.
    Dynamic credentials are then automatically obtained from AWS API and no
    further configuration is necessary. More Information available at:

    .. code-block:: text

        http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html

    If IAM roles are not used you need to specify them either in the minion's
    config file or as a profile. For example, to specify them in the minion's
    config file:

.. code-block:: yaml

    sqs.keyid: GKTADJGHEIQSXMKKRBJ08H
    sqs.key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs

A region may also be specified in the configuration:

.. code-block:: yaml

    sqs.region: us-east-1

It's also possible to specify key, keyid and region via a profile, either
as a passed in dict, or as a string to pull from pillars or minion config:

.. code-block:: yaml

    myprofile:
        keyid: GKTADJGHEIQSXMKKRBJ08H
        key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs
        region: us-east-1

.. versionadded:: 1.0.0
"""

import logging
import urllib.parse

import salt.utils.json

from saltext.boto3.utils import boto3mod

log = logging.getLogger(__name__)

__func_alias__ = {
    "list_": "list",
}

try:
    from botocore.exceptions import ClientError

    logging.getLogger("boto3").setLevel(logging.CRITICAL)
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

__virtualname__ = "boto3_sqs"


def __virtual__():
    """
    Only load if boto3 is available.
    """
    if HAS_BOTO3:
        return __virtualname__
    return (False, "The boto3_sqs module could not be loaded: boto3 is not available.")


def _get_conn(service, region=None, key=None, keyid=None, profile=None):
    """
    Return a boto3 client for ``service`` using this module's dunders.
    """
    return boto3mod.get_connection(
        service,
        opts=__opts__,
        context=__context__,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )


def _preprocess_attributes(attributes):
    """
    Pre-process incoming queue attributes before setting them.
    """
    if isinstance(attributes, str):
        attributes = salt.utils.json.loads(attributes)

    def stringified(val):
        if isinstance(val, dict):
            return salt.utils.json.dumps(val)
        return val

    return {attr: stringified(val) for attr, val in attributes.items()}


def exists(name, region=None, key=None, keyid=None, profile=None):
    """
    Check to see if a queue exists.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_sqs.exists myqueue region=us-east-1
    """
    conn = _get_conn("sqs", region=region, key=key, keyid=keyid, profile=profile)

    try:
        conn.get_queue_url(QueueName=name)
    except ClientError as e:
        missing_code = "AWS.SimpleQueueService.NonExistentQueue"
        if e.response.get("Error", {}).get("Code") == missing_code:
            return {"result": False}
        return {"error": boto3mod.get_error(e)}
    return {"result": True}


def create(
    name,
    attributes=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Create an SQS queue.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_sqs.create myqueue region=us-east-1
    """
    conn = _get_conn("sqs", region=region, key=key, keyid=keyid, profile=profile)

    if attributes is None:
        attributes = {}
    attributes = _preprocess_attributes(attributes)

    try:
        conn.create_queue(QueueName=name, Attributes=attributes)
    except ClientError as e:
        return {"error": boto3mod.get_error(e)}
    return {"result": True}


def delete(name, region=None, key=None, keyid=None, profile=None):
    """
    Delete an SQS queue.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_sqs.delete myqueue region=us-east-1
    """
    conn = _get_conn("sqs", region=region, key=key, keyid=keyid, profile=profile)

    try:
        url = conn.get_queue_url(QueueName=name)["QueueUrl"]
        conn.delete_queue(QueueUrl=url)
    except ClientError as e:
        return {"error": boto3mod.get_error(e)}
    return {"result": True}


def list_(prefix="", region=None, key=None, keyid=None, profile=None):
    """
    Return a list of the names of all visible queues.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_sqs.list region=us-east-1
    """
    conn = _get_conn("sqs", region=region, key=key, keyid=keyid, profile=profile)

    def extract_name(queue_url):
        return urllib.parse.urlparse(queue_url).path.split("/")[2]

    try:
        r = conn.list_queues(QueueNamePrefix=prefix)
        urls = r.get("QueueUrls", [])
        return {"result": [extract_name(url) for url in urls]}
    except ClientError as e:
        return {"error": boto3mod.get_error(e)}


def get_attributes(name, region=None, key=None, keyid=None, profile=None):
    """
    Return attributes currently set on an SQS queue.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_sqs.get_attributes myqueue
    """
    conn = _get_conn("sqs", region=region, key=key, keyid=keyid, profile=profile)

    try:
        url = conn.get_queue_url(QueueName=name)["QueueUrl"]
        r = conn.get_queue_attributes(QueueUrl=url, AttributeNames=["All"])
        return {"result": r["Attributes"]}
    except ClientError as e:
        return {"error": boto3mod.get_error(e)}


def set_attributes(
    name,
    attributes,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Set attributes on an SQS queue.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_sqs.set_attributes myqueue '{ReceiveMessageWaitTimeSeconds: 20}' region=us-east-1
    """
    conn = _get_conn("sqs", region=region, key=key, keyid=keyid, profile=profile)

    attributes = _preprocess_attributes(attributes)

    try:
        url = conn.get_queue_url(QueueName=name)["QueueUrl"]
        conn.set_queue_attributes(QueueUrl=url, Attributes=attributes)
    except ClientError as e:
        return {"error": boto3mod.get_error(e)}
    return {"result": True}
