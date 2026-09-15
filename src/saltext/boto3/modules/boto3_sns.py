"""
Connection module for Amazon SNS using boto3.
=============================================

:depends:
  - boto3 >= 1.28.0
  - botocore >= 1.31.0

:configuration: This module accepts explicit SNS credentials but can
    also utilize IAM roles assigned to the instance through Instance Profiles.
    Dynamic credentials are then automatically obtained from AWS API and no
    further configuration is necessary. More Information available at:

    .. code-block:: text

        http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html

    If IAM roles are not used you need to specify them either in the minion's
    config file or as a profile. For example, to specify them in the minion's
    config file:

.. code-block:: yaml

    sns.keyid: GKTADJGHEIQSXMKKRBJ08H
    sns.key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs

A region may also be specified in the configuration:

.. code-block:: yaml

    sns.region: us-east-1

It's also possible to specify key, keyid and region via a profile, either
as a passed in dict, or as a string to pull from pillars or minion config:

.. code-block:: yaml

    myprofile:
        keyid: GKTADJGHEIQSXMKKRBJ08H
        key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs
        region: us-east-1

.. versionadded:: 1.0.0
"""

# keep lint from choking on _get_conn and _cache_id
# pylint: disable=E0602


import logging

from saltext.boto3.utils import boto3mod

log = logging.getLogger(__name__)

try:
    import botocore
    import jmespath

    logging.getLogger("boto3").setLevel(logging.CRITICAL)
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO = False

__virtualname__ = "boto3_sns"


def __virtual__():
    """
    Only load if boto3 is available. Minimum version is enforced via the
    project's ``pyproject.toml`` dependency declaration.
    """
    if HAS_BOTO3:
        return __virtualname__
    return (False, "The boto3_sns module could not be loaded: boto3 is not available.")


def get_arn(name, region=None, profile=None):
    """
    Return the ARN for an SNS topic. If ``name`` already looks like an ARN it
    is returned unchanged, otherwise an ARN is built using the given
    ``region`` (or ``profile["region"]``, or the ``sns.region`` minion config
    option, falling back to ``us-east-1``) and the AWS account ID obtained
    from :py:func:`boto3_iam.get_account_id`.

    name (str):
        The name of the SNS topic. Can be either the short name or the full ARN.

    region (str, optional):
        The AWS region to use when constructing the ARN if ``name`` is not already an ARN.

    profile (str, optional):
        The AWS profile to use when constructing the ARN if ``name`` is not already an ARN.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_sns.get_arn mytopic
    """
    if isinstance(name, str) and name.startswith("arn:aws:sns:"):
        return name
    if region is None and isinstance(profile, dict):
        region = profile.get("region")
    if region is None:
        region = __salt__["config.option"]("sns.region") or "us-east-1"
    account_id = __salt__["boto3_iam.get_account_id"]()
    return f"arn:aws:sns:{region}:{account_id}:{name}"


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


def list_topics(region=None, key=None, keyid=None, profile=None):
    """
    Returns a list of the requester's topics

    region (str, optional):
        The AWS region to use when listing topics. If not specified, the default region is used.

    key (str, optional):
        The AWS access key to use when listing topics.

    keyid (str, optional):
        The AWS secret key to use when listing topics.

    profile (str, optional):
        The AWS profile to use when listing topics.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_sns.list_topics
    """
    conn = _get_conn("sns", region=region, key=key, keyid=keyid, profile=profile)
    res = {}
    NextToken = ""
    try:
        while NextToken is not None:
            ret = conn.list_topics(NextToken=NextToken)
            NextToken = ret.get("NextToken", None)
            arns = jmespath.search("Topics[*].TopicArn", ret)
            for t in arns:
                short_name = t.split(":")[-1]
                res[short_name] = t
    except botocore.exceptions.ClientError as e:
        log.error("Failed to list SNS topics: %s", e)
        return None
    return res


def describe_topic(name, region=None, key=None, keyid=None, profile=None):
    """
    Returns details about a specific SNS topic, specified by name or ARN.

    name (str):
        The name or ARN of the SNS topic to describe.

    region (str, optional):
        The AWS region to use when describing the topic. If not specified, the default region is used.

    key (str, optional):
        The AWS access key to use when describing the topic.

    keyid (str, optional):
        The AWS secret key to use when describing the topic.

    profile (str, optional):
        The AWS profile to use when describing the topic.

    CLI Example:

    .. code-block:: bash

        salt my_favorite_client boto3_sns.describe_topic a_sns_topic_of_my_choice
    """
    topics = list_topics(region=region, key=key, keyid=keyid, profile=profile)
    if not topics:
        return {}
    ret = {}
    for topic, arn in topics.items():
        if name not in (topic, arn):
            continue

        subscriptions = list_subscriptions_by_topic(
            arn, region=region, key=key, keyid=keyid, profile=profile
        )
        if isinstance(subscriptions, dict):
            subscriptions = subscriptions.get("Subscriptions", [])

        valid_subscriptions = []
        for sub in subscriptions or []:
            sub_arn = sub.get("SubscriptionArn")
            if not sub_arn or not sub_arn.startswith("arn:aws:sns:"):
                # Sometimes a subscription is in PendingAccept or other
                # transitional states and does not yet have a valid ARN.
                log.debug("Subscription with invalid ARN %s skipped...", sub_arn)
                continue
            valid_subscriptions.append(sub)

        ret = {
            "TopicArn": arn,
            "Subscriptions": valid_subscriptions,
            "Attributes": get_topic_attributes(
                arn, region=region, key=key, keyid=keyid, profile=profile
            ),
        }
        break
    return ret


def topic_exists(name, region=None, key=None, keyid=None, profile=None):
    """
    Check to see if an SNS topic exists.

    name (str):
        The name or ARN of the SNS topic to check for existence.

    region (str, optional):
        The AWS region to use when checking for the topic's existence. If not specified, the default region is used.

    key (str, optional):
        The AWS access key to use when checking for the topic's existence.

    keyid (str, optional):
        The AWS secret key to use when checking for the topic's existence.

    profile (str, optional):
        The AWS profile to use when checking for the topic's existence.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_sns.topic_exists mytopic region=us-east-1
    """
    topics = list_topics(region=region, key=key, keyid=keyid, profile=profile)
    if not topics:
        return False
    return name in list(topics.values()) + list(topics)


def create_topic(Name, region=None, key=None, keyid=None, profile=None):
    """
    Create an SNS topic.

    Name (str):
        The name of the SNS topic to create.

    region (str, optional):
        The AWS region to use when creating the topic. If not specified, the default region is used.

    key (str, optional):
        The AWS access key to use when creating the topic.

    keyid (str, optional):
        The AWS secret key to use when creating the topic.

    profile (str, optional):
        The AWS profile to use when creating the topic.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_sns.create_topic mytopic region=us-east-1
    """
    conn = _get_conn("sns", region=region, key=key, keyid=keyid, profile=profile)
    try:
        ret = conn.create_topic(Name=Name)
        log.info("SNS topic %s created with ARN %s", Name, ret["TopicArn"])
        return ret["TopicArn"]
    except botocore.exceptions.ClientError as e:
        log.error("Failed to create SNS topic %s: %s", Name, e)
        return None
    except KeyError:
        log.error("Failed to create SNS topic %s", Name)
        return None


def delete_topic(TopicArn, region=None, key=None, keyid=None, profile=None):
    """
    Delete an SNS topic.

    TopicArn (str):
        The ARN of the SNS topic to delete.

    region (str, optional):
        The AWS region to use when deleting the topic. If not specified, the default region is used.

    key (str, optional):
        The AWS access key to use when deleting the topic.

    keyid (str, optional):
        The AWS secret key to use when deleting the topic.

    profile (str, optional):
        The AWS profile to use when deleting the topic.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_sns.delete_topic mytopic region=us-east-1
    """
    conn = _get_conn("sns", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.delete_topic(TopicArn=TopicArn)
        log.info("SNS topic %s deleted", TopicArn)
        return True
    except botocore.exceptions.ClientError as e:
        log.error("Failed to delete SNS topic %s: %s", TopicArn, e)
        return False


def get_topic_attributes(TopicArn, region=None, key=None, keyid=None, profile=None):
    """
    Returns all of the properties of a topic.  Topic properties returned might differ based on the
    authorization of the user.

    TopicArn (str):
        The ARN of the SNS topic whose attributes are to be retrieved.

    region (str, optional):
        The AWS region to use when retrieving the topic attributes. If not specified, the default region is used.

    key (str, optional):
        The AWS access key to use when retrieving the topic attributes.

    keyid (str, optional):
        The AWS secret key to use when retrieving the topic attributes.

    profile (str, optional):
        The AWS profile to use when retrieving the topic attributes.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_sns.get_topic_attributes someTopic region=us-west-1
    """
    conn = _get_conn("sns", region=region, key=key, keyid=keyid, profile=profile)
    try:
        return conn.get_topic_attributes(TopicArn=TopicArn).get("Attributes")
    except botocore.exceptions.ClientError as e:
        log.error("Failed to garner attributes for SNS topic %s: %s", TopicArn, e)
        return None


def set_topic_attributes(
    TopicArn,
    AttributeName,
    AttributeValue,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Set an attribute of a topic to a new value.

    TopicArn (str):
        The ARN of the SNS topic whose attribute is to be set.

    AttributeName (str):
        The name of the attribute to set.

    AttributeValue (str):
        The new value for the attribute.

    region (str, optional):
        The AWS region to use when setting the topic attribute. If not specified, the default region is used.

    key (str, optional):
        The AWS access key to use when setting the topic attribute.

    keyid (str, optional):
        The AWS secret key to use when setting the topic attribute.

    profile (str, optional):
        The AWS profile to use when setting the topic attribute.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_sns.set_topic_attributes someTopic DisplayName myDisplayNameValue
    """
    conn = _get_conn("sns", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.set_topic_attributes(
            TopicArn=TopicArn,
            AttributeName=AttributeName,
            AttributeValue=AttributeValue,
        )
        log.debug(
            "Set attribute %s=%s on SNS topic %s",
            AttributeName,
            AttributeValue,
            TopicArn,
        )
        return True
    except botocore.exceptions.ClientError as e:
        log.error(
            "Failed to set attribute %s=%s for SNS topic %s: %s",
            AttributeName,
            AttributeValue,
            TopicArn,
            e,
        )
        return False


def list_subscriptions_by_topic(TopicArn, region=None, key=None, keyid=None, profile=None):
    """
    Returns a list of the subscriptions to a specific topic

    TopicArn (str):
        The ARN of the SNS topic whose subscriptions are to be listed.

    region (str, optional):
        The AWS region to use when listing the subscriptions. If not specified, the default region is used.

    key (str, optional):
        The AWS access key to use when listing the subscriptions.

    keyid (str, optional):
        The AWS secret key to use when listing the subscriptions.

    profile (str, optional):
        The AWS profile to use when listing the subscriptions.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_sns.list_subscriptions_by_topic mytopic region=us-east-1
    """
    conn = _get_conn("sns", region=region, key=key, keyid=keyid, profile=profile)
    NextToken = ""
    res = []
    try:
        while NextToken is not None:
            ret = conn.list_subscriptions_by_topic(TopicArn=TopicArn, NextToken=NextToken)
            NextToken = ret.get("NextToken", None)
            subs = ret.get("Subscriptions", [])
            res += subs
    except botocore.exceptions.ClientError as e:
        log.error("Failed to list subscriptions for SNS topic %s: %s", TopicArn, e)
        return None
    return res


def list_subscriptions(region=None, key=None, keyid=None, profile=None):
    """
    Returns a list of the requester's subscriptions to all topics.

    region (str, optional):
        The AWS region to use when listing the subscriptions. If not specified, the default region is used.

    key (str, optional):
        The AWS access key to use when listing the subscriptions.

    keyid (str, optional):
        The AWS secret key to use when listing the subscriptions.

    profile (str, optional):
        The AWS profile to use when listing the subscriptions.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_sns.list_subscriptions region=us-east-1
    """
    conn = _get_conn("sns", region=region, key=key, keyid=keyid, profile=profile)
    NextToken = ""
    res = []
    try:
        while NextToken is not None:
            ret = conn.list_subscriptions(NextToken=NextToken)
            NextToken = ret.get("NextToken", None)
            subs = ret.get("Subscriptions", [])
            res += subs
    except botocore.exceptions.ClientError as e:
        log.error("Failed to list SNS subscriptions: %s", e)
        return None
    return res


def get_subscription_attributes(SubscriptionArn, region=None, key=None, keyid=None, profile=None):
    """
    Returns all of the properties of a subscription.

    SubscriptionArn (str):
        The ARN of the SNS subscription whose attributes are to be retrieved.

    region (str, optional):
        The AWS region to use when retrieving the subscription attributes. If not specified, the default region is used.

    key (str, optional):
        The AWS access key to use when retrieving the subscription attributes.

    keyid (str, optional):
        The AWS secret key to use when retrieving the subscription attributes.

    profile (str, optional):
        The AWS profile to use when retrieving the subscription attributes.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_sns.get_subscription_attributes somesubscription region=us-west-1
    """
    conn = _get_conn("sns", region=region, key=key, keyid=keyid, profile=profile)
    try:
        ret = conn.get_subscription_attributes(SubscriptionArn=SubscriptionArn)
        return ret["Attributes"]
    except botocore.exceptions.ClientError as e:
        log.error("Failed to list attributes for SNS subscription %s: %s", SubscriptionArn, e)
        return None
    except KeyError:
        log.error("Failed to list attributes for SNS subscription %s", SubscriptionArn)
        return None


def set_subscription_attributes(
    SubscriptionArn,
    AttributeName,
    AttributeValue,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Set an attribute of a subscription to a new value.

    SubscriptionArn (str):
        The ARN of the SNS subscription whose attribute is to be set.

    AttributeName (str):
        The name of the attribute to set.

    AttributeValue (str):
        The new value for the attribute.

    region (str, optional):
        The AWS region to use when setting the subscription attribute. If not specified, the default region is used.

    key (str, optional):
        The AWS access key to use when setting the subscription attribute.

    keyid (str, optional):
        The AWS secret key to use when setting the subscription attribute.

    profile (str, optional):
        The AWS profile to use when setting the subscription attribute.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_sns.set_subscription_attributes someSubscription RawMessageDelivery jsonStringValue
    """
    conn = _get_conn("sns", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.set_subscription_attributes(
            SubscriptionArn=SubscriptionArn,
            AttributeName=AttributeName,
            AttributeValue=AttributeValue,
        )
        log.debug(
            "Set attribute %s=%s on SNS subscription %s",
            AttributeName,
            AttributeValue,
            SubscriptionArn,
        )
        return True
    except botocore.exceptions.ClientError as e:
        log.error(
            "Failed to set attribute %s=%s for SNS subscription %s: %s",
            AttributeName,
            AttributeValue,
            SubscriptionArn,
            e,
        )
        return False


def subscribe(TopicArn, Protocol, Endpoint, region=None, key=None, keyid=None, profile=None):
    """
    Subscribe to a Topic.

    TopicArn (str):
        The ARN of the SNS topic to subscribe to.

    Protocol (str):
        The protocol to use for the subscription (e.g., "https", "email", "sms").

    Endpoint (str):
        The endpoint to receive notifications (e.g., an HTTPS URL, an email address, or a phone number).

    region (str, optional):
        The AWS region to use when creating the subscription. If not specified, the default region is used.

    key (str, optional):
        The AWS access key to use when creating the subscription.

    keyid (str, optional):
        The AWS secret key to use when creating the subscription.

    profile (str, optional):
        The AWS profile to use when creating the subscription.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_sns.subscribe mytopic https https://www.example.com/sns-endpoint
    """
    conn = _get_conn("sns", region=region, key=key, keyid=keyid, profile=profile)
    try:
        ret = conn.subscribe(TopicArn=TopicArn, Protocol=Protocol, Endpoint=Endpoint)
        log.info(
            "Subscribed %s %s to topic %s with SubscriptionArn %s",
            Protocol,
            Endpoint,
            TopicArn,
            ret["SubscriptionArn"],
        )
        return ret["SubscriptionArn"]
    except botocore.exceptions.ClientError as e:
        log.error("Failed to create subscription to SNS topic %s: %s", TopicArn, e)
        return None
    except KeyError:
        log.error("Failed to create subscription to SNS topic %s", TopicArn)
        return None


def unsubscribe(SubscriptionArn, region=None, key=None, keyid=None, profile=None):
    """
    Unsubscribe a specific SubscriptionArn of a topic.

    SubscriptionArn (str):
        The ARN of the SNS subscription to unsubscribe from.

    region (str, optional):
        The AWS region to use when unsubscribing. If not specified, the default region is used.

    key (str, optional):
        The AWS access key to use when unsubscribing.

    keyid (str, optional):
        The AWS secret key to use when unsubscribing.

    profile (str, optional):
        The AWS profile to use when unsubscribing.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_sns.unsubscribe my_subscription_arn region=us-east-1
    """
    if not SubscriptionArn.startswith("arn:aws:sns:"):
        # Grrr, AWS sent us an ARN that's NOT and ARN....
        # This can happen if, for instance, a subscription is left in PendingAcceptance or similar
        # Note that anything left in PendingConfirmation will be auto-deleted by AWS after 30 days
        # anyway, so this isn't as ugly a hack as it might seem at first...
        log.info(
            "Invalid subscription ARN `%s` passed - likely a PendingConfirmaton or"
            " such.  Skipping unsubscribe attempt as it would almost certainly fail...",
            SubscriptionArn,
        )
        return True
    subs = list_subscriptions(region=region, key=key, keyid=keyid, profile=profile)
    sub = [s for s in subs if s.get("SubscriptionArn") == SubscriptionArn]
    if not sub:
        log.error("Subscription ARN %s not found", SubscriptionArn)
        return False
    TopicArn = sub[0]["TopicArn"]
    conn = _get_conn("sns", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.unsubscribe(SubscriptionArn=SubscriptionArn)
        log.info("Deleted subscription %s from SNS topic %s", SubscriptionArn, TopicArn)
        return True
    except botocore.exceptions.ClientError as e:
        log.error("Failed to delete subscription %s: %s", SubscriptionArn, e)
        return False
