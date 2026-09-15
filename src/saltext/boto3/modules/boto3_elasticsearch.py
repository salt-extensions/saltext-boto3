"""
Connection module for Amazon Elasticsearch Service using boto3.
===============================================================

:depends:
  - boto3 >= 1.28.0
  - botocore >= 1.31.0

:configuration: This module accepts explicit IAM credentials but can also
    utilize IAM roles assigned to the instance trough Instance Profiles.
    Dynamic credentials are then automatically obtained from AWS API and no
    further configuration is necessary. More Information available at:

    .. code-block:: text

        http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html

    If IAM roles are not used you need to specify them either in the minion's
    config file or as a profile. For example, to specify them in the minion's
    config file:

    .. code-block:: yaml

        es.keyid: GKTADJGHEIQSXMKKRBJ08H
        es.key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs

    A region may also be specified in the configuration:

    .. code-block:: yaml

        es.region: us-east-1

    If a region is not specified, the default is us-east-1.

    It's also possible to specify key, keyid and region via a profile, either
    as a passed in dict, or as a string to pull from pillars or minion config:

    .. code-block:: yaml

        myprofile:
            keyid: GKTADJGHEIQSXMKKRBJ08H
            key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs
            region: us-east-1

    All methods return a dict with:
        'result' key containing a boolean indicating success or failure,
        'error' key containing the errormessage returned by boto on error,
        'response' key containing the data of the response returned by boto on success.

:codeauthor: Herbert Buurman <herbert.buurman@ogd.nl>

.. versionadded:: 1.0.0
"""

# keep lint from choking on _get_conn and _cache_id
# pylint: disable=E0602


import logging

import salt.utils.json
from salt.exceptions import SaltInvocationError
from salt.utils.decorators import depends

from saltext.boto3.utils import boto3mod

try:
    from botocore.exceptions import ClientError
    from botocore.exceptions import ParamValidationError
    from botocore.exceptions import WaiterError

    logging.getLogger("boto3").setLevel(logging.INFO)
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

log = logging.getLogger(__name__)

__virtualname__ = "boto3_elasticsearch"


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


def __virtual__():
    """
    Only load if boto3 is available. Minimum version is enforced via the
    project's ``pyproject.toml`` dependency declaration.
    """
    if HAS_BOTO3:
        return __virtualname__
    return (False, "The boto3_elasticsearch module could not be loaded: boto3 is not available.")


def add_tags(
    domain_name=None,
    arn=None,
    tags=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Attaches tags to an existing Elasticsearch domain.
    Tags are a set of case-sensitive key value pairs.
    An Elasticsearch domain may have up to 10 tags.

    domain_name (str, optional):
        The name of the Elasticsearch domain you want to add tags to.

    arn (str, optional):
        The ARN of the Elasticsearch domain you want to add tags to.
        Specifying this overrides ``domain_name``.

    tags (dict, optional):
        The dict of tags to add to the Elasticsearch domain.

    region (str, optional):
        The AWS region where the Elasticsearch domain is located.

    keyid (str, optional):
        The AWS access key ID.

    key (str, optional):
        The AWS secret access key.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_elasticsearch.add_tags domain_name=mydomain tags='{"foo": "bar", "baz": "qux"}'
    """
    if not any((arn, domain_name)):
        raise SaltInvocationError("At least one of domain_name or arn must be specified.")
    ret = {"result": False}
    if arn is None:
        res = describe_elasticsearch_domain(
            domain_name=domain_name,
            region=region,
            key=key,
            keyid=keyid,
            profile=profile,
        )
        if "error" in res:
            ret.update(res)
        elif not res["result"]:
            ret.update({"error": f'The domain with name "{domain_name}" does not exist.'})
        else:
            arn = res["response"].get("ARN")
    if arn:
        boto_params = {
            "ARN": arn,
            "TagList": [{"Key": k, "Value": value} for k, value in (tags or {}).items()],
        }
        try:
            conn = _get_conn("es", region=region, key=key, keyid=keyid, profile=profile)
            conn.add_tags(**boto_params)
            ret["result"] = True
        except (ParamValidationError, ClientError) as exp:
            ret.update({"error": boto3mod.get_error(exp)["message"]})
    return ret


@depends("botocore", version="1.12.21")
def cancel_elasticsearch_service_software_update(
    domain_name, region=None, keyid=None, key=None, profile=None
):
    """
    Cancels a scheduled service software update for an Amazon ES domain. You can
    only perform this operation before the AutomatedUpdateDate and when the UpdateStatus
    is in the PENDING_UPDATE state.

    domain_name (str):
        The name of the Elasticsearch domain for which to cancel the scheduled service software update.

    region (str, optional):
        The AWS region where the Elasticsearch domain is located.

    keyid (str, optional):
        The AWS access key ID.

    key (str, optional):
        The AWS secret access key.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_elasticsearch.cancel_elasticsearch_service_software_update domain_name=mydomain

    """
    ret = {"result": False}
    try:
        conn = _get_conn("es", region=region, keyid=keyid, key=key, profile=profile)
        res = conn.cancel_elasticsearch_service_software_update(DomainName=domain_name)
        ret["result"] = True
        res["response"] = res["ServiceSoftwareOptions"]
    except (ParamValidationError, ClientError) as exp:
        ret.update({"error": boto3mod.get_error(exp)["message"]})
    return ret


def create_elasticsearch_domain(
    domain_name,
    elasticsearch_version=None,
    elasticsearch_cluster_config=None,
    ebs_options=None,
    access_policies=None,
    snapshot_options=None,
    vpc_options=None,
    cognito_options=None,
    encryption_at_rest_options=None,
    node_to_node_encryption_options=None,
    advanced_options=None,
    log_publishing_options=None,
    blocking=False,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Given a valid config, create a domain.

    domain_name (str):
        The name of the Elasticsearch domain to create.

    elasticsearch_version (str, optional):
        The version of Elasticsearch to use for the domain.

    elasticsearch_cluster_config (dict, optional):
        The configuration for the Elasticsearch cluster.

        Example:

        .. code-block:: json

            {
              "InstanceType": "t2.micro.elasticsearch",
              "InstanceCount": 1,
              "DedicatedMasterEnabled": False,
              "ZoneAwarenessEnabled": False
            }

    ebs_options (dict, optional):
        The EBS options for the domain.

        Example:

        .. code-block:: json

            {
              "EBSEnabled": True,
              "VolumeType": "gp2",
              "VolumeSize": 10,
              "Iops": 0
            }

    access_policies (dict, optional):
        The access policies for the domain.

        Example:

        .. code-block:: json

            {
              "Version": "2012-10-17",
              "Statement": [
                {
                  "Effect": "Allow",
                  "Principal": {"AWS": "*"},
                  "Action": "es:*",
                  "Resource": "arn:aws:es:us-east-1:111111111111:domain/mydomain/*",
                  "Condition": {"IpAddress": {"aws:SourceIp": ["127.0.0.1"]}}
                }
              ]
            }

    snapshot_options (dict, optional):
        The snapshot options for the domain.

        Example:

        .. code-block:: json

            {
              "AutomatedSnapshotStartHour": 0
            }

    vpc_options (dict, optional):
        The VPC options for the domain.

        Example:

        .. code-block:: json

            {
              "SubnetIds": ["subnet-12345678"],
              "SecurityGroupIds": ["sg-12345678"]
            }

    cognito_options (dict, optional):
        The Cognito options for the domain.

        Example:

        .. code-block:: json

            {
              "Enabled": True,
              "UserPoolId": "us-east-1_123456789",
              "IdentityPoolId": "us-east-1:12345678-1234-1234-1234-123456789012",
              "RoleArn": "arn:aws:iam::111111111111:role/CognitoAccessRole"
            }

    encryption_at_rest_options (dict, optional):
        The encryption at rest options for the domain.

        Example:

        .. code-block:: json

            {
              "Enabled": True,
              "KmsKeyId": "arn:aws:kms:us-east-1:111111111111:key/12345678-1234-1234-1234-123456789012"
            }

    node_to_node_encryption_options (dict, optional):
        The node-to-node encryption options for the domain.

        Example:

        .. code-block:: json

            {
              "Enabled": True
            }

    advanced_options (dict, optional):
        The advanced options for the domain.

        Example:

        .. code-block:: json

            {
              "rest.action.multi.allow_explicit_index": "true"
            }

    log_publishing_options (dict, optional):
        The log publishing options for the domain.

        Example:

        .. code-block:: json

            {
              "INDEX_SLOW_LOGS": {
                "CloudWatchLogsLogGroupArn": "arn:aws:logs:us-east-1:111111111111:log-group:my-log-group",
                "Enabled": True
              },
              "SEARCH_SLOW_LOGS": {
                "CloudWatchLogsLogGroupArn": "arn:aws:logs:us-east-1:111111111111:log-group:my-log-group",
                "Enabled": True
              },
              "ES_APPLICATION_LOGS": {
                "CloudWatchLogsLogGroupArn": "arn:aws:logs:us-east-1:111111111111:log-group:my-log-group",
                "Enabled": True
              }
            }


    blocking (bool, optional):
        Whether to block until the domain is available.

    region (str, optional):
        The AWS region where the Elasticsearch domain is located.

    keyid (str, optional):
        The AWS access key ID.

    key (str, optional):
        The AWS secret access key.

    profile (str, optional):
        The profile to use for AWS credentials.

    .. note::
        Not all instance types allow enabling encryption at rest. See https://docs.aws.amazon.com\
        /elasticsearch-service/latest/developerguide/aes-supported-instance-types.html

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_elasticsearch.create_elasticsearch_domain mydomain \
        elasticsearch_cluster_config='{ \
          "InstanceType": "t2.micro.elasticsearch", \
          "InstanceCount": 1, \
          "DedicatedMasterEnabled": False, \
          "ZoneAwarenessEnabled": False}' \
        ebs_options='{ \
          "EBSEnabled": True, \
          "VolumeType": "gp2", \
          "VolumeSize": 10, \
          "Iops": 0}' \
        access_policies='{ \
          "Version": "2012-10-17", \
          "Statement": [ \
            {"Effect": "Allow", \
             "Principal": {"AWS": "*"}, \
             "Action": "es:*", \
             "Resource": "arn:aws:es:us-east-1:111111111111:domain/mydomain/*", \
             "Condition": {"IpAddress": {"aws:SourceIp": ["127.0.0.1"]}}}]}' \
        snapshot_options='{"AutomatedSnapshotStartHour": 0}' \
        advanced_options='{"rest.action.multi.allow_explicit_index": "true"}'
    """
    boto_kwargs = salt.utils.data.filter_falsey(
        {
            "DomainName": domain_name,
            "ElasticsearchVersion": str(elasticsearch_version or ""),
            "ElasticsearchClusterConfig": elasticsearch_cluster_config,
            "EBSOptions": ebs_options,
            "AccessPolicies": (
                salt.utils.json.dumps(access_policies)
                if isinstance(access_policies, dict)
                else access_policies
            ),
            "SnapshotOptions": snapshot_options,
            "VPCOptions": vpc_options,
            "CognitoOptions": cognito_options,
            "EncryptionAtRestOptions": encryption_at_rest_options,
            "NodeToNodeEncryptionOptions": node_to_node_encryption_options,
            "AdvancedOptions": advanced_options,
            "LogPublishingOptions": log_publishing_options,
        }
    )
    ret = {"result": False}
    try:
        conn = _get_conn("es", region=region, key=key, keyid=keyid, profile=profile)
        res = conn.create_elasticsearch_domain(**boto_kwargs)
        if res and "DomainStatus" in res:
            ret["result"] = True
            ret["response"] = res["DomainStatus"]
        if blocking:
            conn.get_waiter("ESDomainAvailable").wait(DomainName=domain_name)
    except (ParamValidationError, ClientError, WaiterError) as exp:
        ret.update({"error": boto3mod.get_error(exp)["message"]})
    return ret


def delete_elasticsearch_domain(
    domain_name, blocking=False, region=None, key=None, keyid=None, profile=None
):
    """
    Permanently deletes the specified Elasticsearch domain and all of its data.
    Once a domain is deleted, it cannot be recovered.

    domain_name (str):
        The name of the domain to delete.

    blocking (bool, optional):
        Whether or not to wait (block) until the Elasticsearch
        domain has been deleted.

    region (str, optional):
        The AWS region where the Elasticsearch domain is located.

    keyid (str, optional):
        The AWS access key ID.

    key (str, optional):
        The AWS secret access key.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_elasticsearch.delete_elasticsearch_domain domain_name="my-domain"

    """
    ret = {"result": False}
    try:
        conn = _get_conn("es", region=region, key=key, keyid=keyid, profile=profile)
        conn.delete_elasticsearch_domain(DomainName=domain_name)
        ret["result"] = True
        if blocking:
            conn.get_waiter("ESDomainDeleted").wait(DomainName=domain_name)
    except (ParamValidationError, ClientError, WaiterError) as exp:
        ret.update({"error": boto3mod.get_error(exp)["message"]})
    return ret


@depends("botocore", version="1.7.30")
def delete_elasticsearch_service_role(region=None, keyid=None, key=None, profile=None):
    """
    Deletes the service-linked role that Elasticsearch Service uses to manage and
    maintain VPC domains. Role deletion will fail if any existing VPC domains use
    the role. You must delete any such Elasticsearch domains before deleting the role.

    region (str, optional):
        The AWS region where the Elasticsearch service role is located.

    keyid (str, optional):
        The AWS access key ID.

    key (str, optional):
        The AWS secret access key.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_elasticsearch.delete_elasticsearch_service_role

    """
    ret = {"result": False}
    try:
        conn = _get_conn("es", region=region, keyid=keyid, key=key, profile=profile)
        conn.delete_elasticsearch_service_role()
        ret["result"] = True
    except (ParamValidationError, ClientError) as exp:
        ret.update({"error": boto3mod.get_error(exp)["message"]})
    return ret


def describe_elasticsearch_domain(domain_name, region=None, keyid=None, key=None, profile=None):
    """
    Given a domain name gets its status description.

    domain_name (str):
        The name of the domain to get the status of.

    region (str, optional):
        The AWS region where the Elasticsearch domain is located.

    keyid (str, optional):
        The AWS access key ID.

    key (str, optional):
        The AWS secret access key.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_elasticsearch.describe_elasticsearch_domain domain_name="my-domain"

    """
    ret = {"result": False}
    try:
        conn = _get_conn("es", region=region, key=key, keyid=keyid, profile=profile)
        res = conn.describe_elasticsearch_domain(DomainName=domain_name)
        if res and "DomainStatus" in res:
            ret["result"] = True
            ret["response"] = res["DomainStatus"]
    except (ParamValidationError, ClientError) as exp:
        ret.update({"error": boto3mod.get_error(exp)["message"]})
    return ret


def describe_elasticsearch_domain_config(
    domain_name, region=None, keyid=None, key=None, profile=None
):
    """
    Provides cluster configuration information about the specified Elasticsearch domain,
    such as the state, creation date, update version, and update date for cluster options.

    domain_name (str):
        The name of the domain to describe.

    region (str, optional):
        The AWS region where the Elasticsearch domain is located.

    keyid (str, optional):
        The AWS access key ID.

    key (str, optional):
        The AWS secret access key.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_elasticsearch.describe_elasticsearch_domain_config domain_name="my-domain"

    """
    ret = {"result": False}
    try:
        conn = _get_conn("es", region=region, key=key, keyid=keyid, profile=profile)
        res = conn.describe_elasticsearch_domain_config(DomainName=domain_name)
        if res and "DomainConfig" in res:
            ret["result"] = True
            ret["response"] = res["DomainConfig"]
    except (ParamValidationError, ClientError) as exp:
        ret.update({"error": boto3mod.get_error(exp)["message"]})
    return ret


def describe_elasticsearch_domains(domain_names, region=None, keyid=None, key=None, profile=None):
    """
    Returns domain configuration information about the specified Elasticsearch
    domains, including the domain ID, domain endpoint, and domain ARN.

    domain_names (list):
        A list of domain names to get information for.

    region (str, optional):
        The AWS region where the Elasticsearch domains are located.

    keyid (str, optional):
        The AWS access key ID.

    key (str, optional):
        The AWS secret access key.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_elasticsearch.describe_elasticsearch_domains '["domain_a", "domain_b"]'
    """
    ret = {"result": False}
    try:
        conn = _get_conn("es", region=region, keyid=keyid, key=key, profile=profile)
        res = conn.describe_elasticsearch_domains(DomainNames=domain_names)
        if res and "DomainStatusList" in res:
            ret["result"] = True
            ret["response"] = res["DomainStatusList"]
    except (ParamValidationError, ClientError) as exp:
        ret.update({"error": boto3mod.get_error(exp)["message"]})
    return ret


@depends("botocore", version="1.5.18")
def describe_elasticsearch_instance_type_limits(
    instance_type,
    elasticsearch_version,
    domain_name=None,
    region=None,
    keyid=None,
    key=None,
    profile=None,
):
    """
    Describe Elasticsearch Limits for a given InstanceType and ElasticsearchVersion.
    When modifying existing Domain, specify the `` DomainName `` to know what Limits
    are supported for modifying.

    instance_type (str):
        The instance type for an Elasticsearch cluster for which Elasticsearch ``Limits`` are needed.

    elasticsearch_version (str):
        Version of Elasticsearch for which ``Limits`` are needed.

    domain_name (str, optional):
        Represents the name of the Domain that we are trying to modify. This should be present only
        if we are querying for Elasticsearch ``Limits`` for an existing domain.

    region (str, optional):
        The AWS region where the Elasticsearch domain is located.

    keyid (str, optional):
        The AWS access key ID.

    key (str, optional):
        The AWS secret access key.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_elasticsearch.describe_elasticsearch_instance_type_limits \
          instance_type=r3.8xlarge.elasticsearch \
          elasticsearch_version='6.2' \
          domain_name='my-domain'
    """
    ret = {"result": False}
    boto_params = salt.utils.data.filter_falsey(
        {
            "DomainName": domain_name,
            "InstanceType": instance_type,
            "ElasticsearchVersion": str(elasticsearch_version),
        }
    )
    try:
        conn = _get_conn("es", region=region, keyid=keyid, key=key, profile=profile)
        res = conn.describe_elasticsearch_instance_type_limits(**boto_params)
        if res and "LimitsByRole" in res:
            ret["result"] = True
            ret["response"] = res["LimitsByRole"]
    except (ParamValidationError, ClientError) as exp:
        ret.update({"error": boto3mod.get_error(exp)["message"]})
    return ret


@depends("botocore", version="1.10.15")
def describe_reserved_elasticsearch_instance_offerings(
    reserved_elasticsearch_instance_offering_id=None,
    region=None,
    keyid=None,
    key=None,
    profile=None,
):
    """
    Lists available reserved Elasticsearch instance offerings.

    reserved_elasticsearch_instance_offering_id (str, optional):
        The offering identifier filter value. Use this parameter to show only the
        available offering that matches the specified reservation identifier.

    region (str, optional):
        The AWS region where the Elasticsearch domain is located.

    keyid (str, optional):
        The AWS access key ID.

    key (str, optional):
        The AWS secret access key.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_elasticsearch.describe_reserved_elasticsearch_instance_offerings \
          reserved_elasticsearch_instance_offering_id='my-offering-id'
    """
    ret = {"result": False}
    try:
        conn = _get_conn("es", region=region, keyid=keyid, key=key, profile=profile)
        boto_params = {
            "ReservedElasticsearchInstanceOfferingId": reserved_elasticsearch_instance_offering_id
        }
        res = []
        for page in conn.get_paginator(
            "describe_reserved_elasticsearch_instance_offerings"
        ).paginate(**boto_params):
            res.extend(page["ReservedElasticsearchInstanceOfferings"])
        if res:
            ret["result"] = True
            ret["response"] = res
    except (ParamValidationError, ClientError) as exp:
        ret.update({"error": boto3mod.get_error(exp)["message"]})
    return ret


@depends("botocore", version="1.10.15")
def describe_reserved_elasticsearch_instances(
    reserved_elasticsearch_instance_id=None,
    region=None,
    keyid=None,
    key=None,
    profile=None,
):
    """
    Returns information about reserved Elasticsearch instances for this account.

    reserved_elasticsearch_instance_id (str, optional):
        The reserved instance identifier filter value. Use this parameter to show only the
        reservation that matches the specified reserved Elasticsearch instance ID.

    region (str, optional):
        The AWS region where the Elasticsearch domain is located.

    keyid (str, optional):
        The AWS access key ID.

    key (str, optional):
        The AWS secret access key.

    profile (str, optional):
        The profile to use for AWS credentials.

    .. note::
        Version 1.9.174 of boto3 has a bug in that reserved_elasticsearch_instance_id
        is considered a required argument, even though the documentation says otherwise.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_elasticsearch.describe_reserved_elasticsearch_instances \
          reserved_elasticsearch_instance_id='my-instance-id'
    """
    ret = {"result": False}
    try:
        conn = _get_conn("es", region=region, keyid=keyid, key=key, profile=profile)
        boto_params = {
            "ReservedElasticsearchInstanceId": reserved_elasticsearch_instance_id,
        }
        res = []
        for page in conn.get_paginator("describe_reserved_elasticsearch_instances").paginate(
            **boto_params
        ):
            res.extend(page["ReservedElasticsearchInstances"])
        if res:
            ret["result"] = True
            ret["response"] = res
    except (ParamValidationError, ClientError) as exp:
        ret.update({"error": boto3mod.get_error(exp)["message"]})
    return ret


@depends("botocore", version="1.10.77")
def get_compatible_elasticsearch_versions(
    domain_name=None, region=None, keyid=None, key=None, profile=None
):
    """
    Returns a list of upgrade compatible Elastisearch versions. You can optionally
    pass a ``domain_name`` to get all upgrade compatible Elasticsearch versions
    for that specific domain.

    domain_name (str, optional):
        The name of an Elasticsearch domain. If specified, the function will return
        upgrade compatible Elasticsearch versions for that specific domain.

    region (str, optional):
        The AWS region where the Elasticsearch domain is located.

    keyid (str, optional):
        The AWS access key ID.

    key (str, optional):
        The AWS secret access key.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_elasticsearch.get_compatible_elasticsearch_versions \
          domain_name='my-domain'
    """
    ret = {"result": False}
    boto_params = salt.utils.data.filter_falsey({"DomainName": domain_name})
    try:
        conn = _get_conn("es", region=region, keyid=keyid, key=key, profile=profile)
        res = conn.get_compatible_elasticsearch_versions(**boto_params)
        if res and "CompatibleElasticsearchVersions" in res:
            ret["result"] = True
            ret["response"] = res["CompatibleElasticsearchVersions"]
    except (ParamValidationError, ClientError) as exp:
        ret.update({"error": boto3mod.get_error(exp)["message"]})
    return ret


@depends("botocore", version="1.10.77")
def get_upgrade_history(domain_name, region=None, keyid=None, key=None, profile=None):
    """
    Retrieves the complete history of the last 10 upgrades that were performed on the domain.

    domain_name (str):
        The name of an Elasticsearch domain. Domain names are unique across the domains owned by an account within an AWS region.
        Domain names start with a letter or number and can contain the following characters: a-z (lowercase), 0-9, and - (hyphen).

    region (str, optional):
        The AWS region where the Elasticsearch domain is located.

    keyid (str, optional):
        The AWS access key ID.

    key (str, optional):
        The AWS secret access key.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_elasticsearch.get_upgrade_history domain_name='myDomain'
    """
    ret = {"result": False}
    try:
        conn = _get_conn("es", region=region, keyid=keyid, key=key, profile=profile)
        boto_params = {"DomainName": domain_name}
        res = []
        for page in conn.get_paginator("get_upgrade_history").paginate(**boto_params):
            res.extend(page["UpgradeHistories"])
        if res:
            ret["result"] = True
            ret["response"] = res
    except (ParamValidationError, ClientError) as exp:
        ret.update({"error": boto3mod.get_error(exp)["message"]})
    return ret


@depends("botocore", version="1.10.77")
def get_upgrade_status(domain_name, region=None, keyid=None, key=None, profile=None):
    """
    Retrieves the latest status of the last upgrade or upgrade eligibility check
    that was performed on the domain.

    domain_name (str):
        The name of an Elasticsearch domain. Domain names are unique across the domains owned by an account within an AWS region.
        Domain names start with a letter or number and can contain the following characters: a-z (lowercase), 0-9, and - (hyphen).

    region (str, optional):
        The AWS region where the Elasticsearch domain is located.

    keyid (str, optional):
        The AWS access key ID.

    key (str, optional):
        The AWS secret access key.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_elasticsearch.get_upgrade_status domain_name='myDomain'
    """
    ret = {"result": False}
    boto_params = {"DomainName": domain_name}
    try:
        conn = _get_conn("es", region=region, keyid=keyid, key=key, profile=profile)
        res = conn.get_upgrade_status(**boto_params)
        ret["result"] = True
        ret["response"] = res
        del res["ResponseMetadata"]
    except (ParamValidationError, ClientError) as exp:
        ret.update({"error": boto3mod.get_error(exp)["message"]})
    return ret


def list_domain_names(region=None, keyid=None, key=None, profile=None):
    """
    Returns the name of all Elasticsearch domains owned by the current user's account.

    region (str, optional):
        The AWS region where the Elasticsearch domains are located.

    keyid (str, optional):
        The AWS access key ID.

    key (str, optional):
        The AWS secret access key.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_elasticsearch.list_domain_names region='us-west-2'
    """
    ret = {"result": False}
    try:
        conn = _get_conn("es", region=region, keyid=keyid, key=key, profile=profile)
        res = conn.list_domain_names()
        if res and "DomainNames" in res:
            ret["result"] = True
            ret["response"] = [item["DomainName"] for item in res["DomainNames"]]
    except (ParamValidationError, ClientError) as exp:
        ret.update({"error": boto3mod.get_error(exp)["message"]})
    return ret


@depends("botocore", version="1.5.18")
def list_elasticsearch_instance_types(
    elasticsearch_version,
    domain_name=None,
    region=None,
    keyid=None,
    key=None,
    profile=None,
):
    """
    List all Elasticsearch instance types that are supported for given ElasticsearchVersion.

    elasticsearch_version (str):
        The version of Elasticsearch for which to list supported instance types.

    domain_name (str, optional):
        The name of the Elasticsearch domain. This should be provided only if querying for
        instance types when modifying an existing domain.

    region (str, optional):
        The AWS region where the Elasticsearch domain is located.

    keyid (str, optional):
        The AWS access key ID.

    key (str, optional):
        The AWS secret access key.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_elasticsearch.list_elasticsearch_instance_types \
            elasticsearch_version='7.10' domain_name='myDomain'
    """
    ret = {"result": False}
    try:
        conn = _get_conn("es", region=region, keyid=keyid, key=key, profile=profile)
        boto_params = salt.utils.data.filter_falsey(
            {
                "ElasticsearchVersion": str(elasticsearch_version),
                "DomainName": domain_name,
            }
        )
        res = []
        for page in conn.get_paginator("list_elasticsearch_instance_types").paginate(**boto_params):
            res.extend(page["ElasticsearchInstanceTypes"])
        if res:
            ret["result"] = True
            ret["response"] = res
    except (ParamValidationError, ClientError) as exp:
        ret.update({"error": boto3mod.get_error(exp)["message"]})
    return ret


@depends("botocore", version="1.5.18")
def list_elasticsearch_versions(region=None, keyid=None, key=None, profile=None):
    """
    List all supported Elasticsearch versions.

    region (str, optional):
        The AWS region where the Elasticsearch domain is located.

    keyid (str, optional):
        The AWS access key ID.

    key (str, optional):
        The AWS secret access key.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_elasticsearch.list_elasticsearch_versions
    """
    ret = {"result": False}
    try:
        conn = _get_conn("es", region=region, keyid=keyid, key=key, profile=profile)
        res = []
        for page in conn.get_paginator("list_elasticsearch_versions").paginate():
            res.extend(page["ElasticsearchVersions"])
        if res:
            ret["result"] = True
            ret["response"] = res
    except (ParamValidationError, ClientError) as exp:
        ret.update({"error": boto3mod.get_error(exp)["message"]})
    return ret


def list_tags(domain_name=None, arn=None, region=None, key=None, keyid=None, profile=None):
    """
    Returns all tags for the given Elasticsearch domain.

    domain_name (str, optional):
        The name of the Elasticsearch domain for which to list tags.

    arn (str, optional):
        The ARN of the Elasticsearch domain for which to list tags.

    region (str, optional):
        The AWS region where the Elasticsearch domain is located.

    keyid (str, optional):
        The AWS access key ID.

    key (str, optional):
        The AWS secret access key.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_elasticsearch.list_tags domain_name='myDomain'
    """
    if not any((arn, domain_name)):
        raise SaltInvocationError("At least one of domain_name or arn must be specified.")
    ret = {"result": False}
    if arn is None:
        res = describe_elasticsearch_domain(
            domain_name=domain_name,
            region=region,
            key=key,
            keyid=keyid,
            profile=profile,
        )
        if "error" in res:
            ret.update(res)
        elif not res["result"]:
            ret.update({"error": f'The domain with name "{domain_name}" does not exist.'})
        else:
            arn = res["response"].get("ARN")
    if arn:
        try:
            conn = _get_conn("es", region=region, key=key, keyid=keyid, profile=profile)
            res = conn.list_tags(ARN=arn)
            ret["result"] = True
            ret["response"] = {item["Key"]: item["Value"] for item in res.get("TagList", [])}
        except (ParamValidationError, ClientError) as exp:
            ret.update({"error": boto3mod.get_error(exp)["message"]})
    return ret


@depends("botocore", version="1.10.15")
def purchase_reserved_elasticsearch_instance_offering(
    reserved_elasticsearch_instance_offering_id,
    reservation_name,
    instance_count=None,
    region=None,
    keyid=None,
    key=None,
    profile=None,
):
    """
    Allows you to purchase reserved Elasticsearch instances.

    reserved_elasticsearch_instance_offering_id (str):
        The ID of the reserved Elasticsearch instance offering to purchase.

    reservation_name (str):
        A customer-specified identifier to track this reservation.

    instance_count (int, optional):
        The number of Elasticsearch instances to reserve.

    region (str, optional):
        The AWS region where the Elasticsearch domain is located.

    keyid (str, optional):
        The AWS access key ID.

    key (str, optional):
        The AWS secret access key.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_elasticsearch.purchase_reserved_elasticsearch_instance_offering my_offering_id \
            my_reservation_name instance_count=1 region=us-west-2 keyid=my_keyid key=my_key profile=my_profile
    """
    ret = {"result": False}
    boto_params = salt.utils.data.filter_falsey(
        {
            "ReservedElasticsearchInstanceOfferingId": reserved_elasticsearch_instance_offering_id,
            "ReservationName": reservation_name,
            "InstanceCount": instance_count,
        }
    )
    try:
        conn = _get_conn("es", region=region, keyid=keyid, key=key, profile=profile)
        res = conn.purchase_reserved_elasticsearch_instance_offering(**boto_params)
        if res:
            ret["result"] = True
            ret["response"] = res
    except (ParamValidationError, ClientError) as exp:
        ret.update({"error": boto3mod.get_error(exp)["message"]})
    return ret


def remove_tags(
    tag_keys,
    domain_name=None,
    arn=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Removes the specified set of tags from the specified Elasticsearch domain.

    tag_keys (list):
        List with tag keys you want to remove from the Elasticsearch domain.

    domain_name (str, optional):
        The name of the Elasticsearch domain you want to remove tags from.

    arn (str, optional):
        The ARN of the Elasticsearch domain you want to remove tags from.
        Specifying this overrides ``domain_name``.

    region (str, optional):
        The AWS region where the Elasticsearch domain is located.

    keyid (str, optional):
        The AWS access key ID.

    key (str, optional):
        The AWS secret access key.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_elasticsearch.remove_tags '["foo", "bar"]' domain_name=my_domain
    """
    if not any((arn, domain_name)):
        raise SaltInvocationError("At least one of domain_name or arn must be specified.")
    ret = {"result": False}
    if arn is None:
        res = describe_elasticsearch_domain(
            domain_name=domain_name,
            region=region,
            key=key,
            keyid=keyid,
            profile=profile,
        )
        if "error" in res:
            ret.update(res)
        elif not res["result"]:
            ret.update({"error": f'The domain with name "{domain_name}" does not exist.'})
        else:
            arn = res["response"].get("ARN")
    if arn:
        try:
            conn = _get_conn("es", region=region, key=key, keyid=keyid, profile=profile)
            conn.remove_tags(ARN=arn, TagKeys=tag_keys)
            ret["result"] = True
        except (ParamValidationError, ClientError) as exp:
            ret.update({"error": boto3mod.get_error(exp)["message"]})
    return ret


@depends("botocore", version="1.12.21")
def start_elasticsearch_service_software_update(
    domain_name, region=None, keyid=None, key=None, profile=None
):
    """
    Schedules a service software update for an Amazon ES domain.

    domain_name (str):
        The name of the domain that you want to update to the latest service software.

    region (str, optional):
        The AWS region where the Elasticsearch domain is located.

    keyid (str, optional):
        The AWS access key ID.

    key (str, optional):
        The AWS secret access key.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_elasticsearch.start_elasticsearch_service_software_update my_domain

    """
    ret = {"result": False}
    boto_params = {"DomainName": domain_name}
    try:
        conn = _get_conn("es", region=region, keyid=keyid, key=key, profile=profile)
        res = conn.start_elasticsearch_service_software_update(**boto_params)
        if res and "ServiceSoftwareOptions" in res:
            ret["result"] = True
            ret["response"] = res["ServiceSoftwareOptions"]
    except (ParamValidationError, ClientError) as exp:
        ret.update({"error": boto3mod.get_error(exp)["message"]})
    return ret


def update_elasticsearch_domain_config(
    domain_name,
    elasticsearch_cluster_config=None,
    ebs_options=None,
    vpc_options=None,
    access_policies=None,
    snapshot_options=None,
    cognito_options=None,
    advanced_options=None,
    log_publishing_options=None,
    blocking=False,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Modifies the cluster configuration of the specified Elasticsearch domain,
    for example setting the instance type and the number of instances.

    domain_name (str):
        The name of the Elasticsearch domain that you want to update.

    elasticsearch_cluster_config (dict, optional):
        The configuration for the Elasticsearch cluster, such as instance type and count.

        Example:

        .. code-block:: json

            {
                "InstanceType": "t2.micro.elasticsearch",
                "InstanceCount": 1,
                "DedicatedMasterEnabled": false,
                "ZoneAwarenessEnabled": false
            }

    ebs_options (dict, optional):
        The configuration for EBS volumes attached to the domain.

        Example:

        .. code-block:: json

            {
                "EBSEnabled": true,
                "VolumeType": "gp2",
                "VolumeSize": 10,
                "Iops": 0
            }

    vpc_options (dict, optional):
        The VPC configuration for the domain.

        Example:

        .. code-block:: json

            {
                "VPCId": "vpc-12345678",
                "SubnetIds": ["subnet-12345678", "subnet-87654321"],
                "SecurityGroupIds": ["sg-12345678"]
            }

    access_policies (dict, optional):
        The access policies for the domain.

        Example:

        .. code-block:: json

            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"AWS": "*"},
                        "Action": "es:*",
                        "Resource": "arn:aws:es:us-east-1:111111111111:domain/mydomain/*",
                        "Condition": {"IpAddress": {"aws:SourceIp": ["127.0.0.1"]}}
                    }
                ]
            }

    snapshot_options (dict, optional):
        The snapshot options for the domain.

        Example:

        .. code-block:: json

            {
                "AutomatedSnapshotStartHour": 0
            }

    cognito_options (dict, optional):
        The Amazon Cognito options for the domain.

        Example:

        .. code-block:: json

            {
                "Enabled": true,
                "UserPoolId": "us-east-1_123456789",
                "IdentityPoolId": "us-east-1:12345678-1234-1234-1234-123456789012",
                "RoleArn": "arn:aws:iam::111111111111:role/CognitoAccessRole"
            }

    advanced_options (dict, optional):
        The advanced options for the domain.

        Example:

        .. code-block:: json

            {
                "rest.action.multi.allow_explicit_index": "true"
            }

    log_publishing_options (dict, optional):
        The log publishing options for the domain.

        Example:

        .. code-block:: json

            {
                "INDEX_SLOW_LOGS": {
                    "CloudWatchLogsLogGroupArn": "arn:aws:logs:us-east-1:111111111111:log-group:my-log-group",
                    "Enabled": true
                }
            }

    blocking (bool, optional):
        Whether to block until the domain is available after the update.

    region (str, optional):
        The AWS region where the Elasticsearch domain is located.

    keyid (str, optional):
        The AWS access key ID.

    key (str, optional):
        The AWS secret access key.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_elasticsearch.update_elasticsearch_domain_config mydomain \
          elasticsearch_cluster_config='{\
            "InstanceType": "t2.micro.elasticsearch", \
            "InstanceCount": 1, \
            "DedicatedMasterEnabled": false, \
            "ZoneAwarenessEnabled": false \
          }' \
          ebs_options='{\
            "EBSEnabled": true, \
            "VolumeType": "gp2", \
            "VolumeSize": 10, \
            "Iops": 0 \
          }' \
          access_policies='{\
            "Version": "2012-10-17", \
            "Statement": [{\
              "Effect": "Allow", \
              "Principal": {"AWS": "*"}, \
              "Action": "es:*", \
              "Resource": "arn:aws:es:us-east-1:111111111111:domain/mydomain/*", \
              "Condition": {"IpAddress": {"aws:SourceIp": ["127.0.0.1"]}}\
            }]\
          }' \
          snapshot_options='{\
            "AutomatedSnapshotStartHour": 0 \
          }' \
          advanced_options='{\
            "rest.action.multi.allow_explicit_index": "true" \
          }'
    """
    ret = {"result": False}
    boto_kwargs = salt.utils.data.filter_falsey(
        {
            "DomainName": domain_name,
            "ElasticsearchClusterConfig": elasticsearch_cluster_config,
            "EBSOptions": ebs_options,
            "SnapshotOptions": snapshot_options,
            "VPCOptions": vpc_options,
            "CognitoOptions": cognito_options,
            "AdvancedOptions": advanced_options,
            "AccessPolicies": (
                salt.utils.json.dumps(access_policies)
                if isinstance(access_policies, dict)
                else access_policies
            ),
            "LogPublishingOptions": log_publishing_options,
        }
    )
    try:
        conn = _get_conn("es", region=region, keyid=keyid, key=key, profile=profile)
        res = conn.update_elasticsearch_domain_config(**boto_kwargs)
        if not res or "DomainConfig" not in res:
            log.warning("Domain was not updated")
        else:
            ret["result"] = True
            ret["response"] = res["DomainConfig"]
        if blocking:
            conn.get_waiter("ESDomainAvailable").wait(DomainName=domain_name)
    except (ParamValidationError, ClientError, WaiterError) as exp:
        ret.update({"error": boto3mod.get_error(exp)["message"]})
    return ret


@depends("botocore", version="1.10.77")
def upgrade_elasticsearch_domain(
    domain_name,
    target_version,
    perform_check_only=None,
    blocking=False,
    region=None,
    keyid=None,
    key=None,
    profile=None,
):
    """
    Allows you to either upgrade your domain or perform an Upgrade eligibility
    check to a compatible Elasticsearch version.

    domain_name (str):
        The name of the Elasticsearch domain to upgrade.

    target_version (str):
        The version of Elasticsearch that you intend to upgrade the domain to.

    perform_check_only (bool):
        This flag, when set to True, indicates that an Upgrade Eligibility Check needs to be performed. This will not actually perform the Upgrade.

    blocking (bool):
        Whether or not to wait (block) until the Elasticsearch domain has been upgraded.

    region (str):
        The AWS region where the Elasticsearch domain is located.

    keyid (str):
        The AWS access key ID.

    key (str):
        The AWS secret access key.

    profile (str):
        The AWS profile to use.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_elasticsearch.upgrade_elasticsearch_domain mydomain \
        target_version='6.7' \
        perform_check_only=True
    """
    ret = {"result": False}
    boto_params = salt.utils.data.filter_falsey(
        {
            "DomainName": domain_name,
            "TargetVersion": str(target_version),
            "PerformCheckOnly": perform_check_only,
        }
    )
    try:
        conn = _get_conn("es", region=region, keyid=keyid, key=key, profile=profile)
        res = conn.upgrade_elasticsearch_domain(**boto_params)
        if res:
            ret["result"] = True
            ret["response"] = res
        if blocking:
            conn.get_waiter("ESUpgradeFinished").wait(DomainName=domain_name)
    except (ParamValidationError, ClientError, WaiterError) as exp:
        ret.update({"error": boto3mod.get_error(exp)["message"]})
    return ret


def exists(domain_name, region=None, key=None, keyid=None, profile=None):
    """
    Given a domain name, check to see if the given domain exists.

    domain_name (str):
        The name of the domain to check.

    region (str):
        The AWS region where the Elasticsearch domain is located.

    keyid (str):
        The AWS access key ID.

    key (str):
        The AWS secret access key.

    profile (str):
        The AWS profile to use.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_elasticsearch.exists mydomain

    """
    ret = {"result": False}
    try:
        conn = _get_conn("es", region=region, key=key, keyid=keyid, profile=profile)
        conn.describe_elasticsearch_domain(DomainName=domain_name)
        ret["result"] = True
    except (ParamValidationError, ClientError) as exp:
        if exp.response.get("Error", {}).get("Code") != "ResourceNotFoundException":
            ret.update({"error": boto3mod.get_error(exp)["message"]})
    return ret


def wait_for_upgrade(domain_name, region=None, keyid=None, key=None, profile=None):
    """
    Block until an upgrade-in-progress for domain ``name`` is finished.

    domain_name (str):
        The name of the domain to wait for an upgrade to finish.

    region (str):
        The AWS region where the Elasticsearch domain is located.

    keyid (str):
        The AWS access key ID.

    key (str):
        The AWS secret access key.

    profile (str):
        The AWS profile to use.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_elasticsearch.wait_for_upgrade mydomain

    """
    ret = {"result": False}
    try:
        conn = _get_conn("es", region=region, keyid=keyid, key=key, profile=profile)
        conn.get_waiter("ESUpgradeFinished").wait(DomainName=domain_name)
        ret["result"] = True
    except (ParamValidationError, ClientError, WaiterError) as exp:
        ret.update({"error": boto3mod.get_error(exp)["message"]})
    return ret


@depends("botocore", version="1.10.77")
def check_upgrade_eligibility(
    domain_name, elasticsearch_version, region=None, keyid=None, key=None, profile=None
):
    """
    Helper function to determine in one call if an Elasticsearch domain can be
    upgraded to the specified Elasticsearch version.

    This assumes that the Elasticsearch domain is at rest at the moment this function
    is called. I.e. The domain is not in the process of :

    - being created.
    - being updated.
    - another upgrade running, or a check thereof.
    - being deleted.

    Behind the scenes, this does 3 things:

    - Check if ``elasticsearch_version`` is among the compatible elasticsearch versions.
    - Perform a check if the Elasticsearch domain is eligible for the upgrade.
    - Check the result of the check and return the result as a boolean.

    domain_name (str):
        The name of the Elasticsearch domain to check.

    elasticsearch_version (str):
        The Elasticsearch version to upgrade to.

    region (str):
        The AWS region where the Elasticsearch domain is located.

    keyid (str):
        The AWS access key ID.

    key (str):
        The AWS secret access key.

    profile (str):
        The AWS profile to use.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_elasticsearch.check_upgrade_eligibility mydomain '6.7'
    """
    ret = {"result": False}
    # Check if the desired version is in the list of compatible versions
    res = get_compatible_elasticsearch_versions(
        domain_name, region=region, keyid=keyid, key=key, profile=profile
    )
    if "error" in res:
        return res
    compatible_versions = res["response"][0]["TargetVersions"]
    if str(elasticsearch_version) not in compatible_versions:
        ret["result"] = True
        ret["response"] = False
        ret["error"] = 'Desired version "{}" not in compatible versions: {}.'.format(
            elasticsearch_version, compatible_versions
        )
        return ret
    # Check if the domain is eligible to upgrade to the desired version
    res = upgrade_elasticsearch_domain(
        domain_name,
        elasticsearch_version,
        perform_check_only=True,
        blocking=True,
        region=region,
        keyid=keyid,
        key=key,
        profile=profile,
    )
    if "error" in res:
        return res
    res = wait_for_upgrade(domain_name, region=region, keyid=keyid, key=key, profile=profile)
    if "error" in res:
        return res
    res = get_upgrade_status(domain_name, region=region, keyid=keyid, key=key, profile=profile)
    ret["result"] = True
    ret["response"] = (
        res["response"]["UpgradeStep"] == "PRE_UPGRADE_CHECK"
        and res["response"]["StepStatus"] == "SUCCEEDED"
    )
    return ret
