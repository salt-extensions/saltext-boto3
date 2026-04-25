"""
Connection module for Amazon CloudFront using boto3.
====================================================

    Renamed from ``boto_cloudfront`` to ``boto3_cloudfront`` and rewritten to use the
    boto3 ``cloudfront`` client APIs directly via
    :py:mod:`saltext.boto3.utils.boto3mod`.  The legacy boto2 code path
    (object-style access, retry loops) has been removed.

:depends:
  - boto3 >= 1.28.0
  - botocore >= 1.31.0

:configuration: This module accepts explicit CloudFront credentials but can
    also utilize IAM roles assigned to the instance through Instance Profiles.
    Dynamic credentials are then automatically obtained from AWS API and no
    further configuration is necessary. More Information available at:

    .. code-block:: text

        http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html

    If IAM roles are not used you need to specify them either in the minion's
    config file or as a profile. For example, to specify them in the minion's
    config file:

.. code-block:: yaml

    cloudfront.keyid: GKTADJGHEIQSXMKKRBJ08H
    cloudfront.key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs

A region may also be specified in the configuration:

.. code-block:: yaml

    cloudfront.region: us-east-1

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

import salt.utils.dictdiffer
import salt.utils.yaml
from salt.utils.odict import OrderedDict

from saltext.boto3.utils import boto3mod

try:
    from botocore.exceptions import ClientError  # noqa: F401

    logging.getLogger("boto3").setLevel(logging.CRITICAL)
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

log = logging.getLogger(__name__)

__virtualname__ = "boto3_cloudfront"


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
    Only load if boto3 is available.
    """
    if HAS_BOTO3:
        return __virtualname__
    return (
        False,
        "The boto3_cloudfront module could not be loaded: boto3 is not available.",
    )


def _list_distributions(conn, name=None):
    """
    Private generator yielding ``(name_tag, distribution)`` tuples for every
    CloudFront distribution (optionally filtered by ``Name`` tag).
    """
    for dl_ in conn.get_paginator("list_distributions").paginate():
        distribution_list = dl_["DistributionList"]
        if "Items" not in distribution_list:
            continue
        for partial_dist in distribution_list["Items"]:
            tags = conn.list_tags_for_resource(Resource=partial_dist["ARN"])
            tags = {kv["Key"]: kv["Value"] for kv in tags["Tags"]["Items"]}

            id_ = partial_dist["Id"]
            if "Name" not in tags:
                log.warning("CloudFront distribution %s has no Name tag.", id_)
                continue
            distribution_name = tags.pop("Name", None)
            if name is not None and distribution_name != name:
                continue

            dist_with_etag = conn.get_distribution(Id=id_)
            distribution = {
                "distribution": dist_with_etag["Distribution"],
                "etag": dist_with_etag["ETag"],
                "tags": tags,
            }
            yield (distribution_name, distribution)


def get_distribution(name, region=None, key=None, keyid=None, profile=None):
    """
    Get information about a CloudFront distribution (configuration, tags) with
    a given ``Name`` tag.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_cloudfront.get_distribution name=mydistribution profile=awsprofile
    """
    conn = _get_conn("cloudfront", region=region, key=key, keyid=keyid, profile=profile)
    distribution = None
    try:
        for _, dist in _list_distributions(conn, name=name):
            if distribution is not None:
                return {"error": f"More than one distribution found with name {name}"}
            distribution = dist
    except ClientError as err:
        return {"error": boto3mod.get_error(err)}
    if not distribution:
        return {"result": None}
    return {"result": distribution}


def export_distributions(region=None, key=None, keyid=None, profile=None):
    """
    Get details of all CloudFront distributions. Produces results that can be
    used to create an SLS file.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_cloudfront.export_distributions --out=txt | \
            sed "s/local: //" > cloudfront_distributions.sls
    """
    results = OrderedDict()
    conn = _get_conn("cloudfront", region=region, key=key, keyid=keyid, profile=profile)
    try:
        for name, distribution in _list_distributions(conn):
            config = distribution["distribution"]["DistributionConfig"]
            tags = distribution["tags"]
            distribution_sls_data = [
                {"name": name},
                {"config": config},
                {"tags": tags},
            ]
            results[f"Manage CloudFront distribution {name}"] = {
                "boto3_cloudfront.present": distribution_sls_data,
            }
    except ClientError as exc:
        log.trace("Boto client error: %s", exc)

    dumper = salt.utils.yaml.get_dumper("IndentedSafeOrderedDumper")
    return salt.utils.yaml.dump(results, default_flow_style=False, Dumper=dumper)


def create_distribution(
    name,
    config,
    tags=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Create a CloudFront distribution with the given name, config, and
    (optionally) tags.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_cloudfront.create_distribution name=mydistribution profile=awsprofile \
            config='{"Comment":"partial configuration","Enabled":true}'
    """
    if tags is None:
        tags = {}
    if "Name" in tags:
        if tags["Name"] != name:
            return {"error": "Must not pass `Name` in `tags` but as `name`"}
    tags["Name"] = name
    tags = {"Items": [{"Key": k, "Value": v} for k, v in tags.items()]}

    conn = _get_conn("cloudfront", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.create_distribution_with_tags(
            DistributionConfigWithTags={"DistributionConfig": config, "Tags": tags},
        )
    except ClientError as err:
        return {"error": boto3mod.get_error(err)}

    return {"result": True}


def update_distribution(
    name,
    config,
    tags=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Update the config (and optionally tags) for the CloudFront distribution
    with the given ``Name`` tag.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_cloudfront.update_distribution name=mydistribution profile=awsprofile \
            config='{"Comment":"partial configuration","Enabled":true}'
    """
    distribution_ret = get_distribution(name, region=region, key=key, keyid=keyid, profile=profile)
    if "error" in distribution_ret:
        return distribution_ret
    dist_with_tags = distribution_ret["result"]

    current_distribution = dist_with_tags["distribution"]
    current_config = current_distribution["DistributionConfig"]
    current_tags = dist_with_tags["tags"]
    etag = dist_with_tags["etag"]

    config_diff = salt.utils.dictdiffer.deep_diff(current_config, config)
    if tags:
        tags_diff = salt.utils.dictdiffer.deep_diff(current_tags, tags)

    conn = _get_conn("cloudfront", region=region, key=key, keyid=keyid, profile=profile)
    try:
        if "old" in config_diff or "new" in config_diff:
            conn.update_distribution(
                DistributionConfig=config,
                Id=current_distribution["Id"],
                IfMatch=etag,
            )
        if tags:
            arn = current_distribution["ARN"]
            if "new" in tags_diff:
                tags_to_add = {
                    "Items": [{"Key": k, "Value": v} for k, v in tags_diff["new"].items()],
                }
                conn.tag_resource(Resource=arn, Tags=tags_to_add)
            if "old" in tags_diff:
                tags_to_remove = {"Items": list(tags_diff["old"].keys())}
                conn.untag_resource(Resource=arn, TagKeys=tags_to_remove)
    except ClientError as err:
        return {"error": boto3mod.get_error(err)}

    return {"result": True}
