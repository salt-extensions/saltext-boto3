"""
Connection module for Amazon S3 using boto3.
============================================

    Renamed from ``boto_s3`` to ``boto3_s3`` and rewritten to use the
    boto3 ``s3`` client APIs directly via
    :py:mod:`saltext.boto3.utils.boto3mod`.  The legacy boto2 code path
    (object-style access, retry loops) has been removed.

:depends:
  - boto3 >= 1.28.0
  - botocore >= 1.31.0

:configuration: This module accepts explicit S3 credentials but can
    also utilize IAM roles assigned to the instance through Instance Profiles.
    Dynamic credentials are then automatically obtained from AWS API and no
    further configuration is necessary. More Information available at:

    .. code-block:: text

        http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html

    If IAM roles are not used you need to specify them either in the minion's
    config file or as a profile. For example, to specify them in the minion's
    config file:

.. code-block:: yaml

    s3.keyid: GKTADJGHEIQSXMKKRBJ08H
    s3.key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs

A region may also be specified in the configuration:

.. code-block:: yaml

    s3.region: us-east-1

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

from saltext.boto3.utils import boto3mod

log = logging.getLogger(__name__)

try:
    import boto3
    import botocore.exceptions

    logging.getLogger("boto3").setLevel(logging.CRITICAL)
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

__virtualname__ = "boto3_s3"


def __virtual__():
    """
    Only load if boto3 is available.
    """
    if HAS_BOTO3:
        return __virtualname__
    return (False, "The boto3_s3 module could not be loaded: boto3 is not available.")


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


def get_object_metadata(
    name,
    extra_args=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Get metadata about an S3 object. Returns ``{"result": None}`` if the
    object does not exist.

    You can pass AWS SSE-C related args and/or ``RequestPayer`` in ``extra_args``.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_s3.get_object_metadata my_bucket/path/to/object
    """
    bucket, _, s3_key = name.partition("/")
    if extra_args is None:
        extra_args = {}

    conn = _get_conn("s3", region=region, key=key, keyid=keyid, profile=profile)

    try:
        metadata = conn.head_object(Bucket=bucket, Key=s3_key, **extra_args)
    except botocore.exceptions.ClientError as e:
        if e.response["Error"].get("Message") == "Not Found":
            return {"result": None}
        return {"error": boto3mod.get_error(e)}

    return {"result": metadata}


def upload_file(
    source,
    name,
    extra_args=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Upload a local file as an S3 object.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_s3.upload_file /path/to/local/file my_bucket/path/to/object
    """
    bucket, _, s3_key = name.partition("/")

    conn = _get_conn("s3", region=region, key=key, keyid=keyid, profile=profile)

    try:
        conn.upload_file(source, bucket, s3_key, ExtraArgs=extra_args)
    except boto3.exceptions.S3UploadFailedError as e:
        return {"error": boto3mod.get_error(e)}

    log.info("S3 object uploaded to %s", name)
    return {"result": True}
