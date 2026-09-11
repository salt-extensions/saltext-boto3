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

.. versionchanged:: 1.2.0
    Added ``download_file`` and ``generate_presigned_url`` object operations.
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

    name (str):
        S3 object location in ``bucket/key`` format.

    extra_args (dict, optional):
        Optional args passed to ``head_object``.

    region (str, optional):
        AWS region.

    key (str, optional):
        AWS access key.

    keyid (str, optional):
        AWS access key ID.

    profile (dict or str, optional):
        AWS profile to use.

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


def get_object(
    name,
    extra_args=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Get an S3 object body.

    .. versionadded:: 1.0.2

    name (str):
        S3 object location in ``bucket/key`` format.

    extra_args (dict, optional):
        Optional args passed to ``get_object``.

    region (str, optional):
        AWS region.

    key (str, optional):
        AWS access key.

    keyid (str, optional):
        AWS access key ID.

    profile (dict or str, optional):
        AWS profile to use.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_s3.get_object my_bucket/path/to/object
    """
    bucket, _, s3_key = name.partition("/")
    if not bucket or not s3_key:
        return {"error": "name must be in bucket/key format"}
    if extra_args is None:
        extra_args = {}

    conn = _get_conn("s3", region=region, key=key, keyid=keyid, profile=profile)

    try:
        response = conn.get_object(Bucket=bucket, Key=s3_key, **extra_args)
    except botocore.exceptions.ClientError as e:
        return {"error": boto3mod.get_error(e)}

    return {"result": response["Body"].read()}


def put_object(
    name,
    data,
    extra_args=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Create or update an S3 object.

    .. versionadded:: 1.0.2

    name (str):
        S3 object location in ``bucket/key`` format.

    data (str):
        Object body content.

    extra_args (dict, optional):
        Optional args passed to ``put_object``.

    region (str, optional):
        AWS region.

    key (str, optional):
        AWS access key.

    keyid (str, optional):
        AWS access key ID.

    profile (dict or str, optional):
        AWS profile to use.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_s3.put_object my_bucket/path/to/object "payload"
    """
    bucket, _, s3_key = name.partition("/")
    if not bucket or not s3_key:
        return {"error": "name must be in bucket/key format"}

    args = {"Bucket": bucket, "Key": s3_key, "Body": data}
    if extra_args:
        args.update(extra_args)

    conn = _get_conn("s3", region=region, key=key, keyid=keyid, profile=profile)

    try:
        conn.put_object(**args)
    except botocore.exceptions.ClientError as e:
        return {"error": boto3mod.get_error(e)}

    return {"result": True}


def delete_object(
    name,
    extra_args=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Delete an S3 object.

    .. versionadded:: 1.2.0

    name (str):
        S3 object location in ``bucket/key`` format.

    extra_args (dict, optional):
        Optional args passed to ``delete_object``.

    region (str, optional):
        AWS region.

    key (str, optional):
        AWS access key.

    keyid (str, optional):
        AWS access key ID.

    profile (dict or str, optional):
        AWS profile to use.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_s3.delete_object my_bucket/path/to/object
    """
    bucket, _, s3_key = name.partition("/")
    if not bucket or not s3_key:
        return {"error": "name must be in bucket/key format"}

    args = {"Bucket": bucket, "Key": s3_key}
    if extra_args:
        args.update(extra_args)

    conn = _get_conn("s3", region=region, key=key, keyid=keyid, profile=profile)

    try:
        conn.delete_object(**args)
    except botocore.exceptions.ClientError as e:
        return {"error": boto3mod.get_error(e)}

    return {"result": True}


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

    source (str):
        Local path to the file to upload.

    name (str):
        S3 object location in ``bucket/key`` format.

    extra_args (dict, optional):
        Optional args passed to ``upload_file``.

    region (str, optional):
        AWS region.

    key (str, optional):
        AWS access key.

    keyid (str, optional):
        AWS access key ID.

    profile (dict or str, optional):
        AWS profile to use.

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


def download_file(
    name,
    destination,
    extra_args=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Download an S3 object to a local file path.

    .. versionadded:: 1.2.0

    name (str):
        S3 object location in ``bucket/key`` format.

    destination (str):
        Local destination path where the object should be written.

    extra_args (dict, optional):
        Optional boto3 Transfer ``ExtraArgs`` for ``download_file``.

    region (str, optional):
        AWS region.

    key (str, optional):
        AWS access key.

    keyid (str, optional):
        AWS access key ID.

    profile (dict or str, optional):
        AWS profile to use.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_s3.download_file my_bucket/path/to/object /tmp/object
    """
    bucket, _, s3_key = name.partition("/")
    if not bucket or not s3_key:
        return {"error": "name must be in bucket/key format"}

    conn = _get_conn("s3", region=region, key=key, keyid=keyid, profile=profile)

    try:
        conn.download_file(bucket, s3_key, destination, ExtraArgs=extra_args)
    except botocore.exceptions.ClientError as e:
        return {"error": boto3mod.get_error(e)}

    log.info("S3 object %s downloaded to %s", name, destination)
    return {"result": True}


def generate_presigned_url(
    name,
    expiration=900,
    extra_args=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Generate a pre-signed GET URL for an S3 object.

    .. versionadded:: 1.2.0

    name (str):
        S3 object location in ``bucket/key`` format.

    expiration (int, optional):
        Number of seconds the pre-signed URL remains valid.

    extra_args (dict, optional):
        Optional dict merged into the request ``Params``.

    region (str, optional):
        AWS region.

    key (str, optional):
        AWS access key.

    keyid (str, optional):
        AWS access key ID.

    profile (dict or str, optional):
        AWS profile to use.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_s3.generate_presigned_url my_bucket/path/to/object expiration=300
    """
    bucket, _, s3_key = name.partition("/")
    if not bucket or not s3_key:
        return {"error": "name must be in bucket/key format"}

    params = {"Bucket": bucket, "Key": s3_key}
    if extra_args:
        params.update(extra_args)

    conn = _get_conn("s3", region=region, key=key, keyid=keyid, profile=profile)

    try:
        url = conn.generate_presigned_url("get_object", Params=params, ExpiresIn=expiration)
    except botocore.exceptions.ClientError as e:
        return {"error": boto3mod.get_error(e)}

    return {"result": url}
