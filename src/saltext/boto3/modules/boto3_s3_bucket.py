"""
Connection module for Amazon S3 Buckets using boto3.
====================================================

    Renamed from ``boto_s3_bucket`` to ``boto3_s3_bucket`` and rewritten to use the
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

import salt.utils.json
from salt.exceptions import SaltInvocationError

from saltext.boto3.utils import boto3mod

log = logging.getLogger(__name__)


try:
    from botocore.exceptions import ClientError

    logging.getLogger("boto3").setLevel(logging.CRITICAL)
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False


__virtualname__ = "boto3_s3_bucket"


def __virtual__():
    """
    Only load if boto3 is available.
    """
    if HAS_BOTO3:
        return __virtualname__
    return (False, "The boto3_s3_bucket module could not be loaded: boto3 is not available.")


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


def exists(Bucket, region=None, key=None, keyid=None, profile=None):
    """
    Given a bucket name, check to see if the given bucket exists.

    Bucket (str):
        The name of the S3 bucket to check for existence.

    region (str, optional):
        The AWS region to use.

    key (str, optional):
        The AWS access key to use.

    keyid (str, optional):
        The AWS secret key to use.

    profile (str, optional):
        The AWS profile to use.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_s3_bucket.exists mybucket

    """

    try:
        conn = _get_conn("s3", region=region, key=key, keyid=keyid, profile=profile)
        conn.head_bucket(Bucket=Bucket)
        return {"exists": True}
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "404":
            return {"exists": False}
        err = boto3mod.get_error(e)
        return {"error": err}


def create(
    Bucket,
    ACL=None,
    LocationConstraint=None,
    GrantFullControl=None,
    GrantRead=None,
    GrantReadACP=None,
    GrantWrite=None,
    GrantWriteACP=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):  # pylint: disable=unused-argument
    """
    Given a valid config, create an S3 Bucket.

    Bucket (str):
        The name of the S3 bucket to create.

    ACL (str, optional):
        The canned ACL to apply to the bucket.

    LocationConstraint (str, optional):
        The AWS region where the bucket will be created.

    GrantFullControl (str, optional):
        The grantee is granted FULL_CONTROL permissions on the bucket.

    GrantRead (str, optional):
        The grantee is granted READ permissions on the bucket.

    GrantReadACP (str, optional):
        The grantee is granted READ_ACP permissions on the bucket.

    GrantWrite (str, optional):
        The grantee is granted WRITE permissions on the bucket.

    GrantWriteACP (str, optional):
        The grantee is granted WRITE_ACP permissions on the bucket.

    region (str, optional):
        The AWS region to use.

    key (str, optional):
        The AWS access key to use.

    keyid (str, optional):
        The AWS secret key to use.

    profile (str, optional):
        The AWS profile to use.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_s3_bucket.create my_bucket \\
                         GrantFullControl='emailaddress=example@example.com' \\
                         GrantRead='uri="http://acs.amazonaws.com/groups/global/AllUsers"' \\
                         GrantReadACP='emailaddress="exampl@example.com",id="2345678909876432"' \\
                         LocationConstraint=us-west-1

    """

    try:
        conn = _get_conn("s3", region=region, key=key, keyid=keyid, profile=profile)
        kwargs = {}
        for arg in (
            "ACL",
            "GrantFullControl",
            "GrantRead",
            "GrantReadACP",
            "GrantWrite",
            "GrantWriteACP",
        ):
            if locals()[arg] is not None:
                kwargs[arg] = str(locals()[arg])
        if LocationConstraint:
            kwargs["CreateBucketConfiguration"] = {"LocationConstraint": LocationConstraint}
        location = conn.create_bucket(Bucket=Bucket, **kwargs)
        conn.get_waiter("bucket_exists").wait(Bucket=Bucket)
        if location:
            log.info("The newly created bucket name is located at %s", location["Location"])

            return {"created": True, "name": Bucket, "Location": location["Location"]}
        else:
            log.warning("Bucket was not created")
            return {"created": False}
    except ClientError as e:
        return {"created": False, "error": boto3mod.get_error(e)}


def delete(
    Bucket,
    MFA=None,
    RequestPayer=None,
    Force=False,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Given a bucket name, delete it, optionally emptying it first.

    Bucket (str):
        The name of the S3 bucket to delete.

    MFA (str, optional):
        The MFA authentication information.

    RequestPayer (str, optional):
        Confirms that the requester knows that they will be charged for the request.

    Force (bool, optional):
        If True, empties the bucket before deleting it.

    region (str, optional):
        The AWS region to use.

    key (str, optional):
        The AWS access key to use.

    keyid (str, optional):
        The AWS secret key to use.

    profile (str, optional):
        The AWS profile to use.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_s3_bucket.delete mybucket

    """

    try:
        conn = _get_conn("s3", region=region, key=key, keyid=keyid, profile=profile)
        if Force:
            empty(
                Bucket,
                MFA=MFA,
                RequestPayer=RequestPayer,
                region=region,
                key=key,
                keyid=keyid,
                profile=profile,
            )
        conn.delete_bucket(Bucket=Bucket)
        return {"deleted": True}
    except ClientError as e:
        return {"deleted": False, "error": boto3mod.get_error(e)}


def delete_objects(
    Bucket,
    Delete,
    MFA=None,
    RequestPayer=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Delete objects in a given S3 bucket.

    Bucket (str):
        The name of the S3 bucket containing the objects to delete.

    Delete (dict):
        A dictionary specifying the objects to delete. Must contain an "Objects" key with a list of object keys.

    MFA (str, optional):
        The MFA authentication information.

    RequestPayer (str, optional):
        Confirms that the requester knows that they will be charged for the request.

    region (str, optional):
        The AWS region to use.

    key (str, optional):
        The AWS access key to use.

    keyid (str, optional):
        The AWS secret key to use.

    profile (str, optional):
        The AWS profile to use.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_s3_bucket.delete_objects mybucket '{Objects: [Key: myobject]}'

    """

    if isinstance(Delete, str):
        Delete = salt.utils.json.loads(Delete)
    if not isinstance(Delete, dict):
        raise SaltInvocationError("Malformed Delete request.")
    if "Objects" not in Delete:
        raise SaltInvocationError("Malformed Delete request.")

    failed = []
    objs = Delete["Objects"]
    for i in range(0, len(objs), 1000):
        chunk = objs[i : i + 1000]
        subset = {"Objects": chunk, "Quiet": True}
        try:
            args = {"Bucket": Bucket}
            if MFA:
                args["MFA"] = MFA
            if RequestPayer:
                args["RequestPayer"] = RequestPayer
            args["Delete"] = subset
            conn = _get_conn("s3", region=region, key=key, keyid=keyid, profile=profile)
            ret = conn.delete_objects(**args)
            failed += ret.get("Errors", [])
        except ClientError as e:
            return {"deleted": False, "error": boto3mod.get_error(e)}

    if failed:
        return {"deleted": False, "failed": failed}
    else:
        return {"deleted": True}


def describe(Bucket, region=None, key=None, keyid=None, profile=None):
    """
    Given a bucket name describe its properties.

    Bucket (str):
        The name of the S3 bucket to describe.

    region (str, optional):
        The AWS region to use.

    key (str, optional):
        The AWS access key to use.

    keyid (str, optional):
        The AWS secret key to use.

    profile (str, optional):
        The AWS profile to use.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_s3_bucket.describe mybucket

    """

    try:
        conn = _get_conn("s3", region=region, key=key, keyid=keyid, profile=profile)
        result = {}
        conn_dict = {
            "ACL": conn.get_bucket_acl,
            "CORS": conn.get_bucket_cors,
            "LifecycleConfiguration": conn.get_bucket_lifecycle_configuration,
            "Location": conn.get_bucket_location,
            "Logging": conn.get_bucket_logging,
            "NotificationConfiguration": conn.get_bucket_notification_configuration,
            "Policy": conn.get_bucket_policy,
            "Replication": conn.get_bucket_replication,
            "RequestPayment": conn.get_bucket_request_payment,
            "Versioning": conn.get_bucket_versioning,
            "Website": conn.get_bucket_website,
        }

        for key, query in conn_dict.items():
            try:
                data = query(Bucket=Bucket)
            except ClientError as e:
                if e.response.get("Error", {}).get("Code") in (
                    "NoSuchLifecycleConfiguration",
                    "NoSuchCORSConfiguration",
                    "NoSuchBucketPolicy",
                    "NoSuchWebsiteConfiguration",
                    "ReplicationConfigurationNotFoundError",
                    "NoSuchTagSet",
                ):
                    continue
                raise
            if "ResponseMetadata" in data:
                del data["ResponseMetadata"]
            result[key] = data

        tags = {}
        try:
            data = conn.get_bucket_tagging(Bucket=Bucket)
            for tagdef in data.get("TagSet"):
                tags[tagdef.get("Key")] = tagdef.get("Value")
        except ClientError as e:
            if not e.response.get("Error", {}).get("Code") == "NoSuchTagSet":
                raise
        if tags:
            result["Tagging"] = tags
        return {"bucket": result}
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "NoSuchBucket":
            return {"bucket": None}
        return {"error": boto3mod.get_error(e)}


def empty(Bucket, MFA=None, RequestPayer=None, region=None, key=None, keyid=None, profile=None):
    """
    Delete all objects in a given S3 bucket.

    Bucket (str):
        The name of the S3 bucket to empty.

    MFA (str, optional):
        The MFA authentication information.

    RequestPayer (str, optional):
        Confirms that the requester knows that they will be charged for the request.

    region (str, optional):
        The AWS region to use.

    key (str, optional):
        The AWS access key to use.

    keyid (str, optional):
        The AWS secret key to use.

    profile (str, optional):
        The AWS profile to use.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_s3_bucket.empty mybucket

    """

    stuff = list_object_versions(Bucket, region=region, key=key, keyid=keyid, profile=profile)
    Delete = {}
    Delete["Objects"] = [
        {"Key": v["Key"], "VersionId": v["VersionId"]} for v in stuff.get("Versions", [])
    ]
    Delete["Objects"] += [
        {"Key": v["Key"], "VersionId": v["VersionId"]} for v in stuff.get("DeleteMarkers", [])
    ]
    if Delete["Objects"]:
        ret = delete_objects(
            Bucket,
            Delete,
            MFA=MFA,
            RequestPayer=RequestPayer,
            region=region,
            key=key,
            keyid=keyid,
            profile=profile,
        )
        failed = ret.get("failed", [])
        if failed:
            return {"deleted": False, "failed": ret[failed]}
    return {"deleted": True}


def list(region=None, key=None, keyid=None, profile=None):
    """
    List all buckets owned by the authenticated sender of the request.

    region (str, optional):
        The AWS region to use.
    key (str, optional):
        The AWS access key to use.

    keyid (str, optional):
        The AWS secret key to use.

    profile (str, optional):
        The AWS profile to use.

    CLI Example:

    .. code-block:: yaml

        Owner: {...}
        Buckets:
          - {...}
          - {...}

    """
    try:
        conn = _get_conn("s3", region=region, key=key, keyid=keyid, profile=profile)
        buckets = conn.list_buckets()
        if not bool(buckets.get("Buckets")):
            log.warning("No buckets found")
        if "ResponseMetadata" in buckets:
            del buckets["ResponseMetadata"]
        return buckets
    except ClientError as e:
        return {"error": boto3mod.get_error(e)}


def list_object_versions(
    Bucket,
    Delimiter=None,
    EncodingType=None,
    Prefix=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    List objects in a given S3 bucket.

    Bucket (str):
        The name of the S3 bucket to list object versions for.

    Delimiter (str, optional):
        A delimiter is a character you use to group keys.

    EncodingType (str, optional):
        Requests Amazon S3 to encode the object keys in the response.

    Prefix (str, optional):
        Limits the response to keys that begin with the specified prefix.

    region (str, optional):
        The AWS region to use.

    key (str, optional):
        The AWS access key to use.

    keyid (str, optional):
        The AWS secret key to use.

    profile (str, optional):
        The AWS profile to use.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_s3_bucket.list_object_versions mybucket

    """

    try:
        Versions = []
        DeleteMarkers = []
        args = {"Bucket": Bucket}
        if Delimiter:
            args["Delimiter"] = Delimiter
        if EncodingType:
            args["EncodingType"] = EncodingType
        if Prefix:
            args["Prefix"] = Prefix
        conn = _get_conn("s3", region=region, key=key, keyid=keyid, profile=profile)
        IsTruncated = True
        while IsTruncated:
            ret = conn.list_object_versions(**args)
            IsTruncated = ret.get("IsTruncated", False)
            if IsTruncated in ("True", "true", True):
                args["KeyMarker"] = ret["NextKeyMarker"]
                args["VersionIdMarker"] = ret["NextVersionIdMarker"]
            Versions += ret.get("Versions", [])
            DeleteMarkers += ret.get("DeleteMarkers", [])
        return {"Versions": Versions, "DeleteMarkers": DeleteMarkers}
    except ClientError as e:
        return {"error": boto3mod.get_error(e)}


def list_objects(
    Bucket,
    Delimiter=None,
    EncodingType=None,
    Prefix=None,
    FetchOwner=False,
    StartAfter=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    List objects in a given S3 bucket.

    Bucket (str):
        The name of the S3 bucket to list objects for.

    Delimiter (str, optional):
        A delimiter is a character you use to group keys.

    EncodingType (str, optional):
        Requests Amazon S3 to encode the object keys in the response.

    Prefix (str, optional):
        Limits the response to keys that begin with the specified prefix.

    FetchOwner (bool, optional):
        Specifies whether to include the owner field in the response.

    StartAfter (str, optional):
        Start listing after this key.

    region (str, optional):
        The AWS region to use.

    key (str, optional):
        The AWS access key to use.

    keyid (str, optional):
        The AWS secret key to use.

    profile (str, optional):
        The AWS profile to use.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_s3_bucket.list_objects mybucket

    """

    try:
        Contents = []
        args = {"Bucket": Bucket, "FetchOwner": FetchOwner}
        if Delimiter:
            args["Delimiter"] = Delimiter
        if EncodingType:
            args["EncodingType"] = EncodingType
        if Prefix:
            args["Prefix"] = Prefix
        if StartAfter:
            args["StartAfter"] = StartAfter
        conn = _get_conn("s3", region=region, key=key, keyid=keyid, profile=profile)
        IsTruncated = True
        while IsTruncated:
            ret = conn.list_objects_v2(**args)
            IsTruncated = ret.get("IsTruncated", False)
            if IsTruncated in ("True", "true", True):
                args["ContinuationToken"] = ret["NextContinuationToken"]
            Contents += ret.get("Contents", [])
        return {"Contents": Contents}
    except ClientError as e:
        return {"error": boto3mod.get_error(e)}


def put_acl(
    Bucket,
    ACL=None,
    AccessControlPolicy=None,
    GrantFullControl=None,
    GrantRead=None,
    GrantReadACP=None,
    GrantWrite=None,
    GrantWriteACP=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):  # pylint: disable=unused-argument
    """
    Given a valid config, update the ACL for a bucket.

    Bucket (str):
        The name of the S3 bucket to update the ACL for.

    ACL (str, optional):
        The canned ACL to apply to the bucket.

    AccessControlPolicy (dict, optional):
        The access control policy to apply to the bucket.

    GrantFullControl (str, optional):
        Allows grantee full control over the bucket.

    GrantRead (str, optional):
        Allows grantee to read the bucket.

    GrantReadACP (str, optional):
        Allows grantee to read the bucket's ACL.

    GrantWrite (str, optional):
        Allows grantee to write to the bucket.

    GrantWriteACP (str, optional):
        Allows grantee to write the bucket's ACL.

    region (str, optional):
        The AWS region to use.

    key (str, optional):
        The AWS access key to use.

    keyid (str, optional):
        The AWS secret key to use.

    profile (str, optional):
        The AWS profile to use.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_s3_bucket.put_acl my_bucket 'public' \\
                         GrantFullControl='emailaddress=example@example.com' \\
                         GrantRead='uri="http://acs.amazonaws.com/groups/global/AllUsers"' \\
                         GrantReadACP='emailaddress="exampl@example.com",id="2345678909876432"'

    """

    try:
        conn = _get_conn("s3", region=region, key=key, keyid=keyid, profile=profile)
        kwargs = {}
        if AccessControlPolicy is not None:
            if isinstance(AccessControlPolicy, str):
                AccessControlPolicy = salt.utils.json.loads(AccessControlPolicy)
            kwargs["AccessControlPolicy"] = AccessControlPolicy
        for arg in (
            "ACL",
            "GrantFullControl",
            "GrantRead",
            "GrantReadACP",
            "GrantWrite",
            "GrantWriteACP",
        ):
            if locals()[arg] is not None:
                kwargs[arg] = str(locals()[arg])
        conn.put_bucket_acl(Bucket=Bucket, **kwargs)
        return {"updated": True, "name": Bucket}
    except ClientError as e:
        return {"updated": False, "error": boto3mod.get_error(e)}


def put_cors(Bucket, CORSRules, region=None, key=None, keyid=None, profile=None):
    """
    Given a valid config, update the CORS rules for a bucket.

    Bucket (str):
        The name of the S3 bucket to update the CORS rules for.

    CORSRules (list):
        A list of CORS rules to apply to the bucket.

    region (str, optional):
        The AWS region to use.

    key (str, optional):
        The AWS access key to use.

    keyid (str, optional):
        The AWS secret key to use.

    profile (str, optional):
        The AWS profile to use.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_s3_bucket.put_cors my_bucket '[{\\
              "AllowedHeaders":[],\\
              "AllowedMethods":["GET"],\\
              "AllowedOrigins":["*"],\\
              "ExposeHeaders":[],\\
              "MaxAgeSeconds":123,\\
        }]'

    """

    try:
        conn = _get_conn("s3", region=region, key=key, keyid=keyid, profile=profile)
        if CORSRules is not None and isinstance(CORSRules, str):
            CORSRules = salt.utils.json.loads(CORSRules)
        conn.put_bucket_cors(Bucket=Bucket, CORSConfiguration={"CORSRules": CORSRules})
        return {"updated": True, "name": Bucket}
    except ClientError as e:
        return {"updated": False, "error": boto3mod.get_error(e)}


def put_lifecycle_configuration(Bucket, Rules, region=None, key=None, keyid=None, profile=None):
    """
    Given a valid config, update the Lifecycle rules for a bucket.

    Bucket (str):
        The name of the S3 bucket to update the Lifecycle rules for.

    Rules (list):
        A list of Lifecycle rules to apply to the bucket.

    region (str, optional):
        The AWS region to use.

    key (str, optional):
        The AWS access key to use.

    keyid (str, optional):
        The AWS secret key to use.

    profile (str, optional):
        The AWS profile to use.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_s3_bucket.put_lifecycle_configuration my_bucket '[{\\
              "Expiration": {...},\\
              "ID": "idstring",\\
              "Prefix": "prefixstring",\\
              "Status": "enabled",\\
              "Transitions": [{...},],\\
              "NoncurrentVersionTransitions": [{...},],\\
              "NoncurrentVersionExpiration": {...},\\
        }]'

    """

    try:
        conn = _get_conn("s3", region=region, key=key, keyid=keyid, profile=profile)
        if Rules is not None and isinstance(Rules, str):
            Rules = salt.utils.json.loads(Rules)
        conn.put_bucket_lifecycle_configuration(
            Bucket=Bucket, LifecycleConfiguration={"Rules": Rules}
        )
        return {"updated": True, "name": Bucket}
    except ClientError as e:
        return {"updated": False, "error": boto3mod.get_error(e)}


def put_logging(
    Bucket,
    TargetBucket=None,
    TargetPrefix=None,
    TargetGrants=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Given a valid config, update the logging parameters for a bucket.

    Bucket (str):
        The name of the S3 bucket to update the logging parameters for.

    TargetBucket (str, optional):
        The name of the S3 bucket where logs should be delivered.

    TargetPrefix (str, optional):
        The prefix for the log object keys.

    TargetGrants (list, optional):
        A list of grants for the target bucket.

    region (str, optional):
        The AWS region to use.

    key (str, optional):
        The AWS access key to use.

    keyid (str, optional):
        The AWS secret key to use.

    profile (str, optional):
        The AWS profile to use.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_s3_bucket.put_logging my_bucket log_bucket '[{...}]' prefix

    """

    try:
        conn = _get_conn("s3", region=region, key=key, keyid=keyid, profile=profile)
        logstate = {}
        targets = {
            "TargetBucket": TargetBucket,
            "TargetGrants": TargetGrants,
            "TargetPrefix": TargetPrefix,
        }
        for key, val in targets.items():
            if val is not None:
                logstate[key] = val
        if logstate:
            logstatus = {"LoggingEnabled": logstate}
        else:
            logstatus = {}
        if TargetGrants is not None and isinstance(TargetGrants, str):
            TargetGrants = salt.utils.json.loads(TargetGrants)
        conn.put_bucket_logging(Bucket=Bucket, BucketLoggingStatus=logstatus)
        return {"updated": True, "name": Bucket}
    except ClientError as e:
        return {"updated": False, "error": boto3mod.get_error(e)}


def put_notification_configuration(
    Bucket,
    TopicConfigurations=None,
    QueueConfigurations=None,
    LambdaFunctionConfigurations=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Given a valid config, update the notification parameters for a bucket.

    Bucket (str):
        The name of the S3 bucket to update the notification parameters for.

    TopicConfigurations (list, optional):
        A list of topic configurations for the bucket.

    QueueConfigurations (list, optional):
        A list of queue configurations for the bucket.

    LambdaFunctionConfigurations (list, optional):
        A list of Lambda function configurations for the bucket.

    region (str, optional):
        The AWS region to use.

    key (str, optional):
        The AWS access key to use.

    keyid (str, optional):
        The AWS secret key to use.

    profile (str, optional):
        The AWS profile to use.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_s3_bucket.put_notification_configuration my_bucket
                [{...}] \\
                [{...}] \\
                [{...}]

    """

    try:
        conn = _get_conn("s3", region=region, key=key, keyid=keyid, profile=profile)
        if TopicConfigurations is None:
            TopicConfigurations = []
        elif isinstance(TopicConfigurations, str):
            TopicConfigurations = salt.utils.json.loads(TopicConfigurations)
        if QueueConfigurations is None:
            QueueConfigurations = []
        elif isinstance(QueueConfigurations, str):
            QueueConfigurations = salt.utils.json.loads(QueueConfigurations)
        if LambdaFunctionConfigurations is None:
            LambdaFunctionConfigurations = []
        elif isinstance(LambdaFunctionConfigurations, str):
            LambdaFunctionConfigurations = salt.utils.json.loads(LambdaFunctionConfigurations)
        # TODO allow the user to use simple names & substitute ARNs for those names
        conn.put_bucket_notification_configuration(
            Bucket=Bucket,
            NotificationConfiguration={
                "TopicConfigurations": TopicConfigurations,
                "QueueConfigurations": QueueConfigurations,
                "LambdaFunctionConfigurations": LambdaFunctionConfigurations,
            },
        )
        return {"updated": True, "name": Bucket}
    except ClientError as e:
        return {"updated": False, "error": boto3mod.get_error(e)}


def put_policy(Bucket, Policy, region=None, key=None, keyid=None, profile=None):
    """
    Given a valid config, update the policy for a bucket.

    Bucket (str):
        The name of the S3 bucket to update the policy for.

    Policy (str):
        The policy to apply to the bucket.
    region (str, optional):
        The AWS region to use.

    key (str, optional):
        The AWS access key to use.

    keyid (str, optional):
        The AWS secret key to use.

    profile (str, optional):
        The AWS profile to use.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_s3_bucket.put_policy my_bucket {...}

    """

    try:
        conn = _get_conn("s3", region=region, key=key, keyid=keyid, profile=profile)
        if Policy is None:
            Policy = "{}"
        elif not isinstance(Policy, str):
            Policy = salt.utils.json.dumps(Policy)
        conn.put_bucket_policy(Bucket=Bucket, Policy=Policy)
        return {"updated": True, "name": Bucket}
    except ClientError as e:
        return {"updated": False, "error": boto3mod.get_error(e)}


def _get_role_arn(name, region=None, key=None, keyid=None, profile=None):
    if name.startswith("arn:aws:iam:"):
        return name

    account_id = __salt__["boto3_iam.get_account_id"](
        region=region, key=key, keyid=keyid, profile=profile
    )
    if profile and "region" in profile:
        region = profile["region"]
    if region is None:
        region = "us-east-1"
    return f"arn:aws:iam::{account_id}:role/{name}"


def put_replication(Bucket, Role, Rules, region=None, key=None, keyid=None, profile=None):
    """
    Given a valid config, update the replication configuration for a bucket.

    Bucket (str):
        The name of the S3 bucket to update the replication configuration for.

    Role (str):
        The IAM role ARN to use for the replication configuration.

    Rules (list):
        A list of replication rules to apply to the bucket.

    region (str, optional):
        The AWS region to use.

    key (str, optional):
        The AWS access key to use.

    keyid (str, optional):
        The AWS secret key to use.

    profile (str, optional):
        The AWS profile to use.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_s3_bucket.put_replication my_bucket my_role [...]

    """

    try:
        conn = _get_conn("s3", region=region, key=key, keyid=keyid, profile=profile)
        Role = _get_role_arn(name=Role, region=region, key=key, keyid=keyid, profile=profile)
        if Rules is None:
            Rules = []
        elif isinstance(Rules, str):
            Rules = salt.utils.json.loads(Rules)
        conn.put_bucket_replication(
            Bucket=Bucket, ReplicationConfiguration={"Role": Role, "Rules": Rules}
        )
        return {"updated": True, "name": Bucket}
    except ClientError as e:
        return {"updated": False, "error": boto3mod.get_error(e)}


def put_request_payment(Bucket, Payer, region=None, key=None, keyid=None, profile=None):
    """
    Given a valid config, update the request payment configuration for a bucket.

    Bucket (str):
        The name of the S3 bucket to update the request payment configuration for.

    Payer (str):
        The payer for the request payment configuration. Valid values are "BucketOwner" and "Requester".

    region (str, optional):
        The AWS region to use.

    key (str, optional):
        The AWS access key to use.

    keyid (str, optional):
        The AWS secret key to use.

    profile (str, optional):
        The AWS profile to use.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_s3_bucket.put_request_payment my_bucket Requester

    """

    try:
        conn = _get_conn("s3", region=region, key=key, keyid=keyid, profile=profile)
        conn.put_bucket_request_payment(Bucket=Bucket, RequestPaymentConfiguration={"Payer": Payer})
        return {"updated": True, "name": Bucket}
    except ClientError as e:
        return {"updated": False, "error": boto3mod.get_error(e)}


def put_tagging(Bucket, region=None, key=None, keyid=None, profile=None, **kwargs):
    """
    Given a valid config, update the tags for a bucket.

    Bucket (str):
        The name of the S3 bucket to update the tags for.

    region (str, optional):
        The AWS region to use.

    key (str, optional):
        The AWS access key to use.

    keyid (str, optional):
        The AWS secret key to use.

    profile (str, optional):
        The AWS profile to use.

    **kwargs:
        The tags to apply to the bucket as key-value pairs.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_s3_bucket.put_tagging my_bucket my_role [...]

    """

    try:
        conn = _get_conn("s3", region=region, key=key, keyid=keyid, profile=profile)
        tagslist = []
        for k, v in kwargs.items():
            if str(k).startswith("__"):
                continue
            tagslist.append({"Key": str(k), "Value": str(v)})
        conn.put_bucket_tagging(Bucket=Bucket, Tagging={"TagSet": tagslist})
        return {"updated": True, "name": Bucket}
    except ClientError as e:
        return {"updated": False, "error": boto3mod.get_error(e)}


def put_versioning(
    Bucket,
    Status,
    MFADelete=None,
    MFA=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Given a valid config, update the versioning configuration for a bucket.

    Bucket (str):
        The name of the S3 bucket to update the versioning configuration for.

    Status (str):
        The versioning status to apply to the bucket. Valid values are "Enabled" and "Suspended".

    MFADelete (str, optional):
        The MFA delete status to apply to the bucket. Valid values are "Enabled" and "Disabled".

    MFA (str, optional):
        The MFA authentication code to use when updating the versioning configuration.

    region (str, optional):
        The AWS region to use.

    key (str, optional):
        The AWS access key to use.

    keyid (str, optional):
        The AWS secret key to use.

    profile (str, optional):
        The AWS profile to use.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_s3_bucket.put_versioning my_bucket Enabled

    """

    try:
        conn = _get_conn("s3", region=region, key=key, keyid=keyid, profile=profile)
        VersioningConfiguration = {"Status": Status}
        if MFADelete is not None:
            VersioningConfiguration["MFADelete"] = MFADelete
        kwargs = {}
        if MFA is not None:
            kwargs["MFA"] = MFA
        conn.put_bucket_versioning(
            Bucket=Bucket, VersioningConfiguration=VersioningConfiguration, **kwargs
        )
        return {"updated": True, "name": Bucket}
    except ClientError as e:
        return {"updated": False, "error": boto3mod.get_error(e)}


def put_website(
    Bucket,
    ErrorDocument=None,
    IndexDocument=None,
    RedirectAllRequestsTo=None,
    RoutingRules=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):  # pylint: disable=unused-argument
    """
    Given a valid config, update the website configuration for a bucket.

    Bucket (str):
        The name of the S3 bucket to update the website configuration for.

    ErrorDocument (dict, optional):
        The error document configuration for the website.

    IndexDocument (dict, optional):
        The index document configuration for the website.

    RedirectAllRequestsTo (dict, optional):
        The redirect all requests to configuration for the website.

    RoutingRules (list, optional):
        The routing rules configuration for the website.

    region (str, optional):
        The AWS region to use.

    key (str, optional):
        The AWS access key to use.

    keyid (str, optional):
        The AWS secret key to use.

    profile (str, optional):
        The AWS profile to use.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_s3_bucket.put_website my_bucket IndexDocument='{"Suffix":"index.html"}'

    """

    try:
        conn = _get_conn("s3", region=region, key=key, keyid=keyid, profile=profile)
        WebsiteConfiguration = {}
        for key in (
            "ErrorDocument",
            "IndexDocument",
            "RedirectAllRequestsTo",
            "RoutingRules",
        ):
            val = locals()[key]
            if val is not None:
                if isinstance(val, str):
                    WebsiteConfiguration[key] = salt.utils.json.loads(val)
                else:
                    WebsiteConfiguration[key] = val
        conn.put_bucket_website(Bucket=Bucket, WebsiteConfiguration=WebsiteConfiguration)
        return {"updated": True, "name": Bucket}
    except ClientError as e:
        return {"updated": False, "error": boto3mod.get_error(e)}


def delete_cors(Bucket, region=None, key=None, keyid=None, profile=None):
    """
    Delete the CORS configuration for the given bucket.

    Bucket (str):
        The name of the S3 bucket to delete the CORS configuration for.

    region (str, optional):
        The AWS region to use.

    key (str, optional):
        The AWS access key to use.

    keyid (str, optional):
        The AWS secret key to use.

    profile (str, optional):
        The AWS profile to use.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_s3_bucket.delete_cors my_bucket

    """

    try:
        conn = _get_conn("s3", region=region, key=key, keyid=keyid, profile=profile)
        conn.delete_bucket_cors(Bucket=Bucket)
        return {"deleted": True, "name": Bucket}
    except ClientError as e:
        return {"deleted": False, "error": boto3mod.get_error(e)}


def delete_lifecycle_configuration(Bucket, region=None, key=None, keyid=None, profile=None):
    """
    Delete the lifecycle configuration for the given bucket.

    Bucket (str):
        The name of the S3 bucket to delete the lifecycle configuration for.

    region (str, optional):
        The AWS region to use.

    key (str, optional):
        The AWS access key to use.

    keyid (str, optional):
        The AWS secret key to use.

    profile (str, optional):
        The AWS profile to use.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_s3_bucket.delete_lifecycle_configuration my_bucket

    """

    try:
        conn = _get_conn("s3", region=region, key=key, keyid=keyid, profile=profile)
        conn.delete_bucket_lifecycle(Bucket=Bucket)
        return {"deleted": True, "name": Bucket}
    except ClientError as e:
        return {"deleted": False, "error": boto3mod.get_error(e)}


def delete_policy(Bucket, region=None, key=None, keyid=None, profile=None):
    """
    Delete the policy from the given bucket.

    Bucket (str):
        The name of the S3 bucket to delete the policy from.

    region (str, optional):
        The AWS region to use.

    key (str, optional):
        The AWS access key to use.

    keyid (str, optional):
        The AWS secret key to use.

    profile (str, optional):
        The AWS profile to use.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_s3_bucket.delete_policy my_bucket

    """

    try:
        conn = _get_conn("s3", region=region, key=key, keyid=keyid, profile=profile)
        conn.delete_bucket_policy(Bucket=Bucket)
        return {"deleted": True, "name": Bucket}
    except ClientError as e:
        return {"deleted": False, "error": boto3mod.get_error(e)}


def delete_replication(Bucket, region=None, key=None, keyid=None, profile=None):
    """
    Delete the replication config from the given bucket.

    Bucket (str):
        The name of the S3 bucket to delete the replication config from.

    region (str, optional):
        The AWS region to use.

    key (str, optional):
        The AWS access key to use.

    keyid (str, optional):
        The AWS secret key to use.

    profile (str, optional):
        The AWS profile to use.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_s3_bucket.delete_replication my_bucket

    """

    try:
        conn = _get_conn("s3", region=region, key=key, keyid=keyid, profile=profile)
        conn.delete_bucket_replication(Bucket=Bucket)
        return {"deleted": True, "name": Bucket}
    except ClientError as e:
        return {"deleted": False, "error": boto3mod.get_error(e)}


def delete_tagging(Bucket, region=None, key=None, keyid=None, profile=None):
    """
    Delete the tags from the given bucket.

    Bucket (str):
        The name of the S3 bucket to delete the tags from.

    region (str, optional):
        The AWS region to use.

    key (str, optional):
        The AWS access key to use.

    keyid (str, optional):
        The AWS secret key to use.

    profile (str, optional):
        The AWS profile to use.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_s3_bucket.delete_tagging my_bucket

    """

    try:
        conn = _get_conn("s3", region=region, key=key, keyid=keyid, profile=profile)
        conn.delete_bucket_tagging(Bucket=Bucket)
        return {"deleted": True, "name": Bucket}
    except ClientError as e:
        return {"deleted": False, "error": boto3mod.get_error(e)}


def delete_website(Bucket, region=None, key=None, keyid=None, profile=None):
    """
    Remove the website configuration from the given bucket.

    Bucket (str):
        The name of the S3 bucket to remove the website configuration from.

    region (str, optional):
        The AWS region to use.

    key (str, optional):
        The AWS access key to use.

    keyid (str, optional):
        The AWS secret key to use.

    profile (str, optional):
        The AWS profile to use.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_s3_bucket.delete_website my_bucket

    """

    try:
        conn = _get_conn("s3", region=region, key=key, keyid=keyid, profile=profile)
        conn.delete_bucket_website(Bucket=Bucket)
        return {"deleted": True, "name": Bucket}
    except ClientError as e:
        return {"deleted": False, "error": boto3mod.get_error(e)}
