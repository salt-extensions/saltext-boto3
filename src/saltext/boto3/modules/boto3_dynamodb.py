"""
Connection module for Amazon DynamoDB using boto3.
==================================================

    Renamed from ``boto_dynamodb`` to ``boto3_dynamodb`` and rewritten
    to use the boto3 ``dynamodb`` client APIs directly via
    :py:mod:`saltext.boto3.utils.boto3mod`.  The legacy boto2 code path
    (object-style access, retry loops) has been removed.

:depends:
  - boto3 >= 1.28.0
  - botocore >= 1.31.0

:configuration: This module accepts explicit DynamoDB credentials but can
    also utilize IAM roles assigned to the instance through Instance Profiles.
    Dynamic credentials are then automatically obtained from AWS API and no
    further configuration is necessary. More Information available at:

    .. code-block:: text

        http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html

    If IAM roles are not used you need to specify them either in the minion's
    config file or as a profile. For example, to specify them in the minion's
    config file:

.. code-block:: yaml

    dynamodb.keyid: GKTADJGHEIQSXMKKRBJ08H
    dynamodb.key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs

A region may also be specified in the configuration:

.. code-block:: yaml

    dynamodb.region: us-east-1

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
import time

from salt.exceptions import SaltInvocationError

from saltext.boto3.utils import boto3mod

try:
    from botocore.exceptions import ClientError
    from botocore.exceptions import ParamValidationError

    logging.getLogger("boto3").setLevel(logging.CRITICAL)
    logging.getLogger("botocore").setLevel(logging.CRITICAL)
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

log = logging.getLogger(__name__)

__virtualname__ = "boto3_dynamodb"

_MAX_WAIT_ATTEMPTS = 30


def __virtual__():
    """
    Only load if boto3 is available.
    """
    if HAS_BOTO3:
        return __virtualname__
    return (
        False,
        "The boto3_dynamodb module could not be loaded: boto3 is not available.",
    )


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


def list_tags_of_resource(resource_arn, region=None, key=None, keyid=None, profile=None):
    """
    Return a dictionary of all tags currently attached to the given resource.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_dynamodb.list_tags_of_resource

    """
    conn = _get_conn("dynamodb", region, key, keyid, profile)
    retries = 10
    sleep = 6
    tags = []
    while retries:
        try:
            marker = ""
            while marker is not None:
                ret = conn.list_tags_of_resource(ResourceArn=resource_arn, NextToken=marker)
                tags += ret.get("Tags", [])
                marker = ret.get("NextToken")
            return {tag["Key"]: tag["Value"] for tag in tags}
        except ParamValidationError as err:
            raise SaltInvocationError(str(err)) from err
        except ClientError as err:
            if retries and err.response.get("Error", {}).get("Code") == "Throttling":
                retries -= 1
                log.debug("Throttled by AWS API, retrying in %s seconds...", sleep)
                time.sleep(sleep)
                continue
            log.error("Failed to list tags for resource %s: %s", resource_arn, err)
            return False


def tag_resource(resource_arn, tags, region=None, key=None, keyid=None, profile=None):
    """
    Set the given tags (dict or list of ``{'Key':..., 'Value':...}``) on the
    given resource.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_dynamodb.tag_resource

    """
    conn = _get_conn("dynamodb", region, key, keyid, profile)
    retries = 10
    sleep = 6
    if isinstance(tags, dict):
        tags = [{"Key": k, "Value": v} for k, v in tags.items()]
    while retries:
        try:
            conn.tag_resource(ResourceArn=resource_arn, Tags=tags)
            return True
        except ParamValidationError as err:
            raise SaltInvocationError(str(err)) from err
        except ClientError as err:
            if retries and err.response.get("Error", {}).get("Code") == "Throttling":
                retries -= 1
                time.sleep(sleep)
                continue
            log.error("Failed to set tags on resource %s: %s", resource_arn, err)
            return False


def untag_resource(resource_arn, tag_keys, region=None, key=None, keyid=None, profile=None):
    """
    Remove the given tag keys from the given resource.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_dynamodb.untag_resource

    """
    conn = _get_conn("dynamodb", region, key, keyid, profile)
    retries = 10
    sleep = 6
    while retries:
        try:
            conn.untag_resource(ResourceArn=resource_arn, TagKeys=tag_keys)
            return True
        except ParamValidationError as err:
            raise SaltInvocationError(str(err)) from err
        except ClientError as err:
            if retries and err.response.get("Error", {}).get("Code") == "Throttling":
                retries -= 1
                time.sleep(sleep)
                continue
            log.error("Failed to remove tags from resource %s: %s", resource_arn, err)
            return False


def exists(table_name, region=None, key=None, keyid=None, profile=None):
    """
    Check whether the given DynamoDB table exists.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_dynamodb.exists

    """
    conn = _get_conn("dynamodb", region, key, keyid, profile)
    try:
        conn.describe_table(TableName=table_name)
    except ClientError as err:
        if err.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
            return False
        raise
    return True


def describe(table_name, region=None, key=None, keyid=None, profile=None):
    """
    Describe a DynamoDB table.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_dynamodb.describe

    """
    conn = _get_conn("dynamodb", region, key, keyid, profile)
    return conn.describe_table(TableName=table_name)


def create_table(
    table_name,
    region=None,
    key=None,
    keyid=None,
    profile=None,
    read_capacity_units=None,
    write_capacity_units=None,
    hash_key=None,
    hash_key_data_type=None,
    range_key=None,
    range_key_data_type=None,
    local_indexes=None,
    global_indexes=None,
):
    """
    Create a DynamoDB table.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_dynamodb.create_table

    """
    if not hash_key:
        raise SaltInvocationError("hash_key is required to create a table.")

    attribute_types = {hash_key: hash_key_data_type}
    key_schema = [{"AttributeName": hash_key, "KeyType": "HASH"}]
    if range_key:
        attribute_types[range_key] = range_key_data_type
        key_schema.append({"AttributeName": range_key, "KeyType": "RANGE"})

    local_specs = []
    if local_indexes:
        for index in local_indexes:
            spec = extract_index(index, global_index=False)
            local_specs.append(spec)
            for ks in spec["KeySchema"]:
                attribute_types.setdefault(ks["AttributeName"], _find_attr_type(index))

    global_specs = []
    if global_indexes:
        for index in global_indexes:
            spec = extract_index(index, global_index=True)
            global_specs.append(spec)
            for ks in spec["KeySchema"]:
                attribute_types.setdefault(ks["AttributeName"], _find_attr_type(index))

    attribute_definitions = [
        {"AttributeName": n, "AttributeType": t} for n, t in attribute_types.items()
    ]

    params = {
        "TableName": table_name,
        "AttributeDefinitions": attribute_definitions,
        "KeySchema": key_schema,
        "ProvisionedThroughput": {
            "ReadCapacityUnits": read_capacity_units,
            "WriteCapacityUnits": write_capacity_units,
        },
    }
    if local_specs:
        params["LocalSecondaryIndexes"] = [
            {k: v for k, v in s.items() if not k.startswith("_")} for s in local_specs
        ]
    if global_specs:
        params["GlobalSecondaryIndexes"] = [
            {k: v for k, v in s.items() if not k.startswith("_")} for s in global_specs
        ]

    conn = _get_conn("dynamodb", region, key, keyid, profile)
    try:
        conn.create_table(**params)
    except ClientError as err:
        log.error("Failed to create table %s: %s", table_name, err)
        return False

    for _ in range(_MAX_WAIT_ATTEMPTS):
        if exists(table_name, region, key, keyid, profile):
            return True
        time.sleep(1)
    return False


def delete(table_name, region=None, key=None, keyid=None, profile=None):
    """
    Delete a DynamoDB table.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_dynamodb.delete

    """
    conn = _get_conn("dynamodb", region, key, keyid, profile)
    try:
        conn.delete_table(TableName=table_name)
    except ClientError as err:
        log.error("Failed to delete table %s: %s", table_name, err)
        return False

    for _ in range(_MAX_WAIT_ATTEMPTS):
        if not exists(table_name, region, key, keyid, profile):
            return True
        time.sleep(1)
    return False


def update(
    table_name,
    throughput=None,
    global_indexes=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Update the provisioned throughput or global secondary indexes of a table.

    throughput
        A dict with keys ``read`` and ``write``.

    global_indexes
        A list of ``GlobalSecondaryIndexUpdates`` entries (passed through to
        the boto3 API).

    CLI Example:

    .. code-block:: bash

        salt-call boto3_dynamodb.update

    """
    params = {"TableName": table_name}
    if throughput is not None:
        params["ProvisionedThroughput"] = {
            "ReadCapacityUnits": throughput["read"],
            "WriteCapacityUnits": throughput["write"],
        }
    if global_indexes is not None:
        params["GlobalSecondaryIndexUpdates"] = global_indexes

    conn = _get_conn("dynamodb", region, key, keyid, profile)
    try:
        conn.update_table(**params)
    except ClientError as err:
        log.error("Failed to update table %s: %s", table_name, err)
        return False
    return True


def create_global_secondary_index(
    table_name, global_index, region=None, key=None, keyid=None, profile=None
):
    """
    Create a single global secondary index. ``global_index`` is an AWS-format
    dict with ``IndexName``, ``KeySchema``, ``Projection``,
    ``ProvisionedThroughput`` (as returned by :func:`extract_index`).

    CLI Example:

    .. code-block:: bash

        salt-call boto3_dynamodb.create_global_secondary_index

    """
    attribute_definitions = []
    seen = set()
    for ks in global_index.get("KeySchema", []):
        name = ks["AttributeName"]
        if name in seen:
            continue
        seen.add(name)
        attribute_definitions.append(
            {
                "AttributeName": name,
                "AttributeType": global_index.get("_AttributeTypes", {}).get(name, "S"),
            }
        )

    params = {
        "TableName": table_name,
        "GlobalSecondaryIndexUpdates": [
            {"Create": {k: v for k, v in global_index.items() if not k.startswith("_")}}
        ],
    }
    if attribute_definitions:
        params["AttributeDefinitions"] = attribute_definitions

    conn = _get_conn("dynamodb", region, key, keyid, profile)
    try:
        conn.update_table(**params)
    except ClientError as err:
        log.error("Failed to create GSI on %s: %s", table_name, err)
        return False
    return True


def update_global_secondary_index(
    table_name, global_indexes, region=None, key=None, keyid=None, profile=None
):
    """
    Update the provisioned throughput of the given global secondary indexes.

    global_indexes
        A dict mapping index names to ``{'read': R, 'write': W}``.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_dynamodb.update_global_secondary_index

    """
    updates = []
    for index_name, tp in global_indexes.items():
        updates.append(
            {
                "Update": {
                    "IndexName": index_name,
                    "ProvisionedThroughput": {
                        "ReadCapacityUnits": tp["read"],
                        "WriteCapacityUnits": tp["write"],
                    },
                }
            }
        )

    conn = _get_conn("dynamodb", region, key, keyid, profile)
    try:
        conn.update_table(TableName=table_name, GlobalSecondaryIndexUpdates=updates)
    except ClientError as err:
        log.error("Failed to update GSIs on %s: %s", table_name, err)
        return False
    return True


def _find_attr_type(index_data):
    """
    Extract hash/range attribute types from the legacy index config shape.
    Returns a fallback of 'S' if not found.
    """
    # Retained for AttributeDefinitions when building create_table; the
    # :func:`extract_index` result stashes attribute types under
    # ``_AttributeTypes`` which we consult here if passed directly.
    if isinstance(index_data, dict) and "_AttributeTypes" in index_data:
        for _, t in index_data["_AttributeTypes"].items():
            return t
    return "S"


def extract_index(index_data, global_index=False):
    """
    Parse a legacy-style index specification (dict or list of OrderedDicts,
    keyed by an outer ``index`` key with a list of single-key dicts) and
    return an AWS API-shape dict suitable for the DynamoDB ``create_table`` or
    ``update_table`` calls.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_dynamodb.extract_index

    """
    parsed_data = {}

    # Legacy shape: {<outer>: [{'name': 'x'}, {'hash_key': 'id'}, ...]}
    for _, value in index_data.items():
        if not isinstance(value, list):
            continue
        for item in value:
            for field, data in item.items():
                if field == "keys_only":
                    parsed_data["keys_only"] = bool(data)
                else:
                    parsed_data[field] = data

    name = parsed_data.get("name")
    hash_key = parsed_data.get("hash_key")
    if not name or not hash_key:
        raise SaltInvocationError("Index requires both 'name' and 'hash_key' fields.")

    hash_type = parsed_data.get("hash_key_data_type", "S")
    attribute_types = {hash_key: hash_type}

    key_schema = [{"AttributeName": hash_key, "KeyType": "HASH"}]
    range_key = parsed_data.get("range_key")
    if range_key:
        range_type = parsed_data.get("range_key_data_type", "S")
        attribute_types[range_key] = range_type
        key_schema.append({"AttributeName": range_key, "KeyType": "RANGE"})

    if parsed_data.get("keys_only") and parsed_data.get("includes"):
        raise SaltInvocationError("Only one type of GSI projection can be used.")

    if parsed_data.get("includes"):
        projection = {
            "ProjectionType": "INCLUDE",
            "NonKeyAttributes": parsed_data["includes"],
        }
    elif parsed_data.get("keys_only"):
        projection = {"ProjectionType": "KEYS_ONLY"}
    else:
        projection = {"ProjectionType": "ALL"}

    spec = {
        "IndexName": name,
        "KeySchema": key_schema,
        "Projection": projection,
    }
    if global_index:
        read = parsed_data.get("read_capacity_units")
        write = parsed_data.get("write_capacity_units")
        if read is not None and write is not None:
            spec["ProvisionedThroughput"] = {
                "ReadCapacityUnits": read,
                "WriteCapacityUnits": write,
            }
    # Stash attribute types for callers that need them when building
    # AttributeDefinitions (e.g. create_table, create_global_secondary_index).
    spec["_AttributeTypes"] = attribute_types
    return spec
