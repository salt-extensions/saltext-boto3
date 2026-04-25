"""
Connection module for Amazon Data Pipeline using boto3.
=======================================================

    Renamed from ``boto_datapipeline`` to ``boto3_datapipeline`` and rewritten
    to use the boto3 ``datapipeline`` client APIs directly via
    :py:mod:`saltext.boto3.utils.boto3mod`.  The legacy boto2 code path
    (object-style access, retry loops) has been removed.

:depends:
  - boto3 >= 1.28.0
  - botocore >= 1.31.0

:configuration: This module accepts explicit Data Pipeline credentials but can
    also utilize IAM roles assigned to the instance through Instance Profiles.
    Dynamic credentials are then automatically obtained from AWS API and no
    further configuration is necessary. More Information available at:

    .. code-block:: text

        http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html

    If IAM roles are not used you need to specify them either in the minion's
    config file or as a profile. For example, to specify them in the minion's
    config file:

.. code-block:: yaml

    datapipeline.keyid: GKTADJGHEIQSXMKKRBJ08H
    datapipeline.key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs

A region may also be specified in the configuration:

.. code-block:: yaml

    datapipeline.region: us-east-1

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

try:
    from botocore.exceptions import BotoCoreError
    from botocore.exceptions import ClientError

    logging.getLogger("boto3").setLevel(logging.CRITICAL)
    logging.getLogger("botocore").setLevel(logging.CRITICAL)
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

log = logging.getLogger(__name__)

__virtualname__ = "boto3_datapipeline"


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
        "The boto3_datapipeline module could not be loaded: boto3 is not available.",
    )


def activate_pipeline(pipeline_id, region=None, key=None, keyid=None, profile=None):
    """
    Start processing pipeline tasks. This function is idempotent.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_datapipeline.activate_pipeline my_pipeline_id
    """
    r = {}
    try:
        client = _get_conn("datapipeline", region=region, key=key, keyid=keyid, profile=profile)
        client.activate_pipeline(pipelineId=pipeline_id)
        r["result"] = True
    except (BotoCoreError, ClientError) as e:
        r["error"] = str(e)
    return r


def create_pipeline(
    name, unique_id, description="", region=None, key=None, keyid=None, profile=None
):
    """
    Create a new, empty pipeline. This function is idempotent.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_datapipeline.create_pipeline my_name my_unique_id
    """
    r = {}
    try:
        client = _get_conn("datapipeline", region=region, key=key, keyid=keyid, profile=profile)
        response = client.create_pipeline(
            name=name,
            uniqueId=unique_id,
            description=description,
        )
        r["result"] = response["pipelineId"]
    except (BotoCoreError, ClientError) as e:
        r["error"] = str(e)
    return r


def delete_pipeline(pipeline_id, region=None, key=None, keyid=None, profile=None):
    """
    Delete a pipeline, its pipeline definition, and its run history.
    This function is idempotent.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_datapipeline.delete_pipeline my_pipeline_id
    """
    r = {}
    try:
        client = _get_conn("datapipeline", region=region, key=key, keyid=keyid, profile=profile)
        client.delete_pipeline(pipelineId=pipeline_id)
        r["result"] = True
    except (BotoCoreError, ClientError) as e:
        r["error"] = str(e)
    return r


def describe_pipelines(pipeline_ids, region=None, key=None, keyid=None, profile=None):
    """
    Retrieve metadata about one or more pipelines.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_datapipeline.describe_pipelines ['my_pipeline_id']
    """
    r = {}
    try:
        client = _get_conn("datapipeline", region=region, key=key, keyid=keyid, profile=profile)
        r["result"] = client.describe_pipelines(pipelineIds=pipeline_ids)
    except (BotoCoreError, ClientError) as e:
        r["error"] = str(e)
    return r


def get_pipeline_definition(
    pipeline_id, version="latest", region=None, key=None, keyid=None, profile=None
):
    """
    Get the definition of the specified pipeline.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_datapipeline.get_pipeline_definition my_pipeline_id
    """
    r = {}
    try:
        client = _get_conn("datapipeline", region=region, key=key, keyid=keyid, profile=profile)
        r["result"] = client.get_pipeline_definition(
            pipelineId=pipeline_id,
            version=version,
        )
    except (BotoCoreError, ClientError) as e:
        r["error"] = str(e)
    return r


def list_pipelines(region=None, key=None, keyid=None, profile=None):
    """
    Get a list of pipeline ids and names for all pipelines.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_datapipeline.list_pipelines profile=myprofile
    """
    r = {}
    try:
        client = _get_conn("datapipeline", region=region, key=key, keyid=keyid, profile=profile)
        paginator = client.get_paginator("list_pipelines")
        pipelines = []
        for page in paginator.paginate():
            pipelines += page["pipelineIdList"]
        r["result"] = pipelines
    except (BotoCoreError, ClientError) as e:
        r["error"] = str(e)
    return r


def pipeline_id_from_name(name, region=None, key=None, keyid=None, profile=None):
    """
    Get the pipeline id, if it exists, for the given name.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_datapipeline.pipeline_id_from_name my_pipeline_name
    """
    r = {}
    result_pipelines = list_pipelines(region=region, key=key, keyid=keyid, profile=profile)
    if "error" in result_pipelines:
        return result_pipelines

    for pipeline in result_pipelines["result"]:
        if pipeline["name"] == name:
            r["result"] = pipeline["id"]
            return r
    r["error"] = f"No pipeline found with name={name}"
    return r


def put_pipeline_definition(
    pipeline_id,
    pipeline_objects,
    parameter_objects=None,
    parameter_values=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Add tasks, schedules, and preconditions to the specified pipeline. This
    function is idempotent and will replace an existing definition.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_datapipeline.put_pipeline_definition my_pipeline_id my_pipeline_objects
    """
    parameter_objects = parameter_objects or []
    parameter_values = parameter_values or []
    r = {}
    try:
        client = _get_conn("datapipeline", region=region, key=key, keyid=keyid, profile=profile)
        response = client.put_pipeline_definition(
            pipelineId=pipeline_id,
            pipelineObjects=pipeline_objects,
            parameterObjects=parameter_objects,
            parameterValues=parameter_values,
        )
        if response["errored"]:
            r["error"] = response["validationErrors"]
        else:
            r["result"] = response
    except (BotoCoreError, ClientError) as e:
        r["error"] = str(e)
    return r
