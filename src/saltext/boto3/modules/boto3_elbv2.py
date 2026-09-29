"""
Connection module for Amazon Elastic Load Balancing v2 using boto3.
===================================================================

    Renamed from ``boto_elbv2`` to ``boto3_elbv2`` and rewritten to use the
    boto3 ``elbv2`` client APIs directly via
    :py:mod:`saltext.boto3.utils.boto3mod`.  The legacy boto2 code path
    (object-style access, retry loops) has been removed.

:depends:
  - boto3 >= 1.28.0
  - botocore >= 1.31.0

:configuration: This module accepts explicit Elastic Load Balancer (ELB) credentials but can
    also utilize IAM roles assigned to the instance through Instance Profiles.
    Dynamic credentials are then automatically obtained from AWS API and no
    further configuration is necessary. More Information available at:

    .. code-block:: text

        http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html

    If IAM roles are not used you need to specify them either in the minion's
    config file or as a profile. For example, to specify them in the minion's
    config file:

.. code-block:: yaml

    elb.keyid: GKTADJGHEIQSXMKKRBJ08H
    elb.key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs

A region may also be specified in the configuration:

.. code-block:: yaml

    elb.region: us-east-1

It's also possible to specify key, keyid and region via a profile, either
as a passed in dict, or as a string to pull from pillars or minion config:

.. code-block:: yaml

    myprofile:
        keyid: GKTADJGHEIQSXMKKRBJ08H
        key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs
        region: us-east-1

.. CLI Example:

.. code-block:: bash

    salt '*' boto3_elbv2.create_target_group name='my-target-group' protocol='HTTP' port=80 vpc_id='vpc-1'

.. versionadded:: 1.0.0
"""

import logging

from saltext.boto3.utils import boto3mod

try:
    from botocore.exceptions import ClientError

    logging.getLogger("boto3").setLevel(logging.CRITICAL)
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

log = logging.getLogger(__name__)

__virtualname__ = "boto3_elbv2"


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
    return (False, "The boto3_elbv2 module could not be loaded: boto3 is not available.")


def create_target_group(
    name,
    protocol,
    port,
    vpc_id,
    region=None,
    key=None,
    keyid=None,
    profile=None,
    health_check_protocol="HTTP",
    health_check_port="traffic-port",
    health_check_path="/",
    health_check_interval_seconds=30,
    health_check_timeout_seconds=5,
    healthy_threshold_count=5,
    unhealthy_threshold_count=2,
):
    """
    Create target group if not present.

    name (str):
        The name of the target group.

    protocol (str):
        The protocol to use for routing traffic to the targets.

    port (int):
        The port on which the targets receive traffic. This port is used unless
        you specify a port override when registering the target.

    vpc_id (str):
        The identifier of the virtual private cloud (VPC).

    region (str, optional):
        The AWS region where the target group is located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    health_check_protocol (str, optional):
        The protocol the load balancer uses when performing health check on
        targets. The default is the HTTP protocol.

    health_check_port (str, optional):
        The port the load balancer uses when performing health checks on
        targets. The default is 'traffic-port', which indicates the port on which each
        target receives traffic from the load balancer.

    health_check_path (str, optional):
        The ping path that is the destination on the targets for health
        checks. The default is /.

    health_check_interval_seconds (int, optional):
        The approximate amount of time, in seconds, between health checks
        of an individual target. The default is 30 seconds.

    health_check_timeout_seconds (int, optional):
        The amount of time, in seconds, during which no response from a
        target means a failed health check. The default is 5 seconds.

    healthy_threshold_count (int, optional):
        The number of consecutive health checks successes required before
        considering an unhealthy target healthy. The default is 5.

    unhealthy_threshold_count (int, optional):
        The number of consecutive health check failures required before
        considering a target unhealthy. The default is 2.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_elbv2.create_target_group learn1give1 protocol=HTTP port=54006 vpc_id=vpc-deadbeef
    """

    conn = _get_conn("elbv2", region=region, key=key, keyid=keyid, profile=profile)
    if target_group_exists(name=name, region=region, key=key, keyid=keyid, profile=profile):
        return True

    try:
        alb = conn.create_target_group(
            Name=name,
            Protocol=protocol,
            Port=port,
            VpcId=vpc_id,
            HealthCheckProtocol=health_check_protocol,
            HealthCheckPort=health_check_port,
            HealthCheckPath=health_check_path,
            HealthCheckIntervalSeconds=health_check_interval_seconds,
            HealthCheckTimeoutSeconds=health_check_timeout_seconds,
            HealthyThresholdCount=healthy_threshold_count,
            UnhealthyThresholdCount=unhealthy_threshold_count,
        )
        if alb:
            log.info("Created ALB %s: %s", name, alb["TargetGroups"][0]["TargetGroupArn"])
            return True
        else:
            log.error("Failed to create ALB %s", name)
            return False
    except ClientError as error:
        log.error(
            "Failed to create ALB %s: %s: %s",
            name,
            error.response["Error"]["Code"],
            error.response["Error"]["Message"],
            exc_info_on_loglevel=logging.DEBUG,
        )


def delete_target_group(name=None, arn=None, region=None, key=None, keyid=None, profile=None):
    """
    Delete target group. Exactly one of ``name`` or ``arn`` must be provided.

    name (str, optional):
        The name of the target group.

    arn (str, optional):
        The ARN of the target group.

    region (str, optional):
        The AWS region where the target group is located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_elbv2.delete_target_group name=mytg region=us-east-1
        salt myminion boto3_elbv2.delete_target_group arn=arn:aws:elasticloadbalancing:us-west-2:644138682826:targetgroup/learn1give1-api/414788a16b5cf163

    .. versionchanged:: 2.0.0
        Added ``arn`` parameter; ``name`` is now optional so either identifier may be used.
        ARN is resolved via ``describe_target_group`` rather than accepted directly as the positional argument.
    """
    if not boto3mod.exactly_one([name, arn]):
        log.error("delete_target_group requires exactly one of 'name' or 'arn'")
        return False

    if not target_group_exists(
        name=name, arn=arn, region=region, key=key, keyid=keyid, profile=profile
    ):
        return True

    tg = describe_target_group(
        name=name, arn=arn, region=region, key=key, keyid=keyid, profile=profile
    )
    if tg is None:
        return False
    tg_arn = tg["TargetGroupArn"]

    conn = _get_conn("elbv2", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.delete_target_group(TargetGroupArn=tg_arn)
        log.info("Deleted target group %s (ARN %s)", name or arn, tg_arn)
        return True
    except ClientError as error:
        log.error(
            "Failed to delete target group %s: %s: %s",
            name or arn,
            error.response["Error"]["Code"],
            error.response["Error"]["Message"],
            exc_info_on_loglevel=logging.DEBUG,
        )
        return False


def describe_target_group(name=None, arn=None, region=None, key=None, keyid=None, profile=None):
    """
    Describe a target group by name or ARN. Exactly one of ``name`` or ``arn`` must be
    provided. Returns the target group dict or ``None`` if not found.

    name (str, optional):
        The name of the target group.

    arn (str, optional):
        The ARN of the target group.

    region (str, optional):
        The AWS region where the target group is located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_elbv2.describe_target_group name=mytg region=us-east-1
        salt myminion boto3_elbv2.describe_target_group arn=arn:aws:elasticloadbalancing:... region=us-east-1

    .. versionadded:: 2.0.0
    """
    if not boto3mod.exactly_one([name, arn]):
        log.error("describe_target_group requires exactly one of 'name' or 'arn'")
        return None

    conn = _get_conn("elbv2", region=region, key=key, keyid=keyid, profile=profile)

    try:
        if arn:
            resp = conn.describe_target_groups(TargetGroupArns=[arn])
        else:
            resp = conn.describe_target_groups(Names=[name])
        groups = resp.get("TargetGroups", [])
        return groups[0] if groups else None
    except ClientError as e:
        log.error("Failed to describe target group %s: %s", arn or name, e)
        return None


def target_group_exists(name=None, arn=None, region=None, key=None, keyid=None, profile=None):
    """
    Check to see if a target group exists. Exactly one of ``name`` or ``arn`` must be provided.

    name (str, optional):
        The name of the target group.

    arn (str, optional):
        The ARN of the target group.

    region (str, optional):
        The AWS region where the target group is located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_elbv2.target_group_exists name=mytg region=us-east-1
        salt myminion boto3_elbv2.target_group_exists arn=arn:aws:elasticloadbalancing:us-west-2:644138682826:targetgroup/learn1give1-api/414788a16b5cf163

    .. versionchanged:: 2.0.0
        Added ``arn`` parameter; ``name`` is now optional so either identifier may be used.
    """
    tg = describe_target_group(
        name=name, arn=arn, region=region, key=key, keyid=keyid, profile=profile
    )
    if tg is None:
        log.warning("The target group does not exist in region %s", region)
        return False
    return True


def describe_target_health(
    name=None, arn=None, targets=None, region=None, key=None, keyid=None, profile=None
):
    """
    Get the current health check status for targets in a target group.
    Exactly one of ``name`` or ``arn`` must be provided.

    name (str, optional):
        The name of the target group.

    arn (str, optional):
        The ARN of the target group.

    targets (list, optional):
        A list of target instance IDs to check the health status for.

    region (str, optional):
        The AWS region where the target group is located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_elbv2.describe_target_health name=mytg targets='["i-isdf23ifjf"]'
        salt myminion boto3_elbv2.describe_target_health arn=arn:aws:elasticloadbalancing:... targets='["i-isdf23ifjf"]'

    .. versionchanged:: 2.0.0
        Added ``arn`` parameter; ``name`` is now optional. First positional argument was ``name``; it is now keyword-only.
    """
    if not boto3mod.exactly_one([name, arn]):
        log.error("describe_target_health requires exactly one of 'name' or 'arn'")
        return {}

    tg = describe_target_group(
        name=name, arn=arn, region=region, key=key, keyid=keyid, profile=profile
    )
    if tg is None:
        return {}
    tg_arn = tg["TargetGroupArn"]

    conn = _get_conn("elbv2", region=region, key=key, keyid=keyid, profile=profile)

    try:
        if targets:
            targetsdict = [{"Id": target} for target in targets]
            instances = conn.describe_target_health(TargetGroupArn=tg_arn, Targets=targetsdict)
        else:
            instances = conn.describe_target_health(TargetGroupArn=tg_arn)
        return {
            instance["Target"]["Id"]: instance["TargetHealth"]["State"]
            for instance in instances["TargetHealthDescriptions"]
        }
    except ClientError as error:
        log.warning(error)
        return {}


def register_targets(targets, name=None, arn=None, region=None, key=None, keyid=None, profile=None):
    """
    Register targets to a target group of an ALB. Exactly one of ``name`` or
    ``arn`` must be provided. ``targets`` is either a single instance ID string
    or a list of instance IDs.

    targets (str or list):
        A single target instance ID or a list of target instance IDs to register.

    name (str, optional):
        The name of the target group.

    arn (str, optional):
        The ARN of the target group.

    region (str, optional):
        The AWS region where the target group is located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_elbv2.register_targets instance_id name=mytg
        salt myminion boto3_elbv2.register_targets "[instance_id,instance_id]" arn=arn:aws:elasticloadbalancing:...

    .. versionchanged:: 2.0.0
        Added ``arn`` parameter. ``name`` (formerly the first positional argument) and ``arn``
        are now keyword-only; ``targets`` moves to the first positional argument.
    """
    if not boto3mod.exactly_one([name, arn]):
        log.error("register_targets requires exactly one of 'name' or 'arn'")
        return False

    tg = describe_target_group(
        name=name, arn=arn, region=region, key=key, keyid=keyid, profile=profile
    )
    if tg is None:
        return False
    tg_arn = tg["TargetGroupArn"]

    if isinstance(targets, str):
        targetsdict = [{"Id": targets}]
    else:
        targetsdict = [{"Id": t} for t in targets]

    conn = _get_conn("elbv2", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.register_targets(TargetGroupArn=tg_arn, Targets=targetsdict)
        return True
    except ClientError as error:
        log.warning(error)
        return False


def deregister_targets(
    targets, name=None, arn=None, region=None, key=None, keyid=None, profile=None
):
    """
    Deregister targets from a target group of an ALB. Exactly one of ``name``
    or ``arn`` must be provided. ``targets`` is either a single instance ID
    string or a list of instance IDs.

    targets (str or list):
        A single target instance ID or a list of target instance IDs to deregister.

    name (str, optional):
        The name of the target group.

    arn (str, optional):
        The ARN of the target group.

    region (str, optional):
        The AWS region where the target group is located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The profile to use for AWS credentials.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_elbv2.deregister_targets instance_id name=mytg
        salt myminion boto3_elbv2.deregister_targets "[instance_id,instance_id]" arn=arn:aws:elasticloadbalancing:...

    .. versionchanged:: 2.0.0
        Added ``arn`` parameter. ``name`` (formerly the first positional argument) and ``arn``
        are now keyword-only; ``targets`` moves to the first positional argument.
    """
    if not boto3mod.exactly_one([name, arn]):
        log.error("deregister_targets requires exactly one of 'name' or 'arn'")
        return False

    tg = describe_target_group(
        name=name, arn=arn, region=region, key=key, keyid=keyid, profile=profile
    )
    if tg is None:
        return False
    tg_arn = tg["TargetGroupArn"]

    if isinstance(targets, str):
        targetsdict = [{"Id": targets}]
    else:
        targetsdict = [{"Id": t} for t in targets]

    conn = _get_conn("elbv2", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.deregister_targets(TargetGroupArn=tg_arn, Targets=targetsdict)
        return True
    except ClientError as error:
        log.warning(error)
        return False
