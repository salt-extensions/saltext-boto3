"""
Manage AWS Elastic Load Balancing v2 (ALB/NLB) using boto3.
===========================================================

    Renamed from ``boto_elbv2`` to ``boto3_elbv2`` and updated to call the
    refactored ``boto3_elbv2`` execution module.

Add and remove targets from an ALB target group.

:depends:
  - boto3 >= 1.28.0
  - botocore >= 1.31.0

This module uses ``boto3``, which can be installed via package, or pip.

Create and destroy Elastic Load Balancers (ELB). Be aware that this interacts with Amazon's
services, and so may incur charges.

This module accepts explicit ELB credentials but can also utilize
IAM roles assigned to the instance through Instance Profiles. Dynamic
credentials are then automatically obtained from AWS API and no further
configuration is necessary. More Information available at:

.. code-block:: text

    http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html

If IAM roles are not used you need to specify them either in the minion's config file
or as a profile. For example, to specify them in the minion's config file:

.. code-block:: yaml

    elb.keyid: GKTADJGHEIQSXMKKRBJ08H
    elb.key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs

It's also possible to specify key, keyid and region via a profile:

.. code-block:: yaml

    myprofile:
        keyid: GKTADJGHEIQSXMKKRBJ08H
        key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs
        region: us-east-1

.. Example:

.. code-block:: yaml

    my-target-group:
      boto3_elbv2.target_group_present:
        - protocol: HTTP
        - port: 80
        - vpc_id: vpc-deadbeef
        - profile: myprofile

.. versionadded:: 1.0.0
"""

import logging

log = logging.getLogger(__name__)


def __virtual__():
    """
    Only load if the boto3_elbv2 execution module is available.
    """
    if "boto3_elbv2.target_group_exists" in __salt__:
        return "boto3_elbv2"
    return (
        False,
        "The boto3_elbv2 state module could not be loaded: boto3_elbv2 execution module is unavailable.",
    )


def target_group_present(
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
    **kwargs,
):
    """
    Ensure a target group exists. Creates it if absent; no-ops if already present.

    name (str):
        The name of the target group.

    protocol (str):
        The protocol to use for routing traffic to the targets (e.g. ``HTTP``, ``HTTPS``).

    port (int):
        The port on which the targets receive traffic.

    vpc_id (str):
        The identifier of the VPC.

    region (str, optional):
        The AWS region where the target group will be created.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The AWS profile to use.

    health_check_protocol (str, optional):
        Protocol the load balancer uses for health checks. Default: ``HTTP``.

    health_check_port (str, optional):
        Port used for health checks. Default: ``traffic-port``.

    health_check_path (str, optional):
        Destination path for health checks. Default: ``/``.

    health_check_interval_seconds (int, optional):
        Seconds between health checks. Default: ``30``.

    health_check_timeout_seconds (int, optional):
        Seconds before a health check times out. Default: ``5``.

    healthy_threshold_count (int, optional):
        Consecutive successes before marking healthy. Default: ``5``.

    unhealthy_threshold_count (int, optional):
        Consecutive failures before marking unhealthy. Default: ``2``.

    Example:

    .. code-block:: yaml

        my-target-group:
          boto3_elbv2.target_group_present:
            - protocol: HTTP
            - port: 80
            - vpc_id: vpc-deadbeef
            - profile: myprofile

    .. versionchanged:: 2.0.0
        Renamed from ``create_target_group``. Replace any existing
        ``boto3_elbv2.create_target_group`` state references with
        ``boto3_elbv2.target_group_present``; the parameters are unchanged.
    """
    ret = {"name": name, "result": True, "comment": "", "changes": {}}

    if __salt__["boto3_elbv2.target_group_exists"](
        name=name, region=region, key=key, keyid=keyid, profile=profile
    ):
        ret["comment"] = f"Target group {name} already present."
        return ret

    if __opts__["test"]:
        ret["result"] = None
        ret["comment"] = f"Target group {name} would be created."
        return ret

    created = __salt__["boto3_elbv2.create_target_group"](
        name,
        protocol,
        port,
        vpc_id,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
        health_check_protocol=health_check_protocol,
        health_check_port=health_check_port,
        health_check_path=health_check_path,
        health_check_interval_seconds=health_check_interval_seconds,
        health_check_timeout_seconds=health_check_timeout_seconds,
        healthy_threshold_count=healthy_threshold_count,
        unhealthy_threshold_count=unhealthy_threshold_count,
        **kwargs,
    )

    if created:
        ret["changes"]["new"] = name
        ret["comment"] = f"Target group {name} created."
    else:
        ret["result"] = False
        ret["comment"] = f"Failed to create target group {name}."
    return ret


def target_group_absent(name, arn=None, region=None, key=None, keyid=None, profile=None):
    """
    Ensure a target group is absent. Deletes it if present; no-ops if already gone.

    name (str):
        The name of the target group. Used as the Salt state ID.

    arn (str, optional):
        The ARN of the target group. When provided, ``name`` is used only as
        the state ID and ``arn`` is used to identify the target group.

    region (str, optional):
        The AWS region where the target group is located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The AWS profile to use.

    Example:

    .. code-block:: yaml

        my-target-group:
          boto3_elbv2.target_group_absent:
            - profile: myprofile

        arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/my-tg/abc123:
          boto3_elbv2.target_group_absent:
            - arn: arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/my-tg/abc123
            - profile: myprofile

    .. versionchanged:: 2.0.0
        Renamed from ``delete_target_group``. Replace any existing
        ``boto3_elbv2.delete_target_group`` state references with
        ``boto3_elbv2.target_group_absent``; the parameters are unchanged.
        Added ``arn`` parameter so the target group can be identified by ARN.
    """
    ret = {"name": name, "result": True, "comment": "", "changes": {}}

    tg_name = None if arn else name

    if not __salt__["boto3_elbv2.target_group_exists"](
        name=tg_name, arn=arn, region=region, key=key, keyid=keyid, profile=profile
    ):
        ret["comment"] = f"Target group {arn or name} already absent."
        return ret

    if __opts__["test"]:
        ret["result"] = None
        ret["comment"] = f"Target group {arn or name} would be deleted."
        return ret

    deleted = __salt__["boto3_elbv2.delete_target_group"](
        name=tg_name, arn=arn, region=region, key=key, keyid=keyid, profile=profile
    )

    if deleted:
        ret["changes"]["old"] = arn or name
        ret["comment"] = f"Target group {arn or name} deleted."
    else:
        ret["result"] = False
        ret["comment"] = f"Failed to delete target group {arn or name}."
    return ret


def targets_registered(
    name, targets, arn=None, region=None, key=None, keyid=None, profile=None, **_kwargs
):
    """
    Ensure the given targets are registered in a target group. Already-registered
    targets are left untouched; only missing ones are added.

    name (str):
        The name of the target group. Used as the Salt state ID.

    targets (list or str):
        One or more target instance IDs to register.

    arn (str, optional):
        The ARN of the target group. When provided, ``name`` is used only as
        the state ID and ``arn`` is used to identify the target group.

    region (str, optional):
        The AWS region where the target group is located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The AWS profile to use.

    Example:

    .. code-block:: yaml

        my-target-group:
          boto3_elbv2.targets_registered:
            - targets:
              - i-1234567890abcdef0
              - i-0987654321fedcba0
            - profile: myprofile

        register-by-arn:
          boto3_elbv2.targets_registered:
            - arn: arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/my-tg/abc123
            - targets:
              - i-1234567890abcdef0
            - profile: myprofile

    .. versionchanged:: 2.0.0
        Previously issued one ``register_targets`` API call per target; now issues
        a single bulk call for all missing targets.
        Added ``arn`` parameter so the target group can be identified by ARN.
    """
    ret = {"name": name, "result": True, "comment": "", "changes": {}}

    tg_name = None if arn else name

    if not __salt__["boto3_elbv2.target_group_exists"](
        name=tg_name, arn=arn, region=region, key=key, keyid=keyid, profile=profile
    ):
        ret["result"] = False
        ret["comment"] = f"Target group {arn or name} not found."
        return ret

    if isinstance(targets, str):
        targets = [targets]

    health = __salt__["boto3_elbv2.describe_target_health"](
        name=tg_name, arn=arn, region=region, key=key, keyid=keyid, profile=profile
    )

    if health is None:
        ret["result"] = False
        ret["comment"] = f"Failed to retrieve target health for {arn or name}."
        return ret

    to_register = [t for t in targets if t not in health or health.get(t) == "draining"]

    if not to_register:
        ret["comment"] = f"All targets already registered in {arn or name}."
        return ret

    if __opts__["test"]:
        ret["result"] = None
        ret["comment"] = f"{len(to_register)} target(s) would be registered in {arn or name}."
        ret["changes"]["old"] = health
        ret["changes"]["new"] = {**health, **{t: "initial" for t in to_register}}
        return ret

    registered = __salt__["boto3_elbv2.register_targets"](
        to_register, name=tg_name, arn=arn, region=region, key=key, keyid=keyid, profile=profile
    )

    if registered:
        new_health = __salt__["boto3_elbv2.describe_target_health"](
            name=tg_name, arn=arn, region=region, key=key, keyid=keyid, profile=profile
        )
        ret["changes"]["old"] = health
        ret["changes"]["new"] = new_health
        ret["comment"] = f"Registered {len(to_register)} target(s) in {arn or name}."
    else:
        ret["result"] = False
        ret["comment"] = f"Failed to register targets in {arn or name}."
    return ret


def targets_deregistered(
    name, targets, arn=None, region=None, key=None, keyid=None, profile=None, **_kwargs
):
    """
    Ensure the given targets are deregistered from a target group. Already-absent or
    draining targets are left untouched; only active ones are removed.

    name (str):
        The name of the target group. Used as the Salt state ID.

    targets (list or str):
        One or more target instance IDs to deregister.

    arn (str, optional):
        The ARN of the target group. When provided, ``name`` is used only as
        the state ID and ``arn`` is used to identify the target group.

    region (str, optional):
        The AWS region where the target group is located.

    key (str, optional):
        The AWS secret access key.

    keyid (str, optional):
        The AWS access key ID.

    profile (str, optional):
        The AWS profile to use.

    Example:

    .. code-block:: yaml

        my-target-group:
          boto3_elbv2.targets_deregistered:
            - targets:
              - i-1234567890abcdef0
            - profile: myprofile

        deregister-by-arn:
          boto3_elbv2.targets_deregistered:
            - arn: arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/my-tg/abc123
            - targets:
              - i-1234567890abcdef0
            - profile: myprofile

    .. versionchanged:: 2.0.0
        Previously issued one ``deregister_targets`` API call per target; now issues
        a single bulk call for all targets to remove.
        Added ``arn`` parameter so the target group can be identified by ARN.
    """
    ret = {"name": name, "result": True, "comment": "", "changes": {}}

    tg_name = None if arn else name

    if not __salt__["boto3_elbv2.target_group_exists"](
        name=tg_name, arn=arn, region=region, key=key, keyid=keyid, profile=profile
    ):
        ret["result"] = False
        ret["comment"] = f"Target group {arn or name} not found."
        return ret

    if isinstance(targets, str):
        targets = [targets]

    health = __salt__["boto3_elbv2.describe_target_health"](
        name=tg_name, arn=arn, region=region, key=key, keyid=keyid, profile=profile
    )

    if health is None:
        ret["result"] = False
        ret["comment"] = f"Failed to retrieve target health for {arn or name}."
        return ret

    to_deregister = [t for t in targets if t in health and health.get(t) != "draining"]

    if not to_deregister:
        ret["comment"] = f"All targets already deregistered from {arn or name}."
        return ret

    if __opts__["test"]:
        ret["result"] = None
        ret["comment"] = f"{len(to_deregister)} target(s) would be deregistered from {arn or name}."
        ret["changes"]["old"] = health
        ret["changes"]["new"] = {**health, **{t: "draining" for t in to_deregister}}
        return ret

    deregistered = __salt__["boto3_elbv2.deregister_targets"](
        to_deregister, name=tg_name, arn=arn, region=region, key=key, keyid=keyid, profile=profile
    )

    if deregistered:
        new_health = __salt__["boto3_elbv2.describe_target_health"](
            name=tg_name, arn=arn, region=region, key=key, keyid=keyid, profile=profile
        )
        ret["changes"]["old"] = health
        ret["changes"]["new"] = new_health
        ret["comment"] = f"Deregistered {len(to_deregister)} target(s) from {arn or name}."
    else:
        ret["result"] = False
        ret["comment"] = f"Failed to deregister targets from {arn or name}."
    return ret
