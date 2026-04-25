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

This module uses boto3, which can be installed via package, or pip.

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

It's also possible to specify key, keyid and region via a profile, either
as a passed in dict, or as a string to pull from pillars or minion config:

.. code-block:: yaml

    myprofile:
        keyid: GKTADJGHEIQSXMKKRBJ08H
        key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs
        region: us-east-1

.. code-block:: yaml

    create-target:
      boto3_elb2.create_targets_group:
        - name: myALB
        - protocol: https
        - port: 443
        - vpc_id: myVPC
        - profile: myprofile

.. versionadded:: 1.0.0
"""

import copy
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
    **kwargs,
):
    """

    Create target group if not present.

    name
        (string) - The name of the target group.
    protocol
        (string) - The protocol to use for routing traffic to the targets
    port
        (int) - The port on which the targets receive traffic. This port is used unless
        you specify a port override when registering the traffic.
    vpc_id
        (string) - The identifier of the virtual private cloud (VPC).
    health_check_protocol
        (string) - The protocol the load balancer uses when performing health check on
        targets. The default is the HTTP protocol.
    health_check_port
        (string) - The port the load balancer uses when performing health checks on
        targets. The default is 'traffic-port', which indicates the port on which each
        target receives traffic from the load balancer.
    health_check_path
        (string) - The ping path that is the destination on the targets for health
        checks. The default is /.
    health_check_interval_seconds
        (integer) - The approximate amount of time, in seconds, between health checks
        of an individual target. The default is 30 seconds.
    health_check_timeout_seconds
        (integer) - The amount of time, in seconds, during which no response from a
        target means a failed health check. The default is 5 seconds.
    healthy_threshold_count
        (integer) - The number of consecutive health checks successes required before
        considering an unhealthy target healthy. The default is 5.
    unhealthy_threshold_count
        (integer) - The number of consecutive health check failures required before
        considering a target unhealthy. The default is 2.

    returns
        (bool) - True on success, False on failure.

    CLI Example:

    .. code-block:: yaml

        create-target:
          boto3_elb2.create_targets_group:
            - name: myALB
            - protocol: https
            - port: 443
            - vpc_id: myVPC
    """
    ret = {"name": name, "result": None, "comment": "", "changes": {}}

    if __salt__["boto3_elbv2.target_group_exists"](name, region, key, keyid, profile):
        ret["result"] = True
        ret["comment"] = f"Target Group {name} already exists"
        return ret

    if __opts__["test"]:
        ret["comment"] = f"Target Group {name} will be created"
        return ret

    state = __salt__["boto3_elbv2.create_target_group"](
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

    if state:
        ret["changes"]["target_group"] = name
        ret["result"] = True
        ret["comment"] = f"Target Group {name} created"
    else:
        ret["result"] = False
        ret["comment"] = f"Target Group {name} creation failed"
    return ret


def delete_target_group(name, region=None, key=None, keyid=None, profile=None):
    """
    Delete target group.

    name
        (string) - The Amazon Resource Name (ARN) of the resource.

    returns
        (bool) - True on success, False on failure.

    CLI Example:

    .. code-block:: bash

        check-target:
          boto3_elb2.delete_targets_group:
            - name: myALB
            - protocol: https
            - port: 443
            - vpc_id: myVPC

    Example:

    .. code-block:: yaml

        ensure-delete-target-group:
          boto3_elbv2.delete_target_group:
            - name: example

    """
    ret = {"name": name, "result": None, "comment": "", "changes": {}}

    if not __salt__["boto3_elbv2.target_group_exists"](name, region, key, keyid, profile):
        ret["result"] = True
        ret["comment"] = f"Target Group {name} does not exists"
        return ret

    if __opts__["test"]:
        ret["comment"] = f"Target Group {name} will be deleted"
        return ret

    state = __salt__["boto3_elbv2.delete_target_group"](
        name, region=region, key=key, keyid=keyid, profile=profile
    )

    if state:
        ret["result"] = True
        ret["changes"]["target_group"] = name
        ret["comment"] = f"Target Group {name} deleted"
    else:
        ret["result"] = False
        ret["comment"] = f"Target Group {name} deletion failed"
    return ret


def targets_registered(name, targets, region=None, key=None, keyid=None, profile=None, **_kwargs):
    """

    Add targets to an Application Load Balancer target group. This state will not remove targets.

    name
        The ARN of the Application Load Balancer Target Group to add targets to.

    targets
        A list of target IDs or a string of a single target that this target group should
        distribute traffic to.

    .. code-block:: yaml

        add-targets:
          boto3_elb.targets_registered:
            - name: arn:myloadbalancer
            - targets:
              - instance-id1
              - instance-id2
    """
    ret = {"name": name, "result": None, "comment": "", "changes": {}}

    if __salt__["boto3_elbv2.target_group_exists"](name, region, key, keyid, profile):
        health = __salt__["boto3_elbv2.describe_target_health"](
            name, region=region, key=key, keyid=keyid, profile=profile
        )
        failure = False
        changes = False
        newhealth_mock = copy.copy(health)

        if isinstance(targets, str):
            targets = [targets]

        for target in targets:
            if target in health and health.get(target) != "draining":
                ret["comment"] = (
                    ret["comment"]
                    + f"Target/s {target} already registered and is {health[target]}.\n"
                )
                ret["result"] = True
            else:
                if __opts__["test"]:
                    changes = True
                    newhealth_mock.update({target: "initial"})
                else:
                    state = __salt__["boto3_elbv2.register_targets"](
                        name,
                        targets,
                        region=region,
                        key=key,
                        keyid=keyid,
                        profile=profile,
                    )
                    if state:
                        changes = True
                        ret["result"] = True
                    else:
                        ret["comment"] = f"Target Group {name} failed to add targets"
                        failure = True
        if failure:
            ret["result"] = False
        if changes:
            ret["changes"]["old"] = health
            if __opts__["test"]:
                ret["comment"] = f"Target Group {name} would be changed"
                ret["result"] = None
                ret["changes"]["new"] = newhealth_mock
            else:
                ret["comment"] = f"Target Group {name} has been changed"
                newhealth = __salt__["boto3_elbv2.describe_target_health"](
                    name, region=region, key=key, keyid=keyid, profile=profile
                )
                ret["changes"]["new"] = newhealth
        return ret
    else:
        ret["comment"] = f"Could not find target group {name}"
    return ret


def targets_deregistered(name, targets, region=None, key=None, keyid=None, profile=None, **_kwargs):
    """
    Remove targets to an Application Load Balancer target group.

    name
        The ARN of the Application Load Balancer Target Group to remove targets from.

    targets
        A list of target IDs or a string of a single target registered to the target group to be removed


    .. code-block:: yaml

        remove-targets:
          boto3_elb.targets_deregistered:
            - name: arn:myloadbalancer
            - targets:
              - instance-id1
              - instance-id2
    """
    ret = {"name": name, "result": None, "comment": "", "changes": {}}
    if __salt__["boto3_elbv2.target_group_exists"](name, region, key, keyid, profile):
        health = __salt__["boto3_elbv2.describe_target_health"](
            name, region=region, key=key, keyid=keyid, profile=profile
        )
        failure = False
        changes = False
        newhealth_mock = copy.copy(health)
        if isinstance(targets, str):
            targets = [targets]
        for target in targets:
            if target not in health or health.get(target) == "draining":
                ret["comment"] = ret["comment"] + f"Target/s {target} already deregistered\n"
                ret["result"] = True
            else:
                if __opts__["test"]:
                    changes = True
                    newhealth_mock.update({target: "draining"})
                else:
                    state = __salt__["boto3_elbv2.deregister_targets"](
                        name,
                        targets,
                        region=region,
                        key=key,
                        keyid=keyid,
                        profile=profile,
                    )
                    if state:
                        changes = True
                        ret["result"] = True
                    else:
                        ret["comment"] = f"Target Group {name} failed to remove targets"
                        failure = True
        if failure:
            ret["result"] = False
        if changes:
            ret["changes"]["old"] = health
            if __opts__["test"]:
                ret["comment"] = f"Target Group {name} would be changed"
                ret["result"] = None
                ret["changes"]["new"] = newhealth_mock
            else:
                ret["comment"] = f"Target Group {name} has been changed"
                newhealth = __salt__["boto3_elbv2.describe_target_health"](
                    name, region=region, key=key, keyid=keyid, profile=profile
                )
                ret["changes"]["new"] = newhealth
        return ret
    else:
        ret["comment"] = f"Could not find target group {name}"
    return ret
