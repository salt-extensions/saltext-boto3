"""
Connection module for Amazon Autoscale Groups using boto3.
==========================================================

    Renamed from ``boto_asg`` to ``boto3_asg`` and rewritten to use the
    boto3 ``autoscaling`` (and ``ec2``) client APIs directly via
    :py:mod:`saltext.boto3.utils.boto3mod`.  The legacy boto2 code path
    (object-style access, retry loops) has been removed.

:depends:
  - boto3 >= 1.28.0
  - botocore >= 1.31.0

:configuration: This module accepts explicit autoscale credentials but can
    also utilize IAM roles assigned to the instance through Instance Profiles.
    Dynamic credentials are then automatically obtained from AWS API and no
    further configuration is necessary. More Information available at:

    .. code-block:: text

        http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html

    If IAM roles are not used you need to specify them either in the minion's
    config file or as a profile. For example, to specify them in the minion's
    config file:

.. code-block:: yaml

    asg.keyid: GKTADJGHEIQSXMKKRBJ08H
    asg.key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs

A region may also be specified in the configuration:

.. code-block:: yaml

    asg.region: us-east-1

It's also possible to specify key, keyid and region via a profile, either
as a passed in dict, or as a string to pull from pillars or minion config:

.. code-block:: yaml

    myprofile:
        keyid: GKTADJGHEIQSXMKKRBJ08H
        key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs
        region: us-east-1

.. versionadded:: 1.0.0
"""

import datetime
import email.mime.multipart
import email.mime.text
import logging

import salt.utils.json
import salt.utils.yaml
from salt.utils import odict

from saltext.boto3.utils import boto3mod

try:
    from botocore.exceptions import ClientError

    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

log = logging.getLogger(__name__)

DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

__virtualname__ = "boto3_asg"


def __virtual__():
    """
    Only load if boto3 is available. Minimum version is enforced via the
    project's ``pyproject.toml`` dependency declaration.
    """
    if HAS_BOTO3:
        return __virtualname__
    return (False, "The boto3_asg module could not be loaded: boto3 is not available.")


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


def _ec2_conn(region=None, key=None, keyid=None, profile=None):
    return _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)


def _paginate(function, result_key, **kwargs):
    """
    Yield items from a boto3 autoscaling paginated response. ``function`` is
    the bound client method and ``result_key`` is the response key that holds
    the list of items (e.g. ``AutoScalingGroups``, ``LaunchConfigurations``,
    ``Tags``).
    """
    next_token = ""
    while next_token is not None:
        call_kwargs = dict(kwargs)
        if next_token:
            call_kwargs["NextToken"] = next_token
        resp = function(**call_kwargs)
        yield from resp.get(result_key, [])
        next_token = resp.get("NextToken")
        if not next_token:
            return


def _maybe_json(value):
    if isinstance(value, str):
        return salt.utils.json.loads(value)
    return value


def exists(name, region=None, key=None, keyid=None, profile=None):
    """
    Check to see if an autoscale group exists.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_asg.exists myasg region=us-east-1
    """
    conn = _get_conn("autoscaling", region=region, key=key, keyid=keyid, profile=profile)
    try:
        resp = conn.describe_auto_scaling_groups(AutoScalingGroupNames=[name])
    except ClientError as e:
        log.error(e)
        return False
    return bool(resp.get("AutoScalingGroups"))


def _asg_to_config(asg, policies, actions):
    """
    Translate a boto3 ``AutoScalingGroup`` dict and its policies/actions into
    the legacy snake_case configuration dict consumed by the state module.
    """
    ret = odict.OrderedDict()
    ret["name"] = asg.get("AutoScalingGroupName")
    ret["availability_zones"] = asg.get("AvailabilityZones", [])
    ret["default_cooldown"] = asg.get("DefaultCooldown")
    ret["desired_capacity"] = asg.get("DesiredCapacity")
    ret["health_check_period"] = asg.get("HealthCheckGracePeriod")
    ret["health_check_type"] = asg.get("HealthCheckType")
    ret["launch_config_name"] = asg.get("LaunchConfigurationName")
    ret["load_balancers"] = asg.get("LoadBalancerNames", [])
    ret["max_size"] = asg.get("MaxSize")
    ret["min_size"] = asg.get("MinSize")
    ret["placement_group"] = asg.get("PlacementGroup")
    vpc_zone = asg.get("VPCZoneIdentifier")
    # Boto2 always returned a comma-separated list; preserve that shape.
    ret["vpc_zone_identifier"] = vpc_zone.split(",") if vpc_zone else []
    ret["tags"] = [
        odict.OrderedDict(
            [
                ("key", t.get("Key")),
                ("value", t.get("Value")),
                ("propagate_at_launch", t.get("PropagateAtLaunch", False)),
            ]
        )
        for t in asg.get("Tags", [])
    ]
    ret["termination_policies"] = asg.get("TerminationPolicies", [])
    ret["suspended_processes"] = sorted(p["ProcessName"] for p in asg.get("SuspendedProcesses", []))

    ret["scaling_policies"] = [
        {
            "name": p.get("PolicyName"),
            "adjustment_type": p.get("AdjustmentType"),
            "scaling_adjustment": p.get("ScalingAdjustment"),
            "min_adjustment_step": p.get("MinAdjustmentStep"),
            "cooldown": p.get("Cooldown"),
        }
        for p in policies
    ]

    ret["scheduled_actions"] = {}
    for action in actions:
        end_time = action.get("EndTime")
        if end_time is not None and hasattr(end_time, "isoformat"):
            end_time = end_time.isoformat()
        start_time = action.get("StartTime")
        if start_time is not None and hasattr(start_time, "isoformat"):
            start_time = start_time.isoformat()
        ret["scheduled_actions"][action["ScheduledActionName"]] = {
            "min_size": action.get("MinSize"),
            "max_size": action.get("MaxSize"),
            "desired_capacity": (
                int(action["DesiredCapacity"])
                if action.get("DesiredCapacity") is not None
                else None
            ),
            "start_time": start_time,
            "end_time": end_time,
            "recurrence": action.get("Recurrence"),
        }
    return ret


def get_config(name, region=None, key=None, keyid=None, profile=None):
    """
    Get the configuration for an autoscale group.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_asg.get_config myasg region=us-east-1
    """
    conn = _get_conn("autoscaling", region=region, key=key, keyid=keyid, profile=profile)
    try:
        resp = conn.describe_auto_scaling_groups(AutoScalingGroupNames=[name])
    except ClientError as e:
        log.error(e)
        return {}
    asgs = resp.get("AutoScalingGroups") or []
    if not asgs:
        return {}
    asg = asgs[0]
    try:
        policies = list(
            _paginate(
                conn.describe_policies,
                "ScalingPolicies",
                AutoScalingGroupName=name,
            )
        )
        actions = list(
            _paginate(
                conn.describe_scheduled_actions,
                "ScheduledUpdateGroupActions",
                AutoScalingGroupName=name,
            )
        )
    except ClientError as e:
        log.error(e)
        return {}
    return _asg_to_config(asg, policies, actions)


def _tags_kwarg(tags, asg_name):
    """
    Build a boto3 Tags list for create/update from the state-style tag dicts.
    """
    out = []
    for tag in tags or []:
        if "key" not in tag:
            log.error("Tag missing key.")
            return None
        if "value" not in tag:
            log.error("Tag missing value.")
            return None
        out.append(
            {
                "ResourceId": asg_name,
                "ResourceType": "auto-scaling-group",
                "Key": tag["key"],
                "Value": tag["value"],
                "PropagateAtLaunch": tag.get("propagate_at_launch", False),
            }
        )
    return out


def _create_kwargs(
    name,
    launch_config_name,
    availability_zones,
    min_size,
    max_size,
    desired_capacity,
    load_balancers,
    default_cooldown,
    health_check_type,
    health_check_period,
    placement_group,
    vpc_zone_identifier,
    tags,
    termination_policies,
):
    """
    Map the legacy create/update kwargs to boto3 CreateAutoScalingGroup
    kwargs, omitting values that are ``None``.
    """
    mapping = [
        ("AutoScalingGroupName", name),
        ("LaunchConfigurationName", launch_config_name),
        ("MinSize", min_size),
        ("MaxSize", max_size),
        ("DesiredCapacity", desired_capacity),
        ("DefaultCooldown", default_cooldown),
        ("HealthCheckType", health_check_type),
        ("HealthCheckGracePeriod", health_check_period),
        ("PlacementGroup", placement_group),
    ]
    kwargs = {k: v for k, v in mapping if v is not None}
    if availability_zones:
        kwargs["AvailabilityZones"] = availability_zones
    if load_balancers:
        kwargs["LoadBalancerNames"] = load_balancers
    if vpc_zone_identifier:
        if isinstance(vpc_zone_identifier, list):
            kwargs["VPCZoneIdentifier"] = ",".join(vpc_zone_identifier)
        else:
            kwargs["VPCZoneIdentifier"] = vpc_zone_identifier
    if termination_policies:
        kwargs["TerminationPolicies"] = termination_policies
    if tags:
        tag_list = _tags_kwarg(tags, name)
        if tag_list is None:
            return None
        kwargs["Tags"] = tag_list
    return kwargs


def create(
    name,
    launch_config_name,
    availability_zones,
    min_size,
    max_size,
    desired_capacity=None,
    load_balancers=None,
    default_cooldown=None,
    health_check_type=None,
    health_check_period=None,
    placement_group=None,
    vpc_zone_identifier=None,
    tags=None,
    termination_policies=None,
    suspended_processes=None,
    scaling_policies=None,
    scheduled_actions=None,
    region=None,
    notification_arn=None,
    notification_types=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Create an autoscale group.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_asg.create myasg mylc '["us-east-1a", "us-east-1e"]' 1 10
    """
    availability_zones = _maybe_json(availability_zones)
    load_balancers = _maybe_json(load_balancers)
    vpc_zone_identifier = _maybe_json(vpc_zone_identifier)
    tags = _maybe_json(tags)
    termination_policies = _maybe_json(termination_policies)
    suspended_processes = _maybe_json(suspended_processes)
    scheduled_actions = _maybe_json(scheduled_actions)

    kwargs = _create_kwargs(
        name,
        launch_config_name,
        availability_zones,
        min_size,
        max_size,
        desired_capacity,
        load_balancers,
        default_cooldown,
        health_check_type,
        health_check_period,
        placement_group,
        vpc_zone_identifier,
        tags,
        termination_policies,
    )
    if kwargs is None:
        return False

    conn = _get_conn("autoscaling", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.create_auto_scaling_group(**kwargs)
        _create_scaling_policies(conn, name, scaling_policies)
        _create_scheduled_actions(conn, name, scheduled_actions)
        if suspended_processes:
            conn.suspend_processes(
                AutoScalingGroupName=name, ScalingProcesses=list(suspended_processes)
            )
        if notification_arn and notification_types:
            conn.put_notification_configuration(
                AutoScalingGroupName=name,
                TopicARN=notification_arn,
                NotificationTypes=list(notification_types),
            )
        log.info("Created ASG %s", name)
        return True
    except ClientError as e:
        log.error("Failed to create ASG %s: %s", name, e)
        return False


def update(
    name,
    launch_config_name,
    availability_zones,
    min_size,
    max_size,
    desired_capacity=None,
    load_balancers=None,
    default_cooldown=None,
    health_check_type=None,
    health_check_period=None,
    placement_group=None,
    vpc_zone_identifier=None,
    tags=None,
    termination_policies=None,
    suspended_processes=None,
    scaling_policies=None,
    scheduled_actions=None,
    notification_arn=None,
    notification_types=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Update an autoscale group.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_asg.update myasg mylc '["us-east-1a", "us-east-1e"]' 1 10
    """
    availability_zones = _maybe_json(availability_zones)
    load_balancers = _maybe_json(load_balancers)
    vpc_zone_identifier = _maybe_json(vpc_zone_identifier)
    tags = _maybe_json(tags)
    termination_policies = _maybe_json(termination_policies)
    suspended_processes = _maybe_json(suspended_processes)
    scheduled_actions = _maybe_json(scheduled_actions)

    conn = _get_conn("autoscaling", region=region, key=key, keyid=keyid, profile=profile)
    if not conn:
        return False, "failed to connect to AWS"

    # Build the tag add/delete deltas against the current tag set.
    try:
        current_tags_resp = conn.describe_tags(
            Filters=[{"Name": "auto-scaling-group", "Values": [name]}]
        )
    except ClientError as e:
        return False, str(e)
    current_tags = [
        {
            "key": t["Key"],
            "value": t["Value"],
            "resource_id": t["ResourceId"],
            "propagate_at_launch": t.get("PropagateAtLaunch", False),
        }
        for t in current_tags_resp.get("Tags", [])
    ]
    add_tags = []
    desired_tags = []
    if tags:
        tags = boto3mod.ordered(tags)
        for tag in tags:
            if "key" not in tag:
                log.error("Tag missing key.")
                return False, f"Tag {tag} missing key"
            if "value" not in tag:
                log.error("Tag missing value.")
                return False, f"Tag {tag} missing value"
            _tag = {
                "key": tag["key"],
                "value": tag["value"],
                "resource_id": name,
                "propagate_at_launch": tag.get("propagate_at_launch", False),
            }
            if _tag not in current_tags:
                add_tags.append(_tag)
            desired_tags.append(_tag)
    delete_tags = [t for t in current_tags if t not in desired_tags]

    try:
        update_kwargs = _update_kwargs(
            name,
            launch_config_name,
            availability_zones,
            min_size,
            max_size,
            desired_capacity,
            default_cooldown,
            health_check_type,
            health_check_period,
            placement_group,
            vpc_zone_identifier,
            termination_policies,
        )
        conn.update_auto_scaling_group(**update_kwargs)

        if load_balancers is not None:
            _reconcile_load_balancers(conn, name, load_balancers)

        if notification_arn and notification_types:
            conn.put_notification_configuration(
                AutoScalingGroupName=name,
                TopicARN=notification_arn,
                NotificationTypes=list(notification_types),
            )
        if add_tags:
            log.debug("Adding/updating tags for ASG: %s", add_tags)
            conn.create_or_update_tags(Tags=_convert_tag_list(add_tags))
        if delete_tags:
            log.debug("Deleting tags from ASG: %s", delete_tags)
            conn.delete_tags(Tags=_convert_tag_list(delete_tags))
        # Resume all processes, then suspend any explicitly specified.
        conn.resume_processes(AutoScalingGroupName=name)
        if suspended_processes:
            conn.suspend_processes(
                AutoScalingGroupName=name, ScalingProcesses=list(suspended_processes)
            )
        # Scaling policies: delete-all + recreate
        for policy in _paginate(
            conn.describe_policies, "ScalingPolicies", AutoScalingGroupName=name
        ):
            conn.delete_policy(AutoScalingGroupName=name, PolicyName=policy["PolicyName"])
        _create_scaling_policies(conn, name, scaling_policies)
        # Scheduled actions: delete-all + recreate
        for action in _paginate(
            conn.describe_scheduled_actions,
            "ScheduledUpdateGroupActions",
            AutoScalingGroupName=name,
        ):
            conn.delete_scheduled_action(
                AutoScalingGroupName=name,
                ScheduledActionName=action["ScheduledActionName"],
            )
        _create_scheduled_actions(conn, name, scheduled_actions)
        return True, ""
    except ClientError as e:
        log.error("Failed to update ASG %s: %s", name, e)
        return False, str(e)


def _update_kwargs(
    name,
    launch_config_name,
    availability_zones,
    min_size,
    max_size,
    desired_capacity,
    default_cooldown,
    health_check_type,
    health_check_period,
    placement_group,
    vpc_zone_identifier,
    termination_policies,
):
    mapping = [
        ("AutoScalingGroupName", name),
        ("LaunchConfigurationName", launch_config_name),
        ("MinSize", min_size),
        ("MaxSize", max_size),
        ("DesiredCapacity", desired_capacity),
        ("DefaultCooldown", default_cooldown),
        ("HealthCheckType", health_check_type),
        ("HealthCheckGracePeriod", health_check_period),
        ("PlacementGroup", placement_group),
    ]
    kwargs = {k: v for k, v in mapping if v is not None}
    if availability_zones:
        kwargs["AvailabilityZones"] = availability_zones
    if vpc_zone_identifier:
        kwargs["VPCZoneIdentifier"] = (
            ",".join(vpc_zone_identifier)
            if isinstance(vpc_zone_identifier, list)
            else vpc_zone_identifier
        )
    if termination_policies:
        kwargs["TerminationPolicies"] = termination_policies
    return kwargs


def _convert_tag_list(tags):
    return [
        {
            "ResourceId": t["resource_id"],
            "ResourceType": "auto-scaling-group",
            "Key": t["key"],
            "Value": t["value"],
            "PropagateAtLaunch": t.get("propagate_at_launch", False),
        }
        for t in tags
    ]


def _reconcile_load_balancers(conn, name, load_balancers):
    try:
        resp = conn.describe_load_balancers(AutoScalingGroupName=name)
        current_names = {lb["LoadBalancerName"] for lb in resp.get("LoadBalancers", [])}
    except ClientError:
        current_names = set()
    desired = set(load_balancers or [])
    to_attach = list(desired - current_names)
    to_detach = list(current_names - desired)
    if to_attach:
        conn.attach_load_balancers(AutoScalingGroupName=name, LoadBalancerNames=to_attach)
    if to_detach:
        conn.detach_load_balancers(AutoScalingGroupName=name, LoadBalancerNames=to_detach)


def _create_scaling_policies(conn, as_name, scaling_policies):
    """helper function to create scaling policies"""
    if not scaling_policies:
        return
    for policy in scaling_policies:
        kwargs = {
            "AutoScalingGroupName": as_name,
            "PolicyName": policy["name"],
            "AdjustmentType": policy["adjustment_type"],
            "ScalingAdjustment": policy["scaling_adjustment"],
        }
        if policy.get("min_adjustment_step") is not None:
            kwargs["MinAdjustmentStep"] = policy["min_adjustment_step"]
        if policy.get("cooldown") is not None:
            kwargs["Cooldown"] = policy["cooldown"]
        conn.put_scaling_policy(**kwargs)


def _create_scheduled_actions(conn, as_name, scheduled_actions):
    """Helper function to create scheduled actions"""
    if not scheduled_actions:
        return
    for action_name, action in scheduled_actions.items():
        kwargs = {
            "AutoScalingGroupName": as_name,
            "ScheduledActionName": action_name,
        }
        for src, dst in (
            ("desired_capacity", "DesiredCapacity"),
            ("min_size", "MinSize"),
            ("max_size", "MaxSize"),
            ("recurrence", "Recurrence"),
        ):
            if action.get(src) is not None:
                kwargs[dst] = action[src]
        for src, dst in (("start_time", "StartTime"), ("end_time", "EndTime")):
            value = action.get(src)
            if value is None:
                continue
            if isinstance(value, str):
                value = datetime.datetime.strptime(value, DATE_FORMAT)
            kwargs[dst] = value
        conn.put_scheduled_update_group_action(**kwargs)


def delete(name, force=False, region=None, key=None, keyid=None, profile=None):
    """
    Delete an autoscale group.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_asg.delete myasg region=us-east-1
    """
    conn = _get_conn("autoscaling", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.delete_auto_scaling_group(AutoScalingGroupName=name, ForceDelete=bool(force))
        log.info("Deleted autoscale group %s.", name)
        return True
    except ClientError as e:
        log.error("Failed to delete autoscale group %s: %s", name, e)
        return False


def get_cloud_init_mime(cloud_init):
    """
    Get a mime multipart encoded string from a cloud-init dict. Currently
    supports boothooks, scripts and cloud-config.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_asg.get_cloud_init_mime <cloud init>
    """
    if isinstance(cloud_init, str):
        cloud_init = salt.utils.json.loads(cloud_init)
    _cloud_init = email.mime.multipart.MIMEMultipart()
    if "boothooks" in cloud_init:
        for _script_name, script in cloud_init["boothooks"].items():
            _script = email.mime.text.MIMEText(script, "cloud-boothook")
            _cloud_init.attach(_script)
    if "scripts" in cloud_init:
        for _script_name, script in cloud_init["scripts"].items():
            _script = email.mime.text.MIMEText(script, "x-shellscript")
            _cloud_init.attach(_script)
    if "cloud-config" in cloud_init:
        cloud_config = cloud_init["cloud-config"]
        _cloud_config = email.mime.text.MIMEText(
            salt.utils.yaml.safe_dump(cloud_config, default_flow_style=False),
            "cloud-config",
        )
        _cloud_init.attach(_cloud_config)
    return _cloud_init.as_string()


def launch_configuration_exists(name, region=None, key=None, keyid=None, profile=None):
    """
    Check for a launch configuration's existence.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_asg.launch_configuration_exists mylc
    """
    conn = _get_conn("autoscaling", region=region, key=key, keyid=keyid, profile=profile)
    try:
        resp = conn.describe_launch_configurations(LaunchConfigurationNames=[name])
    except ClientError as e:
        log.error(e)
        return False
    return bool(resp.get("LaunchConfigurations"))


def get_all_launch_configurations(region=None, key=None, keyid=None, profile=None):
    """
    Fetch and return all Launch Configurations with details.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_asg.get_all_launch_configurations
    """
    conn = _get_conn("autoscaling", region=region, key=key, keyid=keyid, profile=profile)
    try:
        return list(_paginate(conn.describe_launch_configurations, "LaunchConfigurations"))
    except ClientError as e:
        log.error(e)
        return []


def list_launch_configurations(region=None, key=None, keyid=None, profile=None):
    """
    List all Launch Configuration names.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_asg.list_launch_configurations
    """
    return [
        lc["LaunchConfigurationName"]
        for lc in get_all_launch_configurations(
            region=region, key=key, keyid=keyid, profile=profile
        )
    ]


def describe_launch_configuration(name, region=None, key=None, keyid=None, profile=None):
    """
    Dump details of a given launch configuration.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_asg.describe_launch_configuration mylc
    """
    conn = _get_conn("autoscaling", region=region, key=key, keyid=keyid, profile=profile)
    try:
        resp = conn.describe_launch_configurations(LaunchConfigurationNames=[name])
    except ClientError as e:
        log.error(e)
        return None
    lcs = resp.get("LaunchConfigurations") or []
    return lcs[0] if lcs else None


def create_launch_configuration(
    name,
    image_id,
    key_name=None,
    vpc_id=None,
    vpc_name=None,
    security_groups=None,
    user_data=None,
    instance_type="m1.small",
    kernel_id=None,
    ramdisk_id=None,
    block_device_mappings=None,
    instance_monitoring=False,
    spot_price=None,
    instance_profile_name=None,
    ebs_optimized=False,
    associate_public_ip_address=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Create a launch configuration.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_asg.create_launch_configuration mylc image_id=ami-0b9c9f62
    """
    security_groups = _maybe_json(security_groups)
    block_device_mappings = _maybe_json(block_device_mappings)

    # If a VPC is specified, determine the secgroup ids within that VPC.
    if security_groups and (vpc_id or vpc_name):
        security_groups = __salt__["boto3_secgroup.convert_to_group_ids"](
            security_groups,
            vpc_id=vpc_id,
            vpc_name=vpc_name,
            region=region,
            key=key,
            keyid=keyid,
            profile=profile,
        )

    # Translate the old {name: attrs} dict list into boto3 BlockDeviceMappings
    bdm_kwargs = []
    if block_device_mappings:
        for block_device_dict in block_device_mappings:
            for device_name, attributes in block_device_dict.items():
                entry = {"DeviceName": device_name}
                ebs = {}
                for attr, value in attributes.items():
                    if attr == "virtual_name":
                        entry["VirtualName"] = value
                    elif attr == "no_device":
                        entry["NoDevice"] = value
                    elif attr == "volume_type":
                        ebs["VolumeType"] = value
                    elif attr == "size":
                        ebs["VolumeSize"] = value
                    elif attr == "iops":
                        ebs["Iops"] = value
                    elif attr == "snapshot_id":
                        ebs["SnapshotId"] = value
                    elif attr == "delete_on_termination":
                        ebs["DeleteOnTermination"] = value
                    elif attr == "encrypted":
                        ebs["Encrypted"] = value
                if ebs:
                    entry["Ebs"] = ebs
                bdm_kwargs.append(entry)

    kwargs = {
        "LaunchConfigurationName": name,
        "ImageId": image_id,
        "InstanceType": instance_type,
        "InstanceMonitoring": {"Enabled": bool(instance_monitoring)},
        "EbsOptimized": bool(ebs_optimized),
    }
    if key_name is not None:
        kwargs["KeyName"] = key_name
    if security_groups:
        kwargs["SecurityGroups"] = list(security_groups)
    if user_data is not None:
        kwargs["UserData"] = user_data
    if kernel_id is not None:
        kwargs["KernelId"] = kernel_id
    if ramdisk_id is not None:
        kwargs["RamdiskId"] = ramdisk_id
    if spot_price is not None:
        kwargs["SpotPrice"] = str(spot_price)
    if instance_profile_name is not None:
        kwargs["IamInstanceProfile"] = instance_profile_name
    if associate_public_ip_address is not None:
        kwargs["AssociatePublicIpAddress"] = bool(associate_public_ip_address)
    if bdm_kwargs:
        kwargs["BlockDeviceMappings"] = bdm_kwargs

    conn = _get_conn("autoscaling", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.create_launch_configuration(**kwargs)
        log.info("Created LC %s", name)
        return True
    except ClientError as e:
        log.error("Failed to create LC %s: %s", name, e)
        return False


def delete_launch_configuration(name, region=None, key=None, keyid=None, profile=None):
    """
    Delete a launch configuration.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_asg.delete_launch_configuration mylc
    """
    conn = _get_conn("autoscaling", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.delete_launch_configuration(LaunchConfigurationName=name)
        log.info("Deleted LC %s", name)
        return True
    except ClientError as e:
        log.error("Failed to delete LC %s: %s", name, e)
        return False


def get_scaling_policy_arn(
    as_group, scaling_policy_name, region=None, key=None, keyid=None, profile=None
):
    """
    Return the arn for a scaling policy in a specific autoscale group or ``None``
    if not found. Mainly used as a helper method for boto_cloudwatch_alarm, for
    linking alarms to scaling policies.

    CLI Example:

    .. code-block:: bash

        salt '*' boto3_asg.get_scaling_policy_arn mygroup mypolicy
    """
    conn = _get_conn("autoscaling", region=region, key=key, keyid=keyid, profile=profile)
    try:
        for policy in _paginate(
            conn.describe_policies,
            "ScalingPolicies",
            AutoScalingGroupName=as_group,
        ):
            if policy["PolicyName"] == scaling_policy_name:
                return policy["PolicyARN"]
    except ClientError as e:
        log.error(e)
        return None
    log.error("Could not find scaling policy %s for %s", scaling_policy_name, as_group)
    return None


def get_all_groups(region=None, key=None, keyid=None, profile=None):
    """
    Return all AutoScale Groups visible in the account as a list of
    boto3 describe-response dicts.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_asg.get_all_groups region=us-east-1 --output yaml
    """
    conn = _get_conn("autoscaling", region=region, key=key, keyid=keyid, profile=profile)
    try:
        return list(_paginate(conn.describe_auto_scaling_groups, "AutoScalingGroups"))
    except ClientError as e:
        log.error(e)
        return []


def list_groups(region=None, key=None, keyid=None, profile=None):
    """
    Return all AutoScale Group names visible in the account.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_asg.list_groups region=us-east-1
    """
    return [
        a["AutoScalingGroupName"]
        for a in get_all_groups(region=region, key=key, keyid=keyid, profile=profile)
    ]


def get_instances(
    name,
    lifecycle_state="InService",
    health_status="Healthy",
    attribute="private_ip_address",
    attributes=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Return an attribute of all instances in the named autoscale group.

    CLI Example:

    .. code-block:: bash

        salt-call boto3_asg.get_instances my_autoscale_group_name
    """
    conn = _get_conn("autoscaling", region=region, key=key, keyid=keyid, profile=profile)
    try:
        resp = conn.describe_auto_scaling_groups(AutoScalingGroupNames=[name])
    except ClientError as e:
        log.error(e)
        return False
    asgs = resp.get("AutoScalingGroups") or []
    if len(asgs) != 1:
        log.debug(
            "name '%s' returns multiple ASGs: %s",
            name,
            [a["AutoScalingGroupName"] for a in asgs],
        )
        return False
    asg = asgs[0]
    wanted_ls = lifecycle_state
    wanted_hs = health_status
    instance_ids = []
    for inst in asg.get("Instances", []):
        if wanted_ls is not None and inst.get("LifecycleState") != wanted_ls:
            continue
        if wanted_hs is not None and inst.get("HealthStatus") != wanted_hs:
            continue
        instance_ids.append(inst["InstanceId"])
    if not instance_ids:
        return []
    ec2 = _ec2_conn(region=region, key=key, keyid=keyid, profile=profile)
    try:
        reservations = ec2.describe_instances(InstanceIds=instance_ids).get("Reservations", [])
    except ClientError as e:
        log.error(e)
        return False
    instances = [i for r in reservations for i in r.get("Instances", [])]
    if attributes:
        return [[_ec2_instance_attribute(inst, a) for a in attributes] for inst in instances]
    return [
        _ec2_instance_attribute(inst, attribute)
        for inst in instances
        if _ec2_instance_attribute(inst, attribute) is not None
    ]


# Mapping from the legacy boto2 attribute names to boto3 describe_instances
# response fields. Only the fields that callers actually use are mapped; any
# unknown attribute falls through to a CamelCase conversion.
_BOTO2_ATTR_MAP = {
    "id": "InstanceId",
    "instance_id": "InstanceId",
    "private_ip_address": "PrivateIpAddress",
    "public_ip_address": "PublicIpAddress",
    "private_dns_name": "PrivateDnsName",
    "public_dns_name": "PublicDnsName",
    "ip_address": "PublicIpAddress",
    "image_id": "ImageId",
    "instance_type": "InstanceType",
    "key_name": "KeyName",
    "state": "State",
    "subnet_id": "SubnetId",
    "vpc_id": "VpcId",
    "tags": "Tags",
}


def _ec2_instance_attribute(instance, attribute):
    if attribute == "tags":
        return {t["Key"]: t["Value"] for t in instance.get("Tags", [])}
    key = _BOTO2_ATTR_MAP.get(attribute)
    if key is None:
        # best-effort CamelCase of snake_case
        key = "".join(part.title() for part in attribute.split("_"))
    return instance.get(key)


def enter_standby(
    name,
    instance_ids,
    should_decrement_desired_capacity=False,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Switch desired instances to StandBy mode

    CLI Example:

    .. code-block:: bash

        salt-call boto3_asg.enter_standby my_autoscale_group_name '["i-xxxxxx"]'
    """
    conn = _get_conn("autoscaling", region=region, key=key, keyid=keyid, profile=profile)
    try:
        response = conn.enter_standby(
            InstanceIds=instance_ids,
            AutoScalingGroupName=name,
            ShouldDecrementDesiredCapacity=should_decrement_desired_capacity,
        )
    except ClientError as e:
        err = boto3mod.get_error(e)
        if e.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
            return {"exists": False}
        return {"error": err}
    return all(activity["StatusCode"] != "Failed" for activity in response["Activities"])


def exit_standby(
    name,
    instance_ids,
    should_decrement_desired_capacity=False,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Exit desired instances from StandBy mode

    CLI Example:

    .. code-block:: bash

        salt-call boto3_asg.exit_standby my_autoscale_group_name '["i-xxxxxx"]'
    """
    conn = _get_conn("autoscaling", region=region, key=key, keyid=keyid, profile=profile)
    try:
        response = conn.exit_standby(
            InstanceIds=instance_ids,
            AutoScalingGroupName=name,
            ShouldDecrementDesiredCapacity=should_decrement_desired_capacity,
        )
    except ClientError as e:
        err = boto3mod.get_error(e)
        if e.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
            return {"exists": False}
        return {"error": err}
    return all(activity["StatusCode"] != "Failed" for activity in response["Activities"])
