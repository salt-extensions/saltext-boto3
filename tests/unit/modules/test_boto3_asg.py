"""
Unit tests for the ``boto3_asg`` execution module.
"""

from unittest.mock import MagicMock

import pytest

from saltext.boto3.modules import boto3_asg

try:
    import botocore  # pylint: disable=unused-import

    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

pytestmark = [
    pytest.mark.skip_on_fips_enabled_platform,
    pytest.mark.skipif(HAS_BOTO3 is False, reason="The boto3 module must be installed."),
]


@pytest.fixture
def configure_loader_modules():
    return {
        boto3_asg: {
            "__opts__": {},
            "__context__": {},
            "__salt__": {
                "boto3_secgroup.convert_to_group_ids": MagicMock(
                    side_effect=lambda groups, **_: ["sg-" + g for g in groups]
                ),
            },
        }
    }


@pytest.fixture
def conn(make_conn):
    with make_conn(boto3_asg) as client:
        yield client


def test_exists_true(conn):
    conn.describe_auto_scaling_groups.return_value = {
        "AutoScalingGroups": [{"AutoScalingGroupName": "myasg"}]
    }
    assert boto3_asg.exists("myasg") is True


def test_exists_false(conn):
    conn.describe_auto_scaling_groups.return_value = {"AutoScalingGroups": []}
    assert boto3_asg.exists("myasg") is False


def test_exists_client_error(conn, client_error):
    conn.describe_auto_scaling_groups.side_effect = client_error(
        "AuthFailure", "DescribeAutoScalingGroups"
    )
    assert boto3_asg.exists("myasg") is False


def test_get_config_not_found(conn):
    conn.describe_auto_scaling_groups.return_value = {"AutoScalingGroups": []}
    assert boto3_asg.get_config("myasg") == {}


def test_get_config_populated(conn):
    conn.describe_auto_scaling_groups.return_value = {
        "AutoScalingGroups": [
            {
                "AutoScalingGroupName": "myasg",
                "AvailabilityZones": ["us-east-1a", "us-east-1b"],
                "DefaultCooldown": 300,
                "DesiredCapacity": 2,
                "HealthCheckGracePeriod": 60,
                "HealthCheckType": "EC2",
                "LaunchConfigurationName": "mylc",
                "LoadBalancerNames": ["lb1"],
                "MaxSize": 5,
                "MinSize": 1,
                "PlacementGroup": None,
                "VPCZoneIdentifier": "subnet-1,subnet-2",
                "Tags": [
                    {"Key": "role", "Value": "web", "PropagateAtLaunch": True},
                ],
                "TerminationPolicies": ["Default"],
                "SuspendedProcesses": [
                    {"ProcessName": "Launch"},
                    {"ProcessName": "AZRebalance"},
                ],
            }
        ]
    }
    conn.describe_policies.return_value = {
        "ScalingPolicies": [
            {
                "PolicyName": "scale-up",
                "AdjustmentType": "ChangeInCapacity",
                "ScalingAdjustment": 1,
                "Cooldown": 60,
            }
        ]
    }
    conn.describe_scheduled_actions.return_value = {
        "ScheduledUpdateGroupActions": [
            {
                "ScheduledActionName": "morning",
                "MinSize": 2,
                "MaxSize": 10,
                "DesiredCapacity": 5,
                "Recurrence": "0 8 * * *",
            }
        ]
    }
    cfg = boto3_asg.get_config("myasg")
    assert cfg["name"] == "myasg"
    assert cfg["min_size"] == 1
    assert cfg["max_size"] == 5
    assert cfg["launch_config_name"] == "mylc"
    assert cfg["vpc_zone_identifier"] == ["subnet-1", "subnet-2"]
    assert cfg["tags"][0]["key"] == "role"
    assert cfg["suspended_processes"] == ["AZRebalance", "Launch"]
    assert cfg["scaling_policies"][0]["name"] == "scale-up"
    assert cfg["scheduled_actions"]["morning"]["max_size"] == 10


def test_get_config_client_error(conn, client_error):
    conn.describe_auto_scaling_groups.side_effect = client_error(
        "AuthFailure", "DescribeAutoScalingGroups"
    )
    assert boto3_asg.get_config("myasg") == {}


def test_create_basic(conn):
    assert (
        boto3_asg.create(
            name="myasg",
            launch_config_name="mylc",
            availability_zones=["us-east-1a"],
            min_size=1,
            max_size=3,
            vpc_zone_identifier=["subnet-1", "subnet-2"],
            tags=[{"key": "role", "value": "web", "propagate_at_launch": True}],
        )
        is True
    )
    kwargs = conn.create_auto_scaling_group.call_args.kwargs
    assert kwargs["AutoScalingGroupName"] == "myasg"
    assert kwargs["LaunchConfigurationName"] == "mylc"
    assert kwargs["VPCZoneIdentifier"] == "subnet-1,subnet-2"
    assert kwargs["Tags"][0]["Key"] == "role"
    assert kwargs["Tags"][0]["PropagateAtLaunch"] is True


def test_create_with_policies_and_suspended(conn):
    assert (
        boto3_asg.create(
            name="myasg",
            launch_config_name="mylc",
            availability_zones=["us-east-1a"],
            min_size=1,
            max_size=3,
            suspended_processes=["Launch"],
            scaling_policies=[
                {
                    "name": "scale-up",
                    "adjustment_type": "ChangeInCapacity",
                    "scaling_adjustment": 1,
                    "cooldown": 60,
                }
            ],
            scheduled_actions={
                "morning": {"min_size": 1, "max_size": 5, "recurrence": "0 8 * * *"}
            },
            notification_arn="arn:aws:sns:us-east-1:x:topic",
            notification_types=["autoscaling:EC2_INSTANCE_LAUNCH"],
        )
        is True
    )
    conn.suspend_processes.assert_called_once_with(
        AutoScalingGroupName="myasg", ScalingProcesses=["Launch"]
    )
    conn.put_scaling_policy.assert_called_once()
    conn.put_scheduled_update_group_action.assert_called_once()
    conn.put_notification_configuration.assert_called_once()


def test_create_missing_tag_key_returns_false(conn):
    assert (
        boto3_asg.create(
            name="myasg",
            launch_config_name="mylc",
            availability_zones=["us-east-1a"],
            min_size=1,
            max_size=3,
            tags=[{"value": "web"}],
        )
        is False
    )
    conn.create_auto_scaling_group.assert_not_called()


def test_create_client_error(conn, client_error):
    conn.create_auto_scaling_group.side_effect = client_error(
        "AlreadyExists", "CreateAutoScalingGroup"
    )
    assert (
        boto3_asg.create(
            name="myasg",
            launch_config_name="mylc",
            availability_zones=["us-east-1a"],
            min_size=1,
            max_size=3,
        )
        is False
    )


def test_update_tag_add_and_delete(conn):
    conn.describe_tags.return_value = {
        "Tags": [
            {
                "ResourceId": "myasg",
                "ResourceType": "auto-scaling-group",
                "Key": "old",
                "Value": "x",
                "PropagateAtLaunch": True,
            }
        ]
    }
    conn.describe_policies.return_value = {"ScalingPolicies": []}
    conn.describe_scheduled_actions.return_value = {"ScheduledUpdateGroupActions": []}
    ok, msg = boto3_asg.update(
        name="myasg",
        launch_config_name="mylc",
        availability_zones=["us-east-1a"],
        min_size=1,
        max_size=3,
        tags=[{"key": "new", "value": "y", "propagate_at_launch": False}],
    )
    assert ok is True
    assert msg == ""
    conn.create_or_update_tags.assert_called_once()
    added = conn.create_or_update_tags.call_args.kwargs["Tags"][0]
    assert added["Key"] == "new"
    conn.delete_tags.assert_called_once()
    deleted = conn.delete_tags.call_args.kwargs["Tags"][0]
    assert deleted["Key"] == "old"


def test_update_reconciles_load_balancers(conn):
    conn.describe_tags.return_value = {"Tags": []}
    conn.describe_load_balancers.return_value = {"LoadBalancers": [{"LoadBalancerName": "old-lb"}]}
    conn.describe_policies.return_value = {"ScalingPolicies": []}
    conn.describe_scheduled_actions.return_value = {"ScheduledUpdateGroupActions": []}
    ok, _msg = boto3_asg.update(
        name="myasg",
        launch_config_name="mylc",
        availability_zones=["us-east-1a"],
        min_size=1,
        max_size=3,
        load_balancers=["new-lb"],
    )
    assert ok is True
    conn.attach_load_balancers.assert_called_once_with(
        AutoScalingGroupName="myasg", LoadBalancerNames=["new-lb"]
    )
    conn.detach_load_balancers.assert_called_once_with(
        AutoScalingGroupName="myasg", LoadBalancerNames=["old-lb"]
    )


def test_update_recreates_policies_and_actions(conn):
    conn.describe_tags.return_value = {"Tags": []}
    conn.describe_policies.return_value = {"ScalingPolicies": [{"PolicyName": "old-policy"}]}
    conn.describe_scheduled_actions.return_value = {
        "ScheduledUpdateGroupActions": [{"ScheduledActionName": "old-action"}]
    }
    ok, _msg = boto3_asg.update(
        name="myasg",
        launch_config_name="mylc",
        availability_zones=["us-east-1a"],
        min_size=1,
        max_size=3,
        scaling_policies=[
            {
                "name": "scale-up",
                "adjustment_type": "ChangeInCapacity",
                "scaling_adjustment": 1,
                "cooldown": 60,
            }
        ],
        scheduled_actions={"evening": {"min_size": 0, "recurrence": "0 20 * * *"}},
    )
    assert ok is True
    conn.delete_policy.assert_called_once_with(
        AutoScalingGroupName="myasg", PolicyName="old-policy"
    )
    conn.delete_scheduled_action.assert_called_once_with(
        AutoScalingGroupName="myasg", ScheduledActionName="old-action"
    )
    conn.put_scaling_policy.assert_called_once()
    conn.put_scheduled_update_group_action.assert_called_once()


def test_update_client_error_returns_tuple(conn, client_error):
    conn.describe_tags.return_value = {"Tags": []}
    conn.update_auto_scaling_group.side_effect = client_error(
        "ValidationError", "UpdateAutoScalingGroup"
    )
    ok, msg = boto3_asg.update(
        name="myasg",
        launch_config_name="mylc",
        availability_zones=["us-east-1a"],
        min_size=1,
        max_size=3,
    )
    assert ok is False
    assert "ValidationError" in msg or "boom" in msg


def test_delete_force(conn):
    assert boto3_asg.delete("myasg", force=True) is True
    conn.delete_auto_scaling_group.assert_called_once_with(
        AutoScalingGroupName="myasg", ForceDelete=True
    )


def test_delete_client_error(conn, client_error):
    conn.delete_auto_scaling_group.side_effect = client_error(
        "ResourceInUse", "DeleteAutoScalingGroup"
    )
    assert boto3_asg.delete("myasg") is False


def test_launch_configuration_exists_true(conn):
    conn.describe_launch_configurations.return_value = {
        "LaunchConfigurations": [{"LaunchConfigurationName": "mylc"}]
    }
    assert boto3_asg.launch_configuration_exists("mylc") is True


def test_launch_configuration_exists_false(conn):
    conn.describe_launch_configurations.return_value = {"LaunchConfigurations": []}
    assert boto3_asg.launch_configuration_exists("mylc") is False


def test_list_launch_configurations(conn):
    conn.describe_launch_configurations.return_value = {
        "LaunchConfigurations": [
            {"LaunchConfigurationName": "a"},
            {"LaunchConfigurationName": "b"},
        ]
    }
    assert boto3_asg.list_launch_configurations() == ["a", "b"]


def test_describe_launch_configuration_found(conn):
    conn.describe_launch_configurations.return_value = {
        "LaunchConfigurations": [{"LaunchConfigurationName": "mylc", "ImageId": "ami-1"}]
    }
    assert boto3_asg.describe_launch_configuration("mylc")["ImageId"] == "ami-1"


def test_describe_launch_configuration_missing(conn):
    conn.describe_launch_configurations.return_value = {"LaunchConfigurations": []}
    assert boto3_asg.describe_launch_configuration("mylc") is None


def test_create_launch_configuration_translates_block_device_mappings(conn):
    assert (
        boto3_asg.create_launch_configuration(
            name="mylc",
            image_id="ami-1",
            instance_type="t2.micro",
            key_name="mykey",
            block_device_mappings=[
                {"/dev/sda1": {"volume_type": "gp2", "size": 20, "delete_on_termination": True}}
            ],
        )
        is True
    )
    kwargs = conn.create_launch_configuration.call_args.kwargs
    bdm = kwargs["BlockDeviceMappings"][0]
    assert bdm["DeviceName"] == "/dev/sda1"
    assert bdm["Ebs"]["VolumeType"] == "gp2"
    assert bdm["Ebs"]["VolumeSize"] == 20


def test_create_launch_configuration_converts_security_groups_with_vpc(conn):
    assert (
        boto3_asg.create_launch_configuration(
            name="mylc",
            image_id="ami-1",
            vpc_id="vpc-1",
            security_groups=["web"],
        )
        is True
    )
    kwargs = conn.create_launch_configuration.call_args.kwargs
    assert kwargs["SecurityGroups"] == ["sg-web"]


def test_delete_launch_configuration(conn):
    assert boto3_asg.delete_launch_configuration("mylc") is True
    conn.delete_launch_configuration.assert_called_once_with(LaunchConfigurationName="mylc")


def test_delete_launch_configuration_error(conn, client_error):
    conn.delete_launch_configuration.side_effect = client_error(
        "ResourceInUse", "DeleteLaunchConfiguration"
    )
    assert boto3_asg.delete_launch_configuration("mylc") is False


def test_get_scaling_policy_arn_found(conn):
    conn.describe_policies.return_value = {
        "ScalingPolicies": [
            {"PolicyName": "other", "PolicyARN": "arn:1"},
            {"PolicyName": "mypolicy", "PolicyARN": "arn:mypolicy"},
        ]
    }
    assert boto3_asg.get_scaling_policy_arn("myasg", "mypolicy") == "arn:mypolicy"


def test_get_scaling_policy_arn_not_found(conn):
    conn.describe_policies.return_value = {"ScalingPolicies": []}
    assert boto3_asg.get_scaling_policy_arn("myasg", "mypolicy") is None


def test_list_groups(conn):
    conn.describe_auto_scaling_groups.return_value = {
        "AutoScalingGroups": [
            {"AutoScalingGroupName": "asg1"},
            {"AutoScalingGroupName": "asg2"},
        ]
    }
    assert boto3_asg.list_groups() == ["asg1", "asg2"]


def test_get_instances_returns_private_ips(conn):
    conn.describe_auto_scaling_groups.return_value = {
        "AutoScalingGroups": [
            {
                "AutoScalingGroupName": "myasg",
                "Instances": [
                    {"InstanceId": "i-1", "LifecycleState": "InService", "HealthStatus": "Healthy"},
                    {
                        "InstanceId": "i-2",
                        "LifecycleState": "InService",
                        "HealthStatus": "Unhealthy",
                    },
                    {"InstanceId": "i-3", "LifecycleState": "Pending", "HealthStatus": "Healthy"},
                ],
            }
        ]
    }
    conn.describe_instances.return_value = {
        "Reservations": [{"Instances": [{"InstanceId": "i-1", "PrivateIpAddress": "10.0.0.1"}]}]
    }
    result = boto3_asg.get_instances("myasg")
    # Only i-1 is InService + Healthy
    conn.describe_instances.assert_called_once_with(InstanceIds=["i-1"])
    assert result == ["10.0.0.1"]


def test_get_instances_no_matches_returns_empty(conn):
    conn.describe_auto_scaling_groups.return_value = {
        "AutoScalingGroups": [{"AutoScalingGroupName": "myasg", "Instances": []}]
    }
    assert not boto3_asg.get_instances("myasg")


def test_get_instances_wrong_asg_count(conn):
    conn.describe_auto_scaling_groups.return_value = {"AutoScalingGroups": []}
    assert boto3_asg.get_instances("myasg") is False


def test_enter_standby_ok(conn):
    conn.enter_standby.return_value = {"Activities": [{"StatusCode": "PendingSpotBidPlacement"}]}
    assert boto3_asg.enter_standby("myasg", ["i-1"]) is True


def test_enter_standby_failed_activity(conn):
    conn.enter_standby.return_value = {"Activities": [{"StatusCode": "Failed"}]}
    assert boto3_asg.enter_standby("myasg", ["i-1"]) is False


def test_enter_standby_resource_not_found(conn, client_error):
    conn.enter_standby.side_effect = client_error("ResourceNotFoundException", "EnterStandby")
    assert boto3_asg.enter_standby("myasg", ["i-1"]) == {"exists": False}


def test_exit_standby_error_returns_error_dict(conn, client_error):
    conn.exit_standby.side_effect = client_error("AuthFailure", "ExitStandby")
    result = boto3_asg.exit_standby("myasg", ["i-1"])
    assert "error" in result


def test_get_cloud_init_mime_contains_sections():
    result = boto3_asg.get_cloud_init_mime(
        {
            "scripts": {"hello.sh": "#!/bin/sh\necho hi"},
            "cloud-config": {"runcmd": ["echo hi"]},
        }
    )
    assert "x-shellscript" in result
    assert "cloud-config" in result
