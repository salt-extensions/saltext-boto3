"""
Unit tests for the ``boto3_elb`` execution module.
"""

import pytest

from saltext.boto3.modules import boto3_elb

try:
    import botocore  # pylint: disable=unused-import

    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False


pytestmark = [
    pytest.mark.skip_on_fips_enabled_platform,
    pytest.mark.skipif(HAS_BOTO3 is False, reason="botocore is required for these tests."),
]


@pytest.fixture
def configure_loader_modules():
    return {
        boto3_elb: {
            "__opts__": {},
            "__context__": {},
            "__salt__": {},
        }
    }


@pytest.fixture
def conn(make_conn):
    with make_conn(boto3_elb) as client:
        yield client


def test_get_elb_config_populates_fields(conn):
    conn.describe_load_balancers.return_value = {
        "LoadBalancerDescriptions": [
            {
                "AvailabilityZones": ["us-east-1a"],
                "ListenerDescriptions": [
                    {
                        "Listener": {
                            "Protocol": "HTTPS",
                            "LoadBalancerPort": 443,
                            "InstanceProtocol": "HTTP",
                            "InstancePort": 80,
                            "SSLCertificateId": "arn:cert",
                        },
                        "PolicyNames": ["pol1"],
                    }
                ],
                "BackendServerDescriptions": [{"InstancePort": 80, "PolicyNames": ["bp"]}],
                "Subnets": ["sn-1"],
                "SecurityGroups": ["sg-1"],
                "Scheme": "internet-facing",
                "DNSName": "x.elb.amazonaws.com",
                "Policies": {
                    "AppCookieStickinessPolicies": [{"PolicyName": "ac"}],
                    "LBCookieStickinessPolicies": [{"PolicyName": "lc"}],
                    "OtherPolicies": ["op"],
                },
                "CanonicalHostedZoneName": "chz",
                "CanonicalHostedZoneNameID": "chzid",
                "VPCId": "vpc-1",
            }
        ]
    }
    conn.describe_tags.return_value = {
        "TagDescriptions": [{"LoadBalancerName": "e", "Tags": [{"Key": "k", "Value": "v"}]}]
    }
    result = boto3_elb.get_elb_config("e")
    assert result["availability_zones"] == ["us-east-1a"]
    assert result["listeners"][0]["certificate"] == "arn:cert"
    assert result["listeners"][0]["policies"] == ["pol1"]
    assert result["backends"][0]["instance_port"] == 80
    assert result["vpc_id"] == "vpc-1"
    assert result["tags"] == {"k": "v"}
    assert result["policies"] == ["ac", "lc", "op"]


def test_create_creates(conn):
    conn.describe_load_balancers.return_value = {"LoadBalancerDescriptions": []}
    conn.create_load_balancer.return_value = {"DNSName": "x"}
    assert (
        boto3_elb.create(
            "e",
            ["az"],
            [{"elb_port": 80, "elb_protocol": "HTTP", "instance_port": 80}],
            subnets=["sn"],
            security_groups=["sg"],
        )
        is True
    )
    call_kwargs = conn.create_load_balancer.call_args.kwargs
    assert call_kwargs["LoadBalancerName"] == "e"
    assert call_kwargs["AvailabilityZones"] == ["az"]
    assert call_kwargs["Subnets"] == ["sn"]
    assert call_kwargs["SecurityGroups"] == ["sg"]
    assert call_kwargs["Listeners"][0]["Protocol"] == "HTTP"


def test_create_accepts_json_strings(conn):
    conn.describe_load_balancers.return_value = {"LoadBalancerDescriptions": []}
    conn.create_load_balancer.return_value = {"DNSName": "x"}
    assert (
        boto3_elb.create(
            "e",
            '["az"]',
            '[{"elb_port": 80, "elb_protocol": "HTTP", "instance_port": 80}]',
        )
        is True
    )


def test_create_client_error(conn, client_error):
    conn.describe_load_balancers.return_value = {"LoadBalancerDescriptions": []}
    conn.create_load_balancer.side_effect = client_error("Boom", "CreateLoadBalancer")
    assert (
        boto3_elb.create(
            "e", ["az"], [{"elb_port": 80, "elb_protocol": "HTTP", "instance_port": 80}]
        )
        is False
    )


def test_create_listeners_success(conn):
    assert (
        boto3_elb.create_listeners(
            "e", [{"elb_port": 80, "elb_protocol": "HTTP", "instance_port": 80}]
        )
        is True
    )
    conn.create_load_balancer_listeners.assert_called_once()


def test_create_listeners_accepts_string(conn):
    assert (
        boto3_elb.create_listeners(
            "e", '[{"elb_port": 80, "elb_protocol": "HTTP", "instance_port": 80}]'
        )
        is True
    )


def test_create_listeners_error(conn, client_error):
    conn.create_load_balancer_listeners.side_effect = client_error(
        "Boom", "CreateLoadBalancerListeners"
    )
    assert (
        boto3_elb.create_listeners(
            "e", [{"elb_port": 80, "elb_protocol": "HTTP", "instance_port": 80}]
        )
        is False
    )


def test_get_attributes(conn):
    conn.describe_load_balancer_attributes.return_value = {
        "LoadBalancerAttributes": {
            "AccessLog": {
                "Enabled": True,
                "S3BucketName": "b",
                "S3BucketPrefix": "p",
                "EmitInterval": 5,
            },
            "CrossZoneLoadBalancing": {"Enabled": True},
            "ConnectionDraining": {"Enabled": True, "Timeout": 300},
            "ConnectionSettings": {"IdleTimeout": 60},
        }
    }
    ret = boto3_elb.get_attributes("e")
    assert ret["access_log"]["enabled"] is True
    assert ret["access_log"]["s3_bucket_name"] == "b"
    assert ret["connection_draining"]["timeout"] == 300
    assert ret["connecting_settings"]["idle_timeout"] == 60


def test_get_attributes_error(conn, client_error):
    conn.describe_load_balancer_attributes.side_effect = client_error(
        "Boom", "DescribeLoadBalancerAttributes"
    )
    assert not boto3_elb.get_attributes("e")


def test_set_attributes_access_log(conn):
    assert (
        boto3_elb.set_attributes(
            "e",
            {
                "access_log": {
                    "enabled": True,
                    "s3_bucket_name": "b",
                    "s3_bucket_prefix": "p",
                    "emit_interval": 5,
                }
            },
        )
        is True
    )
    kwargs = conn.modify_load_balancer_attributes.call_args.kwargs
    assert kwargs["LoadBalancerAttributes"]["AccessLog"]["S3BucketName"] == "b"


def test_set_attributes_all(conn):
    assert (
        boto3_elb.set_attributes(
            "e",
            {
                "cross_zone_load_balancing": {"enabled": True},
                "connection_draining": {"enabled": True, "timeout": 60},
                "connecting_settings": {"idle_timeout": 90},
            },
        )
        is True
    )
    kwargs = conn.modify_load_balancer_attributes.call_args.kwargs
    api = kwargs["LoadBalancerAttributes"]
    assert api["CrossZoneLoadBalancing"]["Enabled"] is True
    assert api["ConnectionDraining"]["Timeout"] == 60
    assert api["ConnectionSettings"]["IdleTimeout"] == 90


def test_set_attributes_error(conn, client_error):
    conn.modify_load_balancer_attributes.side_effect = client_error("Boom", "ModifyAttributes")
    assert boto3_elb.set_attributes("e", {"cross_zone_load_balancing": {"enabled": True}}) is False


def test_get_health_check(conn):
    conn.describe_load_balancers.return_value = {
        "LoadBalancerDescriptions": [
            {
                "HealthCheck": {
                    "Interval": 30,
                    "Target": "HTTP:80/",
                    "HealthyThreshold": 3,
                    "Timeout": 5,
                    "UnhealthyThreshold": 2,
                }
            }
        ]
    }
    ret = boto3_elb.get_health_check("e")
    assert ret["target"] == "HTTP:80/"
    assert ret["interval"] == 30


def test_get_instance_health(conn):
    conn.describe_instance_health.return_value = {
        "InstanceStates": [
            {
                "InstanceId": "i-1",
                "Description": "In service",
                "State": "InService",
                "ReasonCode": "N/A",
            }
        ]
    }
    ret = boto3_elb.get_instance_health("e")
    assert ret == [
        {
            "instance_id": "i-1",
            "description": "In service",
            "state": "InService",
            "reason_code": "N/A",
        }
    ]
