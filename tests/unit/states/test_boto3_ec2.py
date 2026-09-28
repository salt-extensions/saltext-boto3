"""
Unit tests for the ``boto3_ec2`` state module.
"""

import pytest

from saltext.boto3.states import boto3_ec2

try:
    import botocore.exceptions  # noqa: F401  # pylint: disable=unused-import

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
        boto3_ec2: {
            "__opts__": {"test": False},
            "__salt__": {},
            "__states__": {},
        }
    }


def test_virtual(mock_salt):
    with mock_salt(boto3_ec2, {"boto3_ec2.get_key": True}):
        assert boto3_ec2.__virtual__() == "boto3_ec2"


def test_virtual_no_exec_module(mock_salt):
    with mock_salt(boto3_ec2, {}):
        result = boto3_ec2.__virtual__()
    assert result[0] is False


def test_key_present_already_exists(mock_salt):
    with mock_salt(boto3_ec2, {"boto3_ec2.get_key": ("mykey", "aa:bb")}):
        ret = boto3_ec2.key_present("mykey")
    assert ret["result"] is True
    assert "already exists" in ret["comment"]


def test_key_present_missing_both_options(mock_salt):
    with mock_salt(boto3_ec2, {"boto3_ec2.get_key": False}):
        ret = boto3_ec2.key_present("mykey")
    assert ret["result"] is False


def test_key_present_creates_new(mock_salt, tmp_path):
    salt_map = {
        "boto3_ec2.get_key": False,
        "boto3_ec2.create_key": "PRIV",
    }
    with mock_salt(boto3_ec2, salt_map):
        ret = boto3_ec2.key_present("mykey", save_private=str(tmp_path))
    assert ret["result"] is True
    assert ret["changes"]["new"] == "PRIV"


def test_key_present_create_failed(mock_salt, tmp_path):
    salt_map = {
        "boto3_ec2.get_key": False,
        "boto3_ec2.create_key": False,
    }
    with mock_salt(boto3_ec2, salt_map):
        ret = boto3_ec2.key_present("mykey", save_private=str(tmp_path))
    assert ret["result"] is False


def test_key_absent_deletes(mock_salt):
    salt_map = {
        "boto3_ec2.get_key": ("mykey", "aa:bb"),
        "boto3_ec2.delete_key": True,
    }
    with mock_salt(boto3_ec2, salt_map):
        ret = boto3_ec2.key_absent("mykey")
    assert ret["result"] is True
    assert ret["changes"]["old"] == "mykey"


def test_key_absent_delete_failed(mock_salt):
    salt_map = {
        "boto3_ec2.get_key": ("mykey", "aa:bb"),
        "boto3_ec2.delete_key": False,
    }
    with mock_salt(boto3_ec2, salt_map):
        ret = boto3_ec2.key_absent("mykey")
    assert ret["result"] is False


def test_eni_present_creates_test_mode(mock_salt):
    salt_map = {"boto3_ec2.get_network_interface": {"result": None}}
    with mock_salt(boto3_ec2, salt_map, test=True):
        ret = boto3_ec2.eni_present(
            "myeni", subnet_id="subnet-1", groups=["default"], description="desc"
        )
    assert ret["result"] is None


def test_eni_present_lookup_error(mock_salt):
    salt_map = {"boto3_ec2.get_network_interface": {"error": {"message": "boom"}}}
    with mock_salt(boto3_ec2, salt_map):
        ret = boto3_ec2.eni_present(
            "myeni", subnet_id="subnet-1", groups=["default"], description="desc"
        )
    assert ret["result"] is False


def test_eni_absent_already_gone(mock_salt):
    salt_map = {"boto3_ec2.get_network_interface": {"result": None}}
    with mock_salt(boto3_ec2, salt_map):
        ret = boto3_ec2.eni_absent("myeni")
    assert ret["result"] is True


def test_eni_absent_lookup_error(mock_salt):
    salt_map = {"boto3_ec2.get_network_interface": {"error": {"message": "boom"}}}
    with mock_salt(boto3_ec2, salt_map):
        ret = boto3_ec2.eni_absent("myeni")
    assert ret["result"] is False


def test_snapshot_created_happy(mock_salt):
    salt_map = {"boto3_ec2.create_image": "ami-1"}
    with mock_salt(boto3_ec2, salt_map):
        ret = boto3_ec2.snapshot_created(
            "mysnap", ami_name="myami", instance_name="foo", wait_until_available=False
        )
    assert ret["result"] is True


def test_snapshot_created_failure(mock_salt):
    salt_map = {"boto3_ec2.create_image": False}
    with mock_salt(boto3_ec2, salt_map):
        ret = boto3_ec2.snapshot_created(
            "mysnap", ami_name="myami", instance_name="foo", wait_until_available=False
        )
    assert ret["result"] is False


def test_instance_present_already_exists(mock_salt):
    salt_map = {
        "boto3_ec2.find_instances": ["i-1"],
        "boto3_ec2.get_attribute": False,
        "boto3_ec2.get_tags": [],
    }
    with mock_salt(boto3_ec2, salt_map):
        ret = boto3_ec2.instance_present("foo", instance_name="foo", image_id="ami-1")
    assert ret["result"] in (True, None)


def test_instance_present_test_mode_creates(mock_salt):
    salt_map = {"boto3_ec2.find_instances": []}
    with mock_salt(boto3_ec2, salt_map, test=True):
        ret = boto3_ec2.instance_present("foo", instance_name="foo", image_id="ami-1")
    assert ret["result"] is None


def test_instance_absent_already_gone(mock_salt):
    salt_map = {
        "boto3_ec2.get_id": None,
        "boto3_ec2.find_instances": [],
    }
    with mock_salt(boto3_ec2, salt_map):
        ret = boto3_ec2.instance_absent("foo", instance_name="foo")
    assert ret["result"] is True


def test_instance_absent_terminate_failed(mock_salt):
    salt_map = {
        "boto3_ec2.get_id": "i-1",
        "boto3_ec2.find_instances": ["i-1"],
        "boto3_ec2.get_attribute": {"disableApiTermination": False},
        "boto3_ec2.terminate": False,
    }
    with mock_salt(boto3_ec2, salt_map):
        ret = boto3_ec2.instance_absent("foo", instance_name="foo")
    assert ret["result"] is False


def test_volume_absent_no_volume(mock_salt):
    salt_map = {
        "boto3_ec2.get_id": "i-1",
        "boto3_ec2.get_all_volumes": [],
    }
    with mock_salt(boto3_ec2, salt_map):
        ret = boto3_ec2.volume_absent("foo", instance_name="foo", device="/dev/sdf")
    assert ret["result"] is True


def test_volume_absent_multiple_matches(mock_salt):
    salt_map = {
        "boto3_ec2.get_id": "i-1",
        "boto3_ec2.get_all_volumes": ["vol-1", "vol-2"],
    }
    with mock_salt(boto3_ec2, salt_map):
        ret = boto3_ec2.volume_absent("foo", instance_name="foo", device="/dev/sdf")
    assert ret["result"] is False


def test_volumes_tagged_happy(mock_salt):
    salt_map = {
        "boto3_ec2.set_volumes_tags": {
            "success": True,
            "comment": "ok",
            "changes": {},
        }
    }
    with mock_salt(boto3_ec2, salt_map):
        ret = boto3_ec2.volumes_tagged(
            "foo",
            tag_maps=[{"filters": {"volume_ids": ["vol-1"]}, "tags": {"Name": "foo"}}],
        )
    assert ret["result"] is True


def test_volumes_tagged_failure(mock_salt):
    salt_map = {
        "boto3_ec2.set_volumes_tags": {
            "success": False,
            "comment": "bad",
            "changes": {},
        }
    }
    with mock_salt(boto3_ec2, salt_map):
        ret = boto3_ec2.volumes_tagged(
            "foo",
            tag_maps=[{"filters": {"volume_ids": ["vol-1"]}, "tags": {"Name": "foo"}}],
        )
    assert ret["result"] is False


def test_volume_present_already_attached(mock_salt):
    vol = {
        "VolumeId": "vol-1",
        "AvailabilityZone": "us-east-1a",
        "Attachments": [{"InstanceId": "i-1", "Device": "/dev/sdf"}],
    }
    salt_map = {
        "boto3_ec2.get_id": "i-1",
        "boto3_ec2.find_instances": [
            {"InstanceId": "i-1", "Placement": {"AvailabilityZone": "us-east-1a"}}
        ],
        "boto3_ec2.get_all_volumes": [vol],
    }
    with mock_salt(boto3_ec2, salt_map):
        ret = boto3_ec2.volume_present(
            "foo", instance_name="foo", device="/dev/sdf", volume_id="vol-1"
        )
    assert ret["result"] is True


def test_volume_present_no_instance(mock_salt):
    salt_map = {
        "boto3_ec2.get_id": None,
        "boto3_ec2.get_all_volumes": [],
        "boto3_ec2.find_instances": [],
    }
    with mock_salt(boto3_ec2, salt_map):
        with pytest.raises(Exception):
            boto3_ec2.volume_present(
                "foo", instance_name="foo", device="/dev/sdf", volume_id="vol-1"
            )


def test_private_ips_present_no_op(mock_salt):
    salt_map = {
        "boto3_ec2.get_network_interface": {
            "result": {
                "id": "eni-1",
                "private_ip_addresses": [{"private_ip_address": "10.0.0.5", "primary": True}],
            }
        }
    }
    with mock_salt(boto3_ec2, salt_map):
        ret = boto3_ec2.private_ips_present(
            "eni-1", network_interface_id="eni-1", private_ip_addresses=["10.0.0.5"]
        )
    assert ret["result"] is True


def test_private_ips_present_assign_failed(mock_salt):
    eni_before = {
        "result": {
            "id": "eni-1",
            "private_ip_addresses": [{"private_ip_address": "10.0.0.5", "primary": True}],
        }
    }
    salt_map = {
        "boto3_ec2.get_network_interface": eni_before,
        "boto3_ec2.assign_private_ip_addresses": False,
    }
    with mock_salt(boto3_ec2, salt_map):
        ret = boto3_ec2.private_ips_present(
            "eni-1", network_interface_id="eni-1", private_ip_addresses=["10.0.0.6"]
        )
    assert ret["result"] is False


def test_private_ips_absent_no_op(mock_salt):
    salt_map = {
        "boto3_ec2.get_network_interface": {
            "result": {
                "id": "eni-1",
                "private_ip_addresses": [
                    {"private_ip_address": "10.0.0.5", "primary": True},
                ],
            }
        }
    }
    with mock_salt(boto3_ec2, salt_map):
        ret = boto3_ec2.private_ips_absent(
            "eni-1", network_interface_id="eni-1", private_ip_addresses=["10.0.0.6"]
        )
    assert ret["result"] is True


def test_private_ips_absent_unassign_failed(mock_salt):
    eni_before = {
        "result": {
            "id": "eni-1",
            "private_ip_addresses": [
                {"private_ip_address": "10.0.0.5", "primary": True},
                {"private_ip_address": "10.0.0.6", "primary": False},
            ],
        }
    }
    salt_map = {
        "boto3_ec2.get_network_interface": eni_before,
        "boto3_ec2.unassign_private_ip_addresses": False,
    }
    with mock_salt(boto3_ec2, salt_map):
        ret = boto3_ec2.private_ips_absent(
            "eni-1", network_interface_id="eni-1", private_ip_addresses=["10.0.0.6"]
        )
    assert ret["result"] is False


def test_launch_template_present_already_exists(mock_salt):
    with mock_salt(
        boto3_ec2, {"boto3_ec2.describe_launch_templates": [{"LaunchTemplateName": "lt"}]}
    ):
        ret = boto3_ec2.launch_template_present("lt")
    assert ret["result"] is True
    assert "already present" in ret["comment"]
    assert not ret["changes"]


def test_launch_template_present_creates(mock_salt):
    salt_map = {
        "boto3_ec2.describe_launch_templates": [],
        "boto3_ec2.create_launch_template": {
            "LaunchTemplateId": "lt-1",
            "LaunchTemplateName": "lt",
        },
    }
    with mock_salt(boto3_ec2, salt_map):
        ret = boto3_ec2.launch_template_present("lt", image_id="ami-123", instance_type="t3.medium")
    assert ret["result"] is True
    assert ret["changes"]["new"]


def test_launch_template_present_test_mode(mock_salt):
    with mock_salt(boto3_ec2, {"boto3_ec2.describe_launch_templates": []}, test=True):
        ret = boto3_ec2.launch_template_present("lt")
    assert ret["result"] is None
    assert "would be created" in ret["comment"]


def test_launch_template_present_describe_error(mock_salt):
    with mock_salt(boto3_ec2, {"boto3_ec2.describe_launch_templates": {"error": "AccessDenied"}}):
        ret = boto3_ec2.launch_template_present("lt")
    assert ret["result"] is False
    assert "Error describing" in ret["comment"]


def test_launch_template_absent_already_gone(mock_salt):
    with mock_salt(boto3_ec2, {"boto3_ec2.describe_launch_templates": []}):
        ret = boto3_ec2.launch_template_absent("lt")
    assert ret["result"] is True
    assert "already absent" in ret["comment"]


def test_launch_template_absent_deletes(mock_salt):
    salt_map = {
        "boto3_ec2.describe_launch_templates": [{"LaunchTemplateName": "lt"}],
        "boto3_ec2.delete_launch_template": {"LaunchTemplateName": "lt"},
    }
    with mock_salt(boto3_ec2, salt_map):
        ret = boto3_ec2.launch_template_absent("lt")
    assert ret["result"] is True
    assert ret["changes"]["old"]
    assert "deleted" in ret["comment"]


def test_launch_template_absent_test_mode(mock_salt):
    with mock_salt(
        boto3_ec2,
        {"boto3_ec2.describe_launch_templates": [{"LaunchTemplateName": "lt"}]},
        test=True,
    ):
        ret = boto3_ec2.launch_template_absent("lt")
    assert ret["result"] is None
    assert "would be deleted" in ret["comment"]


def test_instance_metadata_options_no_options(mock_salt):
    with mock_salt(boto3_ec2, {}):
        ret = boto3_ec2.instance_metadata_options("s")
    assert ret["result"] is False
    assert "At least one" in ret["comment"]


def test_instance_metadata_options_test_mode(mock_salt):
    with mock_salt(boto3_ec2, {}, test=True):
        ret = boto3_ec2.instance_metadata_options("s", http_tokens="required")
    assert ret["result"] is None
    assert "would be applied" in ret["comment"]


def test_instance_metadata_options_no_instances(mock_salt):
    with mock_salt(boto3_ec2, {"boto3_ec2.find_instances": []}):
        ret = boto3_ec2.instance_metadata_options("s", tags={"k": "v"}, http_tokens="required")
    assert ret["result"] is True
    assert "No matching" in ret["comment"]


def test_instance_metadata_options_all_compliant(mock_salt):
    instances = [
        {
            "InstanceId": "i-1",
            "MetadataOptions": {"HttpTokens": "required", "HttpPutResponseHopLimit": 3},
        },
        {
            "InstanceId": "i-2",
            "MetadataOptions": {"HttpTokens": "required", "HttpPutResponseHopLimit": 4},
        },
    ]
    salt_map = {"boto3_ec2.find_instances": instances}
    with mock_salt(boto3_ec2, salt_map):
        ret = boto3_ec2.instance_metadata_options(
            "s",
            tags={"eks:cluster-name": "my-cluster"},
            http_tokens="required",
            http_put_response_hop_limit=3,
        )
    assert ret["result"] is True
    assert not ret["changes"]
    assert "compliant" in ret["comment"]


def test_instance_metadata_options_hop_limit_already_higher(mock_salt):
    # hop limit >= desired means compliant — should not update
    instances = [
        {"InstanceId": "i-1", "MetadataOptions": {"HttpPutResponseHopLimit": 5}},
    ]
    salt_map = {"boto3_ec2.find_instances": instances}
    with mock_salt(boto3_ec2, salt_map):
        ret = boto3_ec2.instance_metadata_options("s", http_put_response_hop_limit=3)
    assert ret["result"] is True
    assert not ret["changes"]


def test_instance_metadata_options_updates_instances(mock_salt):
    instances = [
        {
            "InstanceId": "i-1",
            "MetadataOptions": {"HttpTokens": "optional", "HttpPutResponseHopLimit": 1},
        },
    ]
    salt_map = {
        "boto3_ec2.find_instances": instances,
        "boto3_ec2.modify_instance_metadata_options": {},
    }
    with mock_salt(boto3_ec2, salt_map):
        ret = boto3_ec2.instance_metadata_options(
            "s",
            tags={"eks:cluster-name": "my-cluster"},
            http_tokens="required",
            http_put_response_hop_limit=3,
        )
    assert ret["result"] is True
    assert "i-1" in ret["changes"]["updated"]


def test_instance_metadata_options_by_name(mock_salt):
    instances = [
        {"InstanceId": "i-bastion", "MetadataOptions": {"HttpEndpoint": "enabled"}},
    ]
    salt_map = {
        "boto3_ec2.find_instances": instances,
        "boto3_ec2.modify_instance_metadata_options": {},
    }
    with mock_salt(boto3_ec2, salt_map):
        ret = boto3_ec2.instance_metadata_options(
            "s", instance_name="bastion-host", http_endpoint="disabled"
        )
    assert ret["result"] is True
    assert "i-bastion" in ret["changes"]["updated"]


def test_instance_metadata_options_error(mock_salt):
    instances = [
        {
            "InstanceId": "i-bad",
            "MetadataOptions": {"HttpTokens": "optional", "HttpPutResponseHopLimit": 1},
        }
    ]
    salt_map = {
        "boto3_ec2.find_instances": instances,
        "boto3_ec2.modify_instance_metadata_options": {"error": "AccessDenied"},
    }
    with mock_salt(boto3_ec2, salt_map):
        ret = boto3_ec2.instance_metadata_options(
            "s", http_tokens="required", http_put_response_hop_limit=3
        )
    assert ret["result"] is False
