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
