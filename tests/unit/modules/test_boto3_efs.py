"""
Unit tests for the ``boto3_efs`` execution module.
"""

import pytest

from saltext.boto3.modules import boto3_efs

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
        boto3_efs: {
            "__opts__": {},
            "__context__": {},
            "__salt__": {},
        }
    }


@pytest.fixture
def conn(make_conn):
    with make_conn(boto3_efs) as client:
        yield client


def test_create_file_system(conn):
    conn.create_file_system.return_value = {
        "FileSystemId": "fs-123",
        "PerformanceMode": "generalPurpose",
        "Name": "placeholder",
    }
    result = boto3_efs.create_file_system("efs-name")
    assert result["FileSystemId"] == "fs-123"
    assert result["Name"] == "efs-name"
    conn.create_file_system.assert_called_once_with(
        CreationToken="efs-name", PerformanceMode="generalPurpose"
    )
    conn.create_tags.assert_called_once_with(
        FileSystemId="fs-123", Tags=[{"Key": "Name", "Value": "efs-name"}]
    )


def test_create_file_system_explicit_creation_token(conn):
    conn.create_file_system.return_value = {"FileSystemId": "fs-1"}
    boto3_efs.create_file_system("efs-name", creation_token="tok")
    conn.create_file_system.assert_called_once_with(
        CreationToken="tok", PerformanceMode="generalPurpose"
    )


def test_create_file_system_client_error(conn, client_error):
    conn.create_file_system.side_effect = client_error("AccessDenied", "CreateFileSystem")
    assert boto3_efs.create_file_system("efs-name") is False
    conn.create_tags.assert_not_called()


def test_create_file_system_tag_failure_swallowed(conn, client_error):
    conn.create_file_system.return_value = {"FileSystemId": "fs-1"}
    conn.create_tags.side_effect = client_error("AccessDenied", "CreateTags")
    result = boto3_efs.create_file_system("efs-name")
    assert result["FileSystemId"] == "fs-1"


def test_create_file_system_no_id_in_response(conn):
    conn.create_file_system.return_value = {"PerformanceMode": "generalPurpose"}
    result = boto3_efs.create_file_system("efs-name")
    assert "Name" not in result
    conn.create_tags.assert_not_called()


def test_create_mount_target(conn):
    conn.create_mount_target.return_value = {"MountTargetId": "mt-1"}
    result = boto3_efs.create_mount_target("fs-1", "subnet-1")
    assert result == {"MountTargetId": "mt-1"}
    conn.create_mount_target.assert_called_once_with(FileSystemId="fs-1", SubnetId="subnet-1")


def test_create_mount_target_with_optional_args(conn):
    conn.create_mount_target.return_value = {"MountTargetId": "mt-1"}
    boto3_efs.create_mount_target("fs-1", "subnet-1", ipaddress="10.0.0.1", securitygroups=["sg-1"])
    conn.create_mount_target.assert_called_once_with(
        FileSystemId="fs-1",
        SubnetId="subnet-1",
        IpAddress="10.0.0.1",
        SecurityGroups=["sg-1"],
    )


def test_create_mount_target_client_error(conn, client_error):
    conn.create_mount_target.side_effect = client_error("AccessDenied", "CreateMountTarget")
    assert boto3_efs.create_mount_target("fs-1", "subnet-1") is False


def test_create_tags(conn):
    assert boto3_efs.create_tags("fs-1", {"k": "v", "k2": "v2"}) is True
    _, kwargs = conn.create_tags.call_args
    assert kwargs["FileSystemId"] == "fs-1"
    assert {"Key": "k", "Value": "v"} in kwargs["Tags"]
    assert {"Key": "k2", "Value": "v2"} in kwargs["Tags"]


def test_create_tags_client_error(conn, client_error):
    conn.create_tags.side_effect = client_error("AccessDenied", "CreateTags")
    assert boto3_efs.create_tags("fs-1", {"k": "v"}) is False


def test_delete_file_system(conn):
    assert boto3_efs.delete_file_system("fs-1") is True
    conn.delete_file_system.assert_called_once_with(FileSystemId="fs-1")


def test_delete_file_system_client_error(conn, client_error):
    conn.delete_file_system.side_effect = client_error("FileSystemInUse", "DeleteFileSystem")
    assert boto3_efs.delete_file_system("fs-1") is False


def test_delete_mount_target(conn):
    assert boto3_efs.delete_mount_target("mt-1") is True
    conn.delete_mount_target.assert_called_once_with(MountTargetId="mt-1")


def test_delete_mount_target_client_error(conn, client_error):
    conn.delete_mount_target.side_effect = client_error("AccessDenied", "DeleteMountTarget")
    assert boto3_efs.delete_mount_target("mt-1") is False


def test_delete_tags(conn):
    assert boto3_efs.delete_tags("fs-1", ["k1", "k2"]) is True
    conn.delete_tags.assert_called_once_with(FileSystemId="fs-1", Tags=["k1", "k2"])


def test_delete_tags_client_error(conn, client_error):
    conn.delete_tags.side_effect = client_error("AccessDenied", "DeleteTags")
    assert boto3_efs.delete_tags("fs-1", ["k"]) is False


def test_get_file_systems_no_args_paginates(conn):
    conn.describe_file_systems.side_effect = [
        {"FileSystems": [{"FileSystemId": "fs-1"}], "NextMarker": "m1"},
        {"FileSystems": [{"FileSystemId": "fs-2"}]},
    ]
    result = boto3_efs.get_file_systems()
    assert [fs["FileSystemId"] for fs in result] == ["fs-1", "fs-2"]
    assert conn.describe_file_systems.call_count == 2


def test_get_file_systems_by_id(conn):
    conn.describe_file_systems.return_value = {"FileSystems": [{"FileSystemId": "fs-1"}]}
    result = boto3_efs.get_file_systems(filesystemid="fs-1")
    assert result == [{"FileSystemId": "fs-1"}]
    conn.describe_file_systems.assert_called_once_with(FileSystemId="fs-1")


def test_get_file_systems_by_creation_token(conn):
    conn.describe_file_systems.return_value = {"FileSystems": [{"FileSystemId": "fs-1"}]}
    boto3_efs.get_file_systems(creation_token="tok")
    conn.describe_file_systems.assert_called_once_with(CreationToken="tok")


def test_get_file_systems_by_id_and_token(conn):
    conn.describe_file_systems.return_value = {"FileSystems": []}
    boto3_efs.get_file_systems(filesystemid="fs-1", creation_token="tok")
    conn.describe_file_systems.assert_called_once_with(FileSystemId="fs-1", CreationToken="tok")


def test_get_file_systems_client_error(conn, client_error):
    conn.describe_file_systems.side_effect = client_error("AccessDenied", "DescribeFileSystems")
    assert not boto3_efs.get_file_systems()


def test_get_mount_targets_by_filesystem_paginates(conn):
    conn.describe_mount_targets.side_effect = [
        {"MountTargets": [{"MountTargetId": "mt-1"}], "NextMarker": "m"},
        {"MountTargets": [{"MountTargetId": "mt-2"}]},
    ]
    result = boto3_efs.get_mount_targets(filesystemid="fs-1")
    assert [m["MountTargetId"] for m in result] == ["mt-1", "mt-2"]
    assert conn.describe_mount_targets.call_count == 2


def test_get_mount_targets_by_id(conn):
    conn.describe_mount_targets.return_value = {"MountTargets": [{"MountTargetId": "mt-1"}]}
    result = boto3_efs.get_mount_targets(mounttargetid="mt-1")
    assert result == [{"MountTargetId": "mt-1"}]
    conn.describe_mount_targets.assert_called_once_with(MountTargetId="mt-1")


def test_get_mount_targets_neither_arg(conn):
    assert boto3_efs.get_mount_targets() is None
    conn.describe_mount_targets.assert_not_called()


def test_get_mount_targets_client_error(conn, client_error):
    conn.describe_mount_targets.side_effect = client_error("AccessDenied", "DescribeMountTargets")
    assert not boto3_efs.get_mount_targets(filesystemid="fs-1")


def test_get_tags_paginates(conn):
    conn.describe_tags.side_effect = [
        {"Tags": [{"Key": "k1", "Value": "v1"}], "NextMarker": "m"},
        {"Tags": [{"Key": "k2", "Value": "v2"}]},
    ]
    result = boto3_efs.get_tags("fs-1")
    assert [t["Key"] for t in result] == ["k1", "k2"]
    assert conn.describe_tags.call_count == 2


def test_get_tags_client_error(conn, client_error):
    conn.describe_tags.side_effect = client_error("AccessDenied", "DescribeTags")
    assert not boto3_efs.get_tags("fs-1")


def test_set_security_groups(conn):
    assert boto3_efs.set_security_groups("mt-1", ["sg-1", "sg-2"]) is True
    conn.modify_mount_target_security_groups.assert_called_once_with(
        MountTargetId="mt-1", SecurityGroups=["sg-1", "sg-2"]
    )


def test_set_security_groups_client_error(conn, client_error):
    conn.modify_mount_target_security_groups.side_effect = client_error(
        "AccessDenied", "ModifyMountTargetSecurityGroups"
    )
    assert boto3_efs.set_security_groups("mt-1", ["sg-1"]) is False
