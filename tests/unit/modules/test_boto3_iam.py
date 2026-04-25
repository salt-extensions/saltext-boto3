"""
Unit tests for the ``boto3_iam`` execution module.
"""

import pytest

from saltext.boto3.modules import boto3_iam

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
        boto3_iam: {
            "__opts__": {"test": False},
            "__context__": {},
            "__salt__": {},
        }
    }


@pytest.fixture
def conn(make_conn):
    with make_conn(boto3_iam) as client:
        yield client


def test_instance_profile_exists_true(conn):
    conn.get_instance_profile.return_value = {"InstanceProfile": {"InstanceProfileName": "p"}}
    assert boto3_iam.instance_profile_exists("p") is True


def test_instance_profile_exists_false(conn, client_error):
    conn.get_instance_profile.side_effect = client_error("NoSuchEntity", "GetInstanceProfile")
    assert boto3_iam.instance_profile_exists("p") is False


def test_create_instance_profile(conn, client_error):
    conn.get_instance_profile.side_effect = client_error("NoSuchEntity", "GetInstanceProfile")
    conn.create_instance_profile.return_value = {"InstanceProfile": {"InstanceProfileName": "p"}}
    assert boto3_iam.create_instance_profile("p") is True


def test_delete_instance_profile(conn):
    conn.get_instance_profile.return_value = {"InstanceProfile": {"InstanceProfileName": "p"}}
    assert boto3_iam.delete_instance_profile("p") is True
    conn.delete_instance_profile.assert_called_once_with(InstanceProfileName="p")


def test_role_exists_true(conn):
    conn.get_role.return_value = {"Role": {"RoleName": "r"}}
    assert boto3_iam.role_exists("r") is True


def test_role_exists_false(conn, client_error):
    conn.get_role.side_effect = client_error("NoSuchEntity", "GetRole")
    assert boto3_iam.role_exists("r") is False


def test_delete_role(conn):
    conn.get_role.return_value = {"Role": {"RoleName": "r"}}
    assert boto3_iam.delete_role("r") is True
    conn.delete_role.assert_called_once_with(RoleName="r")
