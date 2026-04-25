"""
Unit tests for the ``boto3_iam_role`` state module.
"""

from unittest.mock import MagicMock

import pytest

from saltext.boto3.states import boto3_iam_role as iam_role_state

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
        iam_role_state: {
            "__opts__": {"test": False},
            "__salt__": {},
        }
    }


def test_virtual(mock_salt):
    with mock_salt(iam_role_state, {"boto3_iam.role_exists": True}):
        assert iam_role_state.__virtual__() == "boto3_iam_role"


def test_virtual_no_exec_module(mock_salt):
    with mock_salt(iam_role_state, {}):
        result = iam_role_state.__virtual__()
    assert result[0] is False


def test_present_failure_branches(mock_salt):
    """
    Walks through several failure branches in ``present`` by chaining
    ``side_effect`` lists across the mocked exec-module calls.
    """
    name = "myrole"

    desc_role = {
        "create_date": "2015-02-11T19:47:14Z",
        "role_id": "HIUHBIUBIBNKJNBKJ",
        "assume_role_policy_document": {
            "Version": "2008-10-17",
            "Statement": [
                {
                    "Action": "sts:AssumeRole",
                    "Principal": {"Service": "ec2.amazonaws.com"},
                    "Effect": "Allow",
                }
            ],
        },
        "role_name": "myfakerole",
        "path": "/",
        "arn": "arn:aws:iam::12345:role/myfakerole",
    }
    desc_role2 = dict(desc_role)
    desc_role2["assume_role_policy_document"] = {
        "Version": "2008-10-17",
        "Statement": [
            {
                "Action": "sts:AssumeRole",
                "Principal": {"Service": ["ec2.amazonaws.com", "datapipeline.amazonaws.com"]},
                "Effect": "Allow",
            }
        ],
    }
    build_policy = {
        "Version": "2008-10-17",
        "Statement": [
            {
                "Action": "sts:AssumeRole",
                "Effect": "Allow",
                "Principal": {"Service": "ec2.amazonaws.com"},
            }
        ],
    }
    salt_map = {
        "boto3_iam.describe_role": MagicMock(
            side_effect=[False, desc_role, desc_role, desc_role2, desc_role]
        ),
        "boto3_iam.create_role": MagicMock(return_value=False),
        "boto3_iam.build_policy": MagicMock(return_value=build_policy),
        "boto3_iam.update_assume_role_policy": MagicMock(return_value=False),
        "boto3_iam.instance_profile_exists": MagicMock(side_effect=[False, True, True, True]),
        "boto3_iam.list_attached_role_policies": MagicMock(return_value=[]),
        "boto3_iam.create_instance_profile": MagicMock(return_value=False),
        "boto3_iam.profile_associated": MagicMock(side_effect=[False, True, True, True]),
        "boto3_iam.associate_profile_to_role": MagicMock(return_value=False),
        "boto3_iam.list_role_policies": MagicMock(return_value=[]),
    }
    base_ret = {"name": name, "result": False, "changes": {}, "comment": ""}
    with mock_salt(iam_role_state, salt_map):
        ret = base_ret.copy()
        ret["comment"] = f" Failed to create {name} IAM role."
        assert iam_role_state.present(name) == ret

        ret = base_ret.copy()
        ret["comment"] = " myrole role present. Failed to create myrole instance profile."
        assert iam_role_state.present(name) == ret

        ret = base_ret.copy()
        ret["comment"] = (
            " myrole role present.  Failed to associate myrole"
            " instance profile with myrole role."
        )
        assert iam_role_state.present(name) == ret

        ret = base_ret.copy()
        ret["comment"] = " myrole role present. Failed to update assume role policy."
        assert iam_role_state.present(name) == ret

        ret = base_ret.copy()
        ret["comment"] = " myrole role present.    "
        ret["result"] = True
        assert iam_role_state.present(name) == ret


def test_absent_failure_branches(mock_salt):
    name = "myrole"
    base_ret = {"name": name, "result": False, "changes": {}, "comment": ""}
    side_effect_list = [
        ["mypolicy"],
        ["mypolicy"],
        False,
        True,
        False,
        False,
        True,
        False,
        False,
        False,
        True,
    ]
    chained = MagicMock(side_effect=side_effect_list)
    salt_map = {
        "boto3_iam.list_role_policies": chained,
        "boto3_iam.delete_role_policy": MagicMock(return_value=False),
        "boto3_iam.profile_associated": chained,
        "boto3_iam.disassociate_profile_from_role": MagicMock(return_value=False),
        "boto3_iam.instance_profile_exists": chained,
        "boto3_iam.list_attached_role_policies": MagicMock(return_value=[]),
        "boto3_iam.delete_instance_profile": MagicMock(return_value=False),
        "boto3_iam.role_exists": chained,
        "boto3_iam.delete_role": MagicMock(return_value=False),
    }
    with mock_salt(iam_role_state, salt_map):
        ret = base_ret.copy()
        ret["comment"] = " Failed to add policy mypolicy to role myrole"
        ret["changes"] = {
            "new": {"policies": ["mypolicy"]},
            "old": {"policies": ["mypolicy"]},
        }
        assert iam_role_state.absent(name) == ret

        ret = base_ret.copy()
        ret["comment"] = (
            " No policies in role myrole."
            " No attached policies in role myrole. Failed to disassociate "
            "myrole instance profile from myrole role."
        )
        assert iam_role_state.absent(name) == ret

        ret = base_ret.copy()
        ret["comment"] = (
            " No policies in role myrole."
            " No attached policies in role myrole. "
            " Failed to delete myrole instance profile."
        )
        assert iam_role_state.absent(name) == ret

        ret = base_ret.copy()
        ret["comment"] = (
            " No policies in role myrole."
            " No attached policies in role myrole.  myrole instance profile "
            "does not exist. Failed to delete myrole iam role."
        )
        assert iam_role_state.absent(name) == ret
