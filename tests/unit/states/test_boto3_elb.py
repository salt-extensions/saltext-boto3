"""
Unit tests for the ``boto3_elb`` state module.
"""

import pytest

from saltext.boto3.states import boto3_elb as elb_state

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
        elb_state: {
            "__opts__": {"test": False},
            "__salt__": {},
        }
    }


def test_virtual(mock_salt):
    with mock_salt(elb_state, {"boto3_elb.exists": True}):
        assert elb_state.__virtual__() == "boto3_elb"


def test_virtual_no_exec_module(mock_salt):
    with mock_salt(elb_state, {}):
        result = elb_state.__virtual__()
    assert result[0] is False
    assert "boto3_elb" in result[1]
