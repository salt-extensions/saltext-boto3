"""
Tests for saltext.boto3.utils.boto3mod
"""

from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from salt.exceptions import SaltInvocationError
from salt.utils.versions import Version

from saltext.boto3.utils import boto3mod

try:
    import boto3
    import botocore.exceptions

    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

REQUIRED_BOTO3_VERSION = "1.2.1"

pytestmark = [
    pytest.mark.skip_on_fips_enabled_platform,
    pytest.mark.skipif(HAS_BOTO3 is False, reason="The boto3 module must be installed."),
    pytest.mark.skipif(
        HAS_BOTO3 and Version(boto3.__version__) < Version(REQUIRED_BOTO3_VERSION),
        reason=(
            "The boto3 module must be greater or equal to version " f"{REQUIRED_BOTO3_VERSION}"
        ),
    ),
]

REGION = "us-east-1"
SERVICE = "test-service"
RESOURCE_NAME = "test-resource"
RESOURCE_ID = "test-resource-id"
ACCESS_KEY = "GKTADJGHEIQSXMKKRBJ08H"
SECRET_KEY = "askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs"


@pytest.fixture
def opts():
    return {}


@pytest.fixture
def context():
    return {}


@pytest.fixture
def conn_kwargs():
    return {
        "region": REGION,
        "key": ACCESS_KEY,
        "keyid": SECRET_KEY,
        "profile": {},
    }


@pytest.fixture
def mock_session():
    with patch("boto3.session.Session") as session:
        session.return_value.client.return_value = MagicMock()
        yield session


def test_cache_id_set_and_get_with_no_auth_params(opts, context):
    boto3mod.cache_id(SERVICE, RESOURCE_NAME, opts=opts, context=context, resource_id=RESOURCE_ID)
    assert boto3mod.cache_id(SERVICE, RESOURCE_NAME, opts=opts, context=context) == RESOURCE_ID


def test_cache_id_set_and_get_with_explicit_auth_params(opts, context, conn_kwargs):
    boto3mod.cache_id(
        SERVICE,
        RESOURCE_NAME,
        opts=opts,
        context=context,
        resource_id=RESOURCE_ID,
        **conn_kwargs,
    )
    assert (
        boto3mod.cache_id(SERVICE, RESOURCE_NAME, opts=opts, context=context, **conn_kwargs)
        == RESOURCE_ID
    )


def test_cache_id_with_different_region_returns_none(opts, context):
    boto3mod.cache_id(
        SERVICE,
        RESOURCE_NAME,
        opts=opts,
        context=context,
        resource_id=RESOURCE_ID,
        region="us-east-1",
    )
    assert (
        boto3mod.cache_id(SERVICE, RESOURCE_NAME, opts=opts, context=context, region="us-west-2")
        is None
    )


def test_cache_id_after_invalidation_returns_none(opts, context):
    boto3mod.cache_id(SERVICE, RESOURCE_NAME, opts=opts, context=context, resource_id=RESOURCE_ID)
    boto3mod.cache_id(
        SERVICE,
        RESOURCE_NAME,
        opts=opts,
        context=context,
        resource_id=RESOURCE_ID,
        invalidate=True,
    )
    assert boto3mod.cache_id(SERVICE, RESOURCE_NAME, opts=opts, context=context) is None


def test_cache_id_invalidate_by_value_clears_matching_entries(opts, context):
    boto3mod.cache_id(SERVICE, RESOURCE_NAME, opts=opts, context=context, resource_id=RESOURCE_ID)
    assert (
        boto3mod.cache_id(
            SERVICE,
            "different-name",
            opts=opts,
            context=context,
            resource_id=RESOURCE_ID,
            invalidate=True,
        )
        is True
    )
    assert boto3mod.cache_id(SERVICE, RESOURCE_NAME, opts=opts, context=context) is None


def test_get_connection_caches_in_context(opts, context, mock_session):
    conn1 = boto3mod.get_connection("ec2", opts=opts, context=context, region=REGION)
    conn2 = boto3mod.get_connection("ec2", opts=opts, context=context, region=REGION)
    assert conn1 is conn2
    # Session should only be constructed once thanks to context caching.
    assert mock_session.call_count == 1


def test_get_connection_uses_profile_dict(opts, context, mock_session):
    profile = {"key": ACCESS_KEY, "keyid": SECRET_KEY, "region": REGION}
    boto3mod.get_connection("ec2", opts=opts, context=context, profile=profile)
    mock_session.assert_called_once_with(
        aws_access_key_id=SECRET_KEY,
        aws_secret_access_key=ACCESS_KEY,
        region_name=REGION,
    )


def test_get_connection_resolves_profile_from_opts(context, mock_session):
    opts = {
        "myprof": {"key": ACCESS_KEY, "keyid": SECRET_KEY, "region": REGION},
    }
    boto3mod.get_connection("ec2", opts=opts, context=context, profile="myprof")
    mock_session.assert_called_once_with(
        aws_access_key_id=SECRET_KEY,
        aws_secret_access_key=ACCESS_KEY,
        region_name=REGION,
    )


def test_get_connection_falls_back_to_service_options(context, mock_session):
    opts = {
        "ec2.region": REGION,
        "ec2.key": ACCESS_KEY,
        "ec2.keyid": SECRET_KEY,
    }
    boto3mod.get_connection("ec2", opts=opts, context=context)
    mock_session.assert_called_once_with(
        aws_access_key_id=SECRET_KEY,
        aws_secret_access_key=ACCESS_KEY,
        region_name=REGION,
    )


def test_get_region_resolves_from_profile_dict(opts):
    region = boto3mod.get_region(
        "ec2", None, {"region": "eu-west-1", "key": "k", "keyid": "i"}, opts=opts
    )
    assert region == "eu-west-1"


def test_get_region_defaults_to_us_east_1(opts):
    assert boto3mod.get_region("ec2", None, None, opts=opts) == "us-east-1"


def test_exactly_one():
    assert boto3mod.exactly_one((True, False, False)) is True
    assert boto3mod.exactly_one((True, True, False)) is False
    assert boto3mod.exactly_one((False, False, False)) is False


def test_exactly_n():
    assert boto3mod.exactly_n((True, True, False), n=2) is True
    assert boto3mod.exactly_n((True, True, True), n=2) is False


def test_paged_call_yields_all_pages():
    pages = [
        {"Items": [1], "NextMarker": "a"},
        {"Items": [2], "NextMarker": "b"},
        {"Items": [3]},
    ]
    fn = MagicMock(side_effect=pages)
    result = list(boto3mod.paged_call(fn))
    assert result == pages
    # Marker arg should be passed on subsequent calls.
    assert fn.call_args_list[1].kwargs == {"Marker": "a"}
    assert fn.call_args_list[2].kwargs == {"Marker": "b"}


def test_json_objs_equal_ignores_ordering():
    assert boto3mod.json_objs_equal([1, 2, 3], [3, 2, 1]) is True
    assert boto3mod.json_objs_equal({"a": [1, 2]}, {"a": [2, 1]}) is True
    assert boto3mod.json_objs_equal({"a": 1}, {"a": 2}) is False


def test_get_error_returns_message():
    err = Exception("boom")
    assert boto3mod.get_error(err) == {"message": "boom"}


def test_assign_funcs_is_removed():
    """Ensure the legacy loader-injection helper is gone."""
    assert not hasattr(boto3mod, "assign_funcs")
    assert not hasattr(boto3mod, "get_connection_func")
    assert not hasattr(boto3mod, "cache_id_func")


def test_get_connection_raises_on_no_credentials(opts, context):
    with patch("boto3.session.Session") as session:
        session.side_effect = botocore.exceptions.NoCredentialsError()
        with pytest.raises(SaltInvocationError):
            boto3mod.get_connection("ec2", opts=opts, context=context, region=REGION)
