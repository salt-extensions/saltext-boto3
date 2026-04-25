"""
Unit tests for the ``boto3_cloudfront`` execution module.
"""

from unittest.mock import MagicMock

import pytest

from saltext.boto3.modules import boto3_cloudfront

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
        boto3_cloudfront: {
            "__opts__": {},
            "__context__": {},
            "__salt__": {},
        }
    }


@pytest.fixture
def conn(make_conn):
    with make_conn(boto3_cloudfront) as client:
        yield client


def _make_paginator(pages):
    paginator = MagicMock()
    paginator.paginate.return_value = iter(pages)
    return paginator


@pytest.fixture
def single_distribution(conn):
    conn.get_paginator.return_value = _make_paginator(
        [{"DistributionList": {"Items": [{"Id": "d1", "ARN": "arn:1"}]}}]
    )
    return conn


@pytest.fixture
def tagged_mydist(conn):
    conn.list_tags_for_resource.return_value = {
        "Tags": {"Items": [{"Key": "Name", "Value": "mydist"}, {"Key": "x", "Value": "y"}]}
    }
    return conn


@pytest.fixture
def named_only(conn):
    conn.list_tags_for_resource.return_value = {
        "Tags": {"Items": [{"Key": "Name", "Value": "mydist"}]}
    }
    return conn


@pytest.fixture
def distribution_d1(conn):
    conn.get_distribution.return_value = {
        "Distribution": {"Id": "d1", "ARN": "arn:1", "DistributionConfig": {"c": 1}},
        "ETag": "etag1",
    }
    return conn


@pytest.fixture
def update_setup(conn):
    """Pre-bake the typical lookup chain used by ``update_distribution``."""

    def _setup(current_config, current_tags):
        conn.get_paginator.return_value = _make_paginator(
            [{"DistributionList": {"Items": [{"Id": "d1", "ARN": "arn:1"}]}}]
        )
        conn.list_tags_for_resource.return_value = {
            "Tags": {
                "Items": [{"Key": "Name", "Value": "mydist"}]
                + [{"Key": k, "Value": v} for k, v in current_tags.items()]
            }
        }
        conn.get_distribution.return_value = {
            "Distribution": {
                "Id": "d1",
                "ARN": "arn:1",
                "DistributionConfig": current_config,
            },
            "ETag": "etag1",
        }
        return conn

    return _setup


@pytest.mark.usefixtures("single_distribution", "tagged_mydist", "distribution_d1")
def test_get_distribution():
    result = boto3_cloudfront.get_distribution("mydist")
    assert result["result"]["etag"] == "etag1"
    assert result["result"]["tags"] == {"x": "y"}
    assert result["result"]["distribution"]["Id"] == "d1"


def test_get_distribution_not_found(conn):
    conn.get_paginator.return_value = _make_paginator([{"DistributionList": {"Items": []}}])
    assert boto3_cloudfront.get_distribution("mydist") == {"result": None}


def test_get_distribution_handles_empty_list(conn):
    conn.get_paginator.return_value = _make_paginator([{"DistributionList": {}}])
    assert boto3_cloudfront.get_distribution("mydist") == {"result": None}


@pytest.mark.usefixtures("single_distribution")
def test_get_distribution_skips_untagged(conn):
    conn.list_tags_for_resource.return_value = {"Tags": {"Items": []}}
    assert boto3_cloudfront.get_distribution("mydist") == {"result": None}


def test_get_distribution_filters_by_name(conn):
    conn.get_paginator.return_value = _make_paginator(
        [
            {
                "DistributionList": {
                    "Items": [
                        {"Id": "d1", "ARN": "arn:1"},
                        {"Id": "d2", "ARN": "arn:2"},
                    ]
                }
            }
        ]
    )
    conn.list_tags_for_resource.side_effect = [
        {"Tags": {"Items": [{"Key": "Name", "Value": "other"}]}},
        {"Tags": {"Items": [{"Key": "Name", "Value": "mydist"}]}},
    ]
    conn.get_distribution.return_value = {
        "Distribution": {"Id": "d2", "ARN": "arn:2", "DistributionConfig": {}},
        "ETag": "etag2",
    }
    result = boto3_cloudfront.get_distribution("mydist")
    assert result["result"]["distribution"]["Id"] == "d2"


def test_get_distribution_duplicate_names_error(conn):
    conn.get_paginator.return_value = _make_paginator(
        [
            {
                "DistributionList": {
                    "Items": [
                        {"Id": "d1", "ARN": "arn:1"},
                        {"Id": "d2", "ARN": "arn:2"},
                    ]
                }
            }
        ]
    )
    conn.list_tags_for_resource.return_value = {
        "Tags": {"Items": [{"Key": "Name", "Value": "mydist"}]}
    }
    conn.get_distribution.side_effect = [
        {
            "Distribution": {"Id": "d1", "ARN": "arn:1", "DistributionConfig": {}},
            "ETag": "e1",
        },
        {
            "Distribution": {"Id": "d2", "ARN": "arn:2", "DistributionConfig": {}},
            "ETag": "e2",
        },
    ]
    result = boto3_cloudfront.get_distribution("mydist")
    assert "error" in result
    assert "More than one" in result["error"]


def test_get_distribution_client_error(conn, client_error):
    conn.get_paginator.side_effect = client_error("Denied", "ListDistributions")
    assert "error" in boto3_cloudfront.get_distribution("mydist")


@pytest.mark.usefixtures("single_distribution", "named_only")
def test_export_distributions(conn):
    conn.get_distribution.return_value = {
        "Distribution": {
            "Id": "d1",
            "ARN": "arn:1",
            "DistributionConfig": {"Comment": "c"},
        },
        "ETag": "e",
    }
    result = boto3_cloudfront.export_distributions()
    assert "Manage CloudFront distribution mydist" in result
    assert "boto3_cloudfront.present" in result


def test_export_distributions_handles_client_error(conn, client_error):
    conn.get_paginator.side_effect = client_error("Denied", "ListDistributions")
    assert isinstance(boto3_cloudfront.export_distributions(), str)


def test_create_distribution_tags_name_mismatch(conn):
    result = boto3_cloudfront.create_distribution("mydist", {"c": 1}, tags={"Name": "other"})
    assert "error" in result
    conn.create_distribution_with_tags.assert_not_called()


def test_create_distribution(conn):
    result = boto3_cloudfront.create_distribution("mydist", {"c": 1}, tags={"x": "y"})
    conn.create_distribution_with_tags.assert_called_once()
    kwargs = conn.create_distribution_with_tags.call_args.kwargs
    assert kwargs["DistributionConfigWithTags"]["DistributionConfig"] == {"c": 1}
    items = kwargs["DistributionConfigWithTags"]["Tags"]["Items"]
    assert {i["Key"] for i in items} == {"x", "Name"}
    assert result == {"result": True}


def test_create_distribution_with_no_tags(conn):
    boto3_cloudfront.create_distribution("mydist", {"c": 1})
    items = conn.create_distribution_with_tags.call_args.kwargs["DistributionConfigWithTags"][
        "Tags"
    ]["Items"]
    assert items == [{"Key": "Name", "Value": "mydist"}]


def test_create_distribution_allows_matching_name_tag(conn):
    result = boto3_cloudfront.create_distribution("mydist", {"c": 1}, tags={"Name": "mydist"})
    assert result == {"result": True}


def test_create_distribution_client_error(conn, client_error):
    conn.create_distribution_with_tags.side_effect = client_error(
        "Denied", "CreateDistributionWithTags"
    )
    assert "error" in boto3_cloudfront.create_distribution("mydist", {"c": 1})


def test_update_distribution_config_changes(conn, update_setup):
    update_setup({"Comment": "old"}, {})
    result = boto3_cloudfront.update_distribution("mydist", {"Comment": "new"})
    conn.update_distribution.assert_called_once_with(
        DistributionConfig={"Comment": "new"}, Id="d1", IfMatch="etag1"
    )
    conn.tag_resource.assert_not_called()
    conn.untag_resource.assert_not_called()
    assert result == {"result": True}


def test_update_distribution_no_config_changes(conn, update_setup):
    update_setup({"Comment": "same"}, {})
    boto3_cloudfront.update_distribution("mydist", {"Comment": "same"})
    conn.update_distribution.assert_not_called()


def test_update_distribution_tags_added(conn, update_setup):
    update_setup({"Comment": "c"}, {"old_tag": "v"})
    boto3_cloudfront.update_distribution("mydist", {"Comment": "c"}, tags={"new_tag": "v2"})
    conn.tag_resource.assert_called_once()
    add_items = conn.tag_resource.call_args.kwargs["Tags"]["Items"]
    assert add_items == [{"Key": "new_tag", "Value": "v2"}]
    conn.untag_resource.assert_called_once()
    assert conn.untag_resource.call_args.kwargs["TagKeys"]["Items"] == ["old_tag"]


def test_update_distribution_propagates_get_error(conn, client_error):
    conn.get_paginator.side_effect = client_error("Denied", "ListDistributions")
    assert "error" in boto3_cloudfront.update_distribution("mydist", {"Comment": "x"})


def test_update_distribution_client_error_on_update(conn, client_error, update_setup):
    update_setup({"Comment": "old"}, {})
    conn.update_distribution.side_effect = client_error("Denied", "UpdateDistribution")
    assert "error" in boto3_cloudfront.update_distribution("mydist", {"Comment": "new"})
