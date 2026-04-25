"""
Unit tests for the ``boto3_elasticsearch`` execution module.
"""

from unittest.mock import MagicMock

import pytest
from salt.exceptions import SaltInvocationError

from saltext.boto3.modules import boto3_elasticsearch

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
        boto3_elasticsearch: {
            "__opts__": {},
            "__context__": {},
            "__salt__": {},
        }
    }


@pytest.fixture
def conn(make_conn):
    with make_conn(boto3_elasticsearch) as client:
        yield client


def test_describe_domain_success(conn):
    conn.describe_elasticsearch_domain.return_value = {
        "DomainStatus": {"DomainName": "d1", "ARN": "arn:aws:es:..."},
    }
    result = boto3_elasticsearch.describe_elasticsearch_domain("d1")
    assert result["result"] is True
    assert result["response"]["DomainName"] == "d1"


def test_describe_domain_missing_status_key(conn):
    conn.describe_elasticsearch_domain.return_value = {}
    result = boto3_elasticsearch.describe_elasticsearch_domain("d1")
    assert result["result"] is False


def test_describe_domain_client_error(conn, client_error):
    conn.describe_elasticsearch_domain.side_effect = client_error("NotFound", "DescribeDomain")
    result = boto3_elasticsearch.describe_elasticsearch_domain("d1")
    assert result["result"] is False
    assert "error" in result


def test_describe_domain_config(conn):
    conn.describe_elasticsearch_domain_config.return_value = {
        "DomainConfig": {"ElasticsearchVersion": {"Options": "7.10"}}
    }
    result = boto3_elasticsearch.describe_elasticsearch_domain_config("d1")
    assert result["result"] is True
    assert "ElasticsearchVersion" in result["response"]


def test_describe_domains(conn):
    conn.describe_elasticsearch_domains.return_value = {
        "DomainStatusList": [{"DomainName": "a"}, {"DomainName": "b"}]
    }
    result = boto3_elasticsearch.describe_elasticsearch_domains(["a", "b"])
    assert result["result"] is True


def test_exists_true(conn):
    conn.describe_elasticsearch_domain.return_value = {"DomainStatus": {"DomainName": "d1"}}
    result = boto3_elasticsearch.exists("d1")
    assert result["result"] is True


def test_exists_not_found_no_error(conn, client_error):
    conn.describe_elasticsearch_domain.side_effect = client_error(
        "ResourceNotFoundException", "DescribeDomain"
    )
    result = boto3_elasticsearch.exists("d1")
    assert result == {"result": False}


def test_exists_other_error_surfaces(conn, client_error):
    conn.describe_elasticsearch_domain.side_effect = client_error("AuthFailure", "DescribeDomain")
    result = boto3_elasticsearch.exists("d1")
    assert result["result"] is False
    assert "error" in result


def test_delete_domain_success(conn):
    result = boto3_elasticsearch.delete_elasticsearch_domain("d1")
    assert result["result"] is True
    conn.delete_elasticsearch_domain.assert_called_once_with(DomainName="d1")


def test_delete_domain_blocking(conn):
    waiter = MagicMock()
    conn.get_waiter.return_value = waiter
    result = boto3_elasticsearch.delete_elasticsearch_domain("d1", blocking=True)
    assert result["result"] is True
    conn.get_waiter.assert_called_once_with("ESDomainDeleted")
    waiter.wait.assert_called_once_with(DomainName="d1")


def test_delete_domain_error(conn, client_error):
    conn.delete_elasticsearch_domain.side_effect = client_error("NotFound", "DeleteDomain")
    result = boto3_elasticsearch.delete_elasticsearch_domain("d1")
    assert result["result"] is False
    assert "error" in result


def test_list_domain_names(conn):
    conn.list_domain_names.return_value = {
        "DomainNames": [{"DomainName": "a"}, {"DomainName": "b"}]
    }
    result = boto3_elasticsearch.list_domain_names()
    assert result["result"] is True


def test_add_tags_requires_identifier():
    with pytest.raises(SaltInvocationError):
        boto3_elasticsearch.add_tags(tags={"a": "b"})


def test_add_tags_with_arn(conn):
    result = boto3_elasticsearch.add_tags(arn="arn:aws:es:...", tags={"k": "v"})
    assert result["result"] is True
    kwargs = conn.add_tags.call_args.kwargs
    assert kwargs["ARN"] == "arn:aws:es:..."
    assert kwargs["TagList"] == [{"Key": "k", "Value": "v"}]


def test_add_tags_lookup_by_domain(conn):
    conn.describe_elasticsearch_domain.return_value = {
        "DomainStatus": {"DomainName": "d1", "ARN": "arn:aws:es:..."}
    }
    result = boto3_elasticsearch.add_tags(domain_name="d1", tags={"k": "v"})
    assert result["result"] is True
    kwargs = conn.add_tags.call_args.kwargs
    assert kwargs["ARN"] == "arn:aws:es:..."


def test_add_tags_domain_missing(conn):
    conn.describe_elasticsearch_domain.return_value = {}
    result = boto3_elasticsearch.add_tags(domain_name="d1", tags={"k": "v"})
    assert result["result"] is False
    assert "does not exist" in result["error"]


def test_list_tags_requires_identifier():
    with pytest.raises(SaltInvocationError):
        boto3_elasticsearch.list_tags()


def test_list_tags_success(conn):
    conn.list_tags.return_value = {
        "TagList": [{"Key": "a", "Value": "1"}, {"Key": "b", "Value": "2"}]
    }
    result = boto3_elasticsearch.list_tags(arn="arn:aws:es:...")
    assert result["result"] is True
    assert result["response"] == {"a": "1", "b": "2"}


def test_remove_tags_requires_identifier():
    with pytest.raises(SaltInvocationError):
        boto3_elasticsearch.remove_tags(tag_keys=["a"])


def test_create_domain_client_error(conn, client_error):
    conn.create_elasticsearch_domain.side_effect = client_error("Bad", "CreateDomain")
    result = boto3_elasticsearch.create_elasticsearch_domain("d1")
    assert result["result"] is False
    assert "error" in result


def test_create_domain_success(conn):
    conn.create_elasticsearch_domain.return_value = {"DomainStatus": {"DomainName": "d1"}}
    result = boto3_elasticsearch.create_elasticsearch_domain("d1")
    assert result["result"] is True
    kwargs = conn.create_elasticsearch_domain.call_args.kwargs
    assert kwargs["DomainName"] == "d1"


def test_get_compatible_versions(conn):
    conn.get_compatible_elasticsearch_versions.return_value = {
        "CompatibleElasticsearchVersions": [{"SourceVersion": "7.9", "TargetVersions": ["7.10"]}]
    }
    result = boto3_elasticsearch.get_compatible_elasticsearch_versions("d1")
    assert result["result"] is True


def test_list_elasticsearch_versions(conn):
    paginator = MagicMock()
    paginator.paginate.return_value = [{"ElasticsearchVersions": ["7.10", "7.9"]}]
    conn.get_paginator.return_value = paginator
    result = boto3_elasticsearch.list_elasticsearch_versions()
    assert result["result"] is True
    assert result["response"] == ["7.10", "7.9"]


def test_get_upgrade_status(conn):
    conn.get_upgrade_status.return_value = {
        "UpgradeStep": "PRE_UPGRADE_CHECK",
        "StepStatus": "SUCCEEDED",
        "ResponseMetadata": {},
    }
    result = boto3_elasticsearch.get_upgrade_status("d1")
    assert result["result"] is True
    assert result["response"]["StepStatus"] == "SUCCEEDED"
