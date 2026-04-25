"""
Unit tests for the ``boto3_route53`` execution module.
"""

from unittest.mock import patch

import pytest
from salt.exceptions import SaltInvocationError

from saltext.boto3.modules import boto3_route53

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
        boto3_route53: {
            "__opts__": {},
            "__context__": {},
            "__salt__": {},
        }
    }


@pytest.fixture
def conn(make_conn):
    with make_conn(boto3_route53) as client:
        yield client


@pytest.fixture(autouse=True)
def _no_wait():
    """Short-circuit ``_wait_for_sync`` so create/delete operations return immediately."""
    with patch.object(boto3_route53, "_wait_for_sync", return_value=True):
        yield


def test_aws_encode_ascii():
    assert boto3_route53._aws_encode("example.org.") == "example.org."


def test_aws_encode_idna():
    result = boto3_route53._aws_encode("ドメイン.テスト.")
    assert result.startswith("xn--")


def test_get_hosted_zone(conn):
    conn.get_hosted_zone.return_value = {
        "HostedZone": {"Id": "/hostedzone/Z1", "Name": "x."},
        "ResponseMetadata": {},
    }
    result = boto3_route53.get_hosted_zone("Z1")
    assert result == [{"HostedZone": {"Id": "/hostedzone/Z1", "Name": "x."}}]


def test_list_hosted_zones(conn):
    conn.list_hosted_zones.return_value = {
        "HostedZones": [{"Id": "/hostedzone/Z1", "Name": "x."}],
        "ResponseMetadata": {},
    }
    assert boto3_route53.list_hosted_zones() == [{"Id": "/hostedzone/Z1", "Name": "x."}]


def test_get_hosted_zones_by_domain(conn):
    conn.list_hosted_zones.return_value = {
        "HostedZones": [
            {"Id": "/hostedzone/Z1", "Name": "example.org."},
            {"Id": "/hostedzone/Z2", "Name": "other.org."},
        ],
        "ResponseMetadata": {},
    }
    conn.get_hosted_zone.return_value = {
        "HostedZone": {"Id": "/hostedzone/Z1", "Name": "example.org."},
        "ResponseMetadata": {},
    }
    result = boto3_route53.get_hosted_zones_by_domain("example.org.")
    assert result == [{"HostedZone": {"Id": "/hostedzone/Z1", "Name": "example.org."}}]
    conn.get_hosted_zone.assert_called_once_with(Id="/hostedzone/Z1")


def test_find_hosted_zone_requires_exactly_one():
    with pytest.raises(SaltInvocationError):
        boto3_route53.find_hosted_zone()
    with pytest.raises(SaltInvocationError):
        boto3_route53.find_hosted_zone(Id="Z1", Name="x.")


def test_find_hosted_zone_bad_private_flag():
    with pytest.raises(SaltInvocationError):
        boto3_route53.find_hosted_zone(Id="Z1", PrivateZone="yes")


def test_find_hosted_zone_by_id(conn):
    conn.get_hosted_zone.return_value = {
        "HostedZone": {"Id": "Z1", "Config": {"PrivateZone": False}},
        "ResponseMetadata": {},
    }
    result = boto3_route53.find_hosted_zone(Id="Z1")
    assert result == [{"HostedZone": {"Id": "Z1", "Config": {"PrivateZone": False}}}]


def test_find_hosted_zone_filters_by_private(conn):
    conn.list_hosted_zones.return_value = {
        "HostedZones": [
            {"Id": "Z1", "Name": "x."},
            {"Id": "Z2", "Name": "x."},
        ],
        "ResponseMetadata": {},
    }
    conn.get_hosted_zone.side_effect = [
        {"HostedZone": {"Id": "Z1", "Config": {"PrivateZone": False}}, "ResponseMetadata": {}},
        {"HostedZone": {"Id": "Z2", "Config": {"PrivateZone": True}}, "ResponseMetadata": {}},
    ]
    result = boto3_route53.find_hosted_zone(Name="x.", PrivateZone=True)
    assert len(result) == 1
    assert result[0]["HostedZone"]["Id"] == "Z2"


def test_find_hosted_zone_multiple_matches_returns_empty(conn):
    conn.list_hosted_zones.return_value = {
        "HostedZones": [
            {"Id": "Z1", "Name": "x."},
            {"Id": "Z2", "Name": "x."},
        ],
        "ResponseMetadata": {},
    }
    conn.get_hosted_zone.side_effect = [
        {"HostedZone": {"Id": "Z1", "Config": {"PrivateZone": False}}, "ResponseMetadata": {}},
        {"HostedZone": {"Id": "Z2", "Config": {"PrivateZone": False}}, "ResponseMetadata": {}},
    ]
    assert not boto3_route53.find_hosted_zone(Name="x.")


def test_create_hosted_zone_requires_fqdn():
    with pytest.raises(SaltInvocationError):
        boto3_route53.create_hosted_zone("example.org")


def test_create_hosted_zone_already_exists(conn):
    conn.list_hosted_zones.return_value = {
        "HostedZones": [{"Id": "Z1", "Name": "example.org."}],
        "ResponseMetadata": {},
    }
    conn.get_hosted_zone.return_value = {
        "HostedZone": {"Id": "Z1", "Name": "example.org.", "Config": {"PrivateZone": False}},
        "ResponseMetadata": {},
    }
    assert boto3_route53.create_hosted_zone("example.org.") is None


def test_create_hosted_zone_public(conn):
    conn.list_hosted_zones.return_value = {"HostedZones": [], "ResponseMetadata": {}}
    conn.create_hosted_zone.return_value = {
        "HostedZone": {"Id": "Z1"},
        "ChangeInfo": {"Id": "C1", "Status": "PENDING"},
        "ResponseMetadata": {},
    }
    result = boto3_route53.create_hosted_zone("example.org.", CallerReference="ref1")
    assert len(result) == 1
    assert result[0]["HostedZone"]["Id"] == "Z1"
    kwargs = conn.create_hosted_zone.call_args.kwargs
    assert kwargs["Name"] == "example.org."
    assert kwargs["HostedZoneConfig"] == {"Comment": "", "PrivateZone": False}
    assert kwargs["CallerReference"] == "ref1"


def test_create_hosted_zone_private_requires_vpc(conn):
    conn.list_hosted_zones.return_value = {"HostedZones": [], "ResponseMetadata": {}}
    with pytest.raises(SaltInvocationError):
        boto3_route53.create_hosted_zone("example.org.", PrivateZone=True)


def test_delete_hosted_zone(conn):
    conn.delete_hosted_zone.return_value = {"ChangeInfo": {"Id": "C1"}}
    assert boto3_route53.delete_hosted_zone("Z1") is True
    conn.delete_hosted_zone.assert_called_once_with(Id="Z1")


def test_delete_hosted_zone_client_error(conn, client_error):
    conn.delete_hosted_zone.side_effect = client_error("NotFound", "DeleteHostedZone")
    assert boto3_route53.delete_hosted_zone("Z1") is False


def test_delete_hosted_zone_by_domain_missing(conn):
    conn.list_hosted_zones.return_value = {"HostedZones": [], "ResponseMetadata": {}}
    assert boto3_route53.delete_hosted_zone_by_domain("nope.org.") is False


def test_delete_hosted_zone_by_domain(conn):
    conn.list_hosted_zones.return_value = {
        "HostedZones": [{"Id": "/hostedzone/Z1", "Name": "example.org."}],
        "ResponseMetadata": {},
    }
    conn.get_hosted_zone.return_value = {
        "HostedZone": {"Id": "/hostedzone/Z1", "Config": {"PrivateZone": False}},
        "ResponseMetadata": {},
    }
    conn.delete_hosted_zone.return_value = {"ChangeInfo": {"Id": "C1"}}
    assert boto3_route53.delete_hosted_zone_by_domain("example.org.") is True


def test_update_hosted_zone_comment_requires_one():
    with pytest.raises(SaltInvocationError):
        boto3_route53.update_hosted_zone_comment()


def test_update_hosted_zone_comment_by_id(conn):
    conn.update_hosted_zone_comment.return_value = {
        "HostedZone": {"Id": "Z1", "Config": {"Comment": "new"}},
        "ResponseMetadata": {},
    }
    result = boto3_route53.update_hosted_zone_comment(Id="Z1", Comment="new")
    assert result[0]["HostedZone"]["Config"]["Comment"] == "new"
    conn.update_hosted_zone_comment.assert_called_once_with(Id="Z1", Comment="new")


def test_get_resource_records(conn):
    conn.list_resource_record_sets.return_value = {
        "ResourceRecordSets": [
            {"Name": "x.example.org.", "Type": "A", "TTL": 60},
            {"Name": "x.example.org.", "Type": "AAAA", "TTL": 60},
        ],
        "ResponseMetadata": {},
    }
    result = boto3_route53.get_resource_records("Z1", StartRecordName="x.example.org.")
    assert len(result) == 2
    assert all(r["Name"] == "x.example.org." for r in result)
