"""
Unit tests for the ``boto3_cognitoidentity`` execution module.
"""

from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from saltext.boto3.modules import boto3_cognitoidentity

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
        boto3_cognitoidentity: {
            "__opts__": {},
            "__context__": {},
            "__salt__": {},
        }
    }


@pytest.fixture
def conn(make_conn):
    with make_conn(boto3_cognitoidentity) as client:
        yield client


def test_find_identity_pool_ids_passthrough():
    assert boto3_cognitoidentity._find_identity_pool_ids("ignored", "explicit-id", MagicMock()) == [
        "explicit-id"
    ]


def test_find_identity_pool_ids_paged():
    client = MagicMock()
    with patch.object(
        boto3_cognitoidentity.boto3mod,
        "paged_call",
        return_value=[
            {
                "IdentityPools": [
                    {"IdentityPoolName": "other", "IdentityPoolId": "idx"},
                    {"IdentityPoolName": "match", "IdentityPoolId": "id1"},
                ]
            },
            {"IdentityPools": [{"IdentityPoolName": "match", "IdentityPoolId": "id2"}]},
        ],
    ):
        ids = boto3_cognitoidentity._find_identity_pool_ids("match", None, client)
    assert ids == ["id1", "id2"]


def test_describe_identity_pools(conn):
    conn.describe_identity_pool.side_effect = [
        {"IdentityPoolId": "id1", "IdentityPoolName": "p", "ResponseMetadata": {}}
    ]
    with patch.object(boto3_cognitoidentity, "_find_identity_pool_ids", return_value=["id1"]):
        result = boto3_cognitoidentity.describe_identity_pools("p")
    assert result["identity_pools"][0]["IdentityPoolId"] == "id1"
    assert "ResponseMetadata" not in result["identity_pools"][0]


def test_describe_identity_pools_empty(conn):
    with patch.object(boto3_cognitoidentity, "_find_identity_pool_ids", return_value=[]):
        assert boto3_cognitoidentity.describe_identity_pools("p") == {"identity_pools": None}


def test_describe_identity_pools_client_error(conn, client_error):
    with patch.object(
        boto3_cognitoidentity.boto3mod,
        "paged_call",
        side_effect=client_error("Denied", "ListIdentityPools"),
    ):
        result = boto3_cognitoidentity.describe_identity_pools("p")
    assert "error" in result


def test_create_identity_pool(conn):
    conn.create_identity_pool.return_value = {
        "IdentityPoolId": "id1",
        "IdentityPoolName": "p",
        "ResponseMetadata": {},
    }
    result = boto3_cognitoidentity.create_identity_pool("p", DeveloperProviderName="dev")
    kwargs = conn.create_identity_pool.call_args.kwargs
    assert kwargs["IdentityPoolName"] == "p"
    assert kwargs["SupportedLoginProviders"] == {}
    assert kwargs["OpenIdConnectProviderARNs"] == []
    assert kwargs["DeveloperProviderName"] == "dev"
    assert result["created"] is True
    assert "ResponseMetadata" not in result["identity_pool"]


def test_create_identity_pool_no_developer(conn):
    conn.create_identity_pool.return_value = {"IdentityPoolId": "id1"}
    boto3_cognitoidentity.create_identity_pool("p")
    assert "DeveloperProviderName" not in conn.create_identity_pool.call_args.kwargs


def test_create_identity_pool_client_error(conn, client_error):
    conn.create_identity_pool.side_effect = client_error("Denied", "CreateIdentityPool")
    assert boto3_cognitoidentity.create_identity_pool("p")["created"] is False


def test_delete_identity_pools(conn):
    with patch.object(
        boto3_cognitoidentity, "_find_identity_pool_ids", return_value=["id1", "id2"]
    ):
        result = boto3_cognitoidentity.delete_identity_pools("p")
    assert result == {"deleted": True, "count": 2}
    assert conn.delete_identity_pool.call_count == 2


def test_delete_identity_pools_nothing_to_delete(conn):
    with patch.object(boto3_cognitoidentity, "_find_identity_pool_ids", return_value=[]):
        assert boto3_cognitoidentity.delete_identity_pools("p") == {"deleted": False, "count": 0}


def test_delete_identity_pools_client_error(conn, client_error):
    with patch.object(boto3_cognitoidentity, "_find_identity_pool_ids", return_value=["id1"]):
        conn.delete_identity_pool.side_effect = client_error("Denied", "DeleteIdentityPool")
        result = boto3_cognitoidentity.delete_identity_pools("p")
    assert result["deleted"] is False
    assert "error" in result


def test_get_identity_pool_roles(conn):
    conn.get_identity_pool_roles.return_value = {
        "IdentityPoolId": "id1",
        "Roles": {},
        "ResponseMetadata": {},
    }
    with patch.object(boto3_cognitoidentity, "_find_identity_pool_ids", return_value=["id1"]):
        result = boto3_cognitoidentity.get_identity_pool_roles("p")
    assert result["identity_pool_roles"][0]["IdentityPoolId"] == "id1"
    assert "ResponseMetadata" not in result["identity_pool_roles"][0]


def test_get_identity_pool_roles_empty(conn):
    with patch.object(boto3_cognitoidentity, "_find_identity_pool_ids", return_value=[]):
        assert boto3_cognitoidentity.get_identity_pool_roles("p") == {"identity_pool_roles": None}


def test_get_identity_pool_roles_client_error(conn, client_error):
    with patch.object(boto3_cognitoidentity, "_find_identity_pool_ids", return_value=["id1"]):
        conn.get_identity_pool_roles.side_effect = client_error("Denied", "GetIdentityPoolRoles")
        result = boto3_cognitoidentity.get_identity_pool_roles("p")
    assert "error" in result


def test_get_role_arn_passthrough(configure_loader_modules):
    with patch.dict(boto3_cognitoidentity.__salt__, {"boto3_iam.describe_role": MagicMock()}):
        assert (
            boto3_cognitoidentity._get_role_arn("arn:aws:iam::1:role/x") == "arn:aws:iam::1:role/x"
        )


def test_get_role_arn_resolves(configure_loader_modules):
    describe_role = MagicMock(return_value={"arn": "arn:aws:iam::1:role/y"})
    with patch.dict(boto3_cognitoidentity.__salt__, {"boto3_iam.describe_role": describe_role}):
        assert boto3_cognitoidentity._get_role_arn("y") == "arn:aws:iam::1:role/y"


def test_get_role_arn_not_found(configure_loader_modules):
    with patch.dict(
        boto3_cognitoidentity.__salt__,
        {"boto3_iam.describe_role": MagicMock(return_value=None)},
    ):
        assert boto3_cognitoidentity._get_role_arn("y") is None


def test_set_identity_pool_roles_both(configure_loader_modules, conn):
    describe_role = MagicMock(
        side_effect=[{"arn": "arn:aws:iam::1:role/auth"}, {"arn": "arn:aws:iam::1:role/unauth"}]
    )
    with patch.dict(boto3_cognitoidentity.__salt__, {"boto3_iam.describe_role": describe_role}):
        result = boto3_cognitoidentity.set_identity_pool_roles(
            "id1", AuthenticatedRole="auth", UnauthenticatedRole="unauth"
        )
    assert result["set"] is True
    assert result["roles"] == {
        "authenticated": "arn:aws:iam::1:role/auth",
        "unauthenticated": "arn:aws:iam::1:role/unauth",
    }
    conn.set_identity_pool_roles.assert_called_once()


def test_set_identity_pool_roles_invalid_auth(configure_loader_modules, conn):
    with patch.dict(
        boto3_cognitoidentity.__salt__,
        {"boto3_iam.describe_role": MagicMock(return_value=None)},
    ):
        result = boto3_cognitoidentity.set_identity_pool_roles("id1", AuthenticatedRole="missing")
    assert result["set"] is False
    assert "AuthenticatedRole" in result["error"]


def test_set_identity_pool_roles_invalid_unauth(configure_loader_modules, conn):
    with patch.dict(
        boto3_cognitoidentity.__salt__,
        {"boto3_iam.describe_role": MagicMock(return_value=None)},
    ):
        result = boto3_cognitoidentity.set_identity_pool_roles("id1", UnauthenticatedRole="missing")
    assert result["set"] is False
    assert "UnauthenticatedRole" in result["error"]


def test_set_identity_pool_roles_clear(conn):
    result = boto3_cognitoidentity.set_identity_pool_roles("id1")
    assert result == {"set": True, "roles": {}}
    conn.set_identity_pool_roles.assert_called_once_with(IdentityPoolId="id1", Roles={})


def test_set_identity_pool_roles_client_error(conn, client_error):
    conn.set_identity_pool_roles.side_effect = client_error("Denied", "SetIdentityPoolRoles")
    assert boto3_cognitoidentity.set_identity_pool_roles("id1")["set"] is False


def test_update_identity_pool_no_match(conn):
    with patch.object(
        boto3_cognitoidentity,
        "describe_identity_pools",
        return_value={"identity_pools": None},
    ):
        result = boto3_cognitoidentity.update_identity_pool("id1")
    assert result == {"updated": False, "error": "No matching pool"}


def test_update_identity_pool_describe_error(conn):
    with patch.object(
        boto3_cognitoidentity,
        "describe_identity_pools",
        return_value={"error": {"message": "fail"}},
    ):
        result = boto3_cognitoidentity.update_identity_pool("id1")
    assert result["updated"] is False


def test_update_identity_pool(conn):
    existing = {
        "IdentityPoolId": "id1",
        "IdentityPoolName": "old",
        "AllowUnauthenticatedIdentities": False,
        "SupportedLoginProviders": {},
        "OpenIdConnectProviderARNs": [],
    }
    conn.update_identity_pool.return_value = {
        "IdentityPoolId": "id1",
        "IdentityPoolName": "new",
        "ResponseMetadata": {},
    }
    with patch.object(
        boto3_cognitoidentity,
        "describe_identity_pools",
        return_value={"identity_pools": [existing]},
    ):
        result = boto3_cognitoidentity.update_identity_pool(
            "id1",
            IdentityPoolName="new",
            AllowUnauthenticatedIdentities=True,
            SupportedLoginProviders={"graph.facebook.com": "123"},
            OpenIdConnectProviderARNs=["arn:x"],
            DeveloperProviderName="dev",
        )
    kwargs = conn.update_identity_pool.call_args.kwargs
    assert kwargs["IdentityPoolName"] == "new"
    assert kwargs["AllowUnauthenticatedIdentities"] is True
    assert kwargs["SupportedLoginProviders"] == {"graph.facebook.com": "123"}
    assert kwargs["OpenIdConnectProviderARNs"] == ["arn:x"]
    assert kwargs["DeveloperProviderName"] == "dev"
    assert result["updated"] is True
    assert "ResponseMetadata" not in result["identity_pool"]


def test_update_identity_pool_preserves_developer_name(conn):
    existing = {
        "IdentityPoolId": "id1",
        "IdentityPoolName": "old",
        "AllowUnauthenticatedIdentities": False,
        "DeveloperProviderName": "existing_dev",
    }
    conn.update_identity_pool.return_value = {"IdentityPoolId": "id1"}
    with patch.object(
        boto3_cognitoidentity,
        "describe_identity_pools",
        return_value={"identity_pools": [existing]},
    ):
        boto3_cognitoidentity.update_identity_pool("id1", DeveloperProviderName="new_dev")
    assert "DeveloperProviderName" not in conn.update_identity_pool.call_args.kwargs


def test_update_identity_pool_client_error(conn, client_error):
    existing = {
        "IdentityPoolId": "id1",
        "IdentityPoolName": "old",
        "AllowUnauthenticatedIdentities": False,
    }
    conn.update_identity_pool.side_effect = client_error("Denied", "UpdateIdentityPool")
    with patch.object(
        boto3_cognitoidentity,
        "describe_identity_pools",
        return_value={"identity_pools": [existing]},
    ):
        result = boto3_cognitoidentity.update_identity_pool("id1", IdentityPoolName="new")
    assert result["updated"] is False
