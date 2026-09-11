"""Unit tests for the ``boto3_secretsmanager`` execution module."""

import pytest

from saltext.boto3.modules import boto3_secretsmanager

try:
    import boto3  # pylint: disable=unused-import
    import botocore.exceptions  # pylint: disable=unused-import

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
        boto3_secretsmanager: {
            "__opts__": {},
            "__context__": {},
            "__salt__": {},
        }
    }


@pytest.fixture
def conn(make_conn):
    with make_conn(boto3_secretsmanager) as client:
        yield client


def test_get(conn):
    conn.get_secret_value.return_value = {
        "ARN": "arn:secret",
        "Name": "dso/dev/rds",
        "VersionId": "version-1",
        "VersionStages": ["AWSCURRENT"],
        "SecretString": '{"password":"value"}',
    }
    result = boto3_secretsmanager.get("dso/dev/rds")
    assert result["exists"] is True
    assert result["secret_string"] == '{"password":"value"}'
    conn.get_secret_value.assert_called_once_with(SecretId="dso/dev/rds")


def test_get_missing(conn, client_error):
    conn.get_secret_value.side_effect = client_error("ResourceNotFoundException", "GetSecretValue")
    assert boto3_secretsmanager.get("missing") == {"exists": False}


def test_exists(conn):
    conn.describe_secret.return_value = {"ARN": "arn:secret"}
    assert boto3_secretsmanager.exists("dso/dev/rds") == {"exists": True}
    conn.describe_secret.assert_called_once_with(SecretId="dso/dev/rds")


def test_create(conn):
    conn.create_secret.return_value = {"ARN": "arn:secret", "Name": "dso/dev/rds"}
    result = boto3_secretsmanager.create(
        "dso/dev/rds",
        '{"password":"value"}',
        description="DSO database credentials",
        tags={"Environment": "dev"},
    )
    assert result["created"] is True
    conn.create_secret.assert_called_once_with(
        Name="dso/dev/rds",
        SecretString='{"password":"value"}',
        Description="DSO database credentials",
        Tags=[{"Key": "Environment", "Value": "dev"}],
    )


def test_put(conn):
    conn.put_secret_value.return_value = {"VersionId": "version-2"}
    result = boto3_secretsmanager.put("dso/dev/rds", '{"password":"new"}')
    assert result["updated"] is True
    conn.put_secret_value.assert_called_once_with(
        SecretId="dso/dev/rds", SecretString='{"password":"new"}'
    )


def test_get_error(conn, client_error):
    conn.get_secret_value.side_effect = client_error("AccessDeniedException", "GetSecretValue")
    assert "error" in boto3_secretsmanager.get("dso/dev/rds")
