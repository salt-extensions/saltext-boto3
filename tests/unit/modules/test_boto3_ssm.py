"""
Unit tests for the ``boto3_ssm`` execution module.
"""

import pytest

from saltext.boto3.modules import boto3_ssm

try:
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
        boto3_ssm: {
            "__opts__": {},
            "__context__": {},
            "__salt__": {},
        }
    }


@pytest.fixture
def conn(make_conn):
    with make_conn(boto3_ssm) as client:
        # The module references conn.exceptions.* — create stand-in classes.
        client.exceptions.ParameterNotFound = type("ParameterNotFound", (Exception,), {})
        client.exceptions.ParameterAlreadyExists = type("ParameterAlreadyExists", (Exception,), {})
        yield client


def test_get_parameter(conn):
    conn.get_parameter.return_value = {"Parameter": {"Value": "v"}}
    assert boto3_ssm.get_parameter("n") == "v"
    conn.get_parameter.assert_called_once_with(Name="n", WithDecryption=False)


def test_get_parameter_decrypt(conn):
    conn.get_parameter.return_value = {"Parameter": {"Value": "v"}}
    boto3_ssm.get_parameter("n", withdecryption=True)
    conn.get_parameter.assert_called_once_with(Name="n", WithDecryption=True)


def test_get_parameter_json(conn):
    conn.get_parameter.return_value = {"Parameter": {"Value": '{"a": 1}'}}
    assert boto3_ssm.get_parameter("n", resp_json=True) == {"a": 1}


def test_get_parameter_not_found(conn):
    conn.get_parameter.side_effect = conn.exceptions.ParameterNotFound()
    assert boto3_ssm.get_parameter("n") is False


def test_put_parameter(conn):
    conn.put_parameter.return_value = {"Version": 3}
    assert boto3_ssm.put_parameter("n", "v") == 3
    conn.put_parameter.assert_called_once_with(Name="n", Value="v", Type="String", Overwrite=False)


def test_put_parameter_with_all_args(conn):
    conn.put_parameter.return_value = {"Version": 1}
    boto3_ssm.put_parameter(
        "n",
        "v",
        Description="d",
        Type="SecureString",
        KeyId="alias/aws/ssm",
        Overwrite=True,
        AllowedPattern=".*",
    )
    kw = conn.put_parameter.call_args.kwargs
    assert kw["Description"] == "d"
    assert kw["KeyId"] == "alias/aws/ssm"
    assert kw["AllowedPattern"] == ".*"
    assert kw["Overwrite"] is True


def test_put_parameter_bad_type(conn):
    with pytest.raises(AssertionError):
        boto3_ssm.put_parameter("n", "v", Type="Bogus")


def test_put_parameter_secure_without_key(conn):
    with pytest.raises(AssertionError):
        boto3_ssm.put_parameter("n", "v", Type="SecureString")


def test_put_parameter_already_exists(conn):
    conn.put_parameter.side_effect = conn.exceptions.ParameterAlreadyExists()
    assert boto3_ssm.put_parameter("n", "v") is False


def test_delete_parameter(conn):
    conn.delete_parameter.return_value = {"ResponseMetadata": {"HTTPStatusCode": 200}}
    assert boto3_ssm.delete_parameter("n") is True


def test_delete_parameter_failed(conn):
    conn.delete_parameter.return_value = {"ResponseMetadata": {"HTTPStatusCode": 500}}
    assert boto3_ssm.delete_parameter("n") is False


def test_delete_parameter_not_found(conn):
    conn.delete_parameter.side_effect = conn.exceptions.ParameterNotFound()
    assert boto3_ssm.delete_parameter("n") is False


class TestRunShellScriptDocument:
    """Tests for run_shell_script_document."""

    def test_with_tags(self, conn, mock_salt):
        conn.send_command.return_value = {"Command": {"CommandId": "cmd-1", "Status": "Pending"}}
        with mock_salt(
            boto3_ssm,
            {"cp.get_file_str": "#!/bin/bash\necho hi\n"},
        ):
            result = boto3_ssm.run_shell_script_document(
                document_name="AWS-RunShellScript",
                salt_script_source="salt://scripts/hi.sh",
                target_type="tags",
                target_tags={"eks:cluster-name": "my-cluster"},
            )
        assert "error" not in result
        assert result.get("CommandId") == "cmd-1"

    def test_with_instance_ids(self, conn, mock_salt):
        conn.send_command.return_value = {"Command": {"CommandId": "cmd-2", "Status": "Pending"}}
        with mock_salt(
            boto3_ssm,
            {"cp.get_file_str": "#!/bin/bash\necho hi\n"},
        ):
            result = boto3_ssm.run_shell_script_document(
                document_name="AWS-RunShellScript",
                salt_script_source="salt://scripts/hi.sh",
                target_type="instance_ids",
                instance_ids=["i-111", "i-222"],
            )
        assert "error" not in result
        call_kwargs = conn.send_command.call_args.kwargs
        assert call_kwargs.get("InstanceIds") == ["i-111", "i-222"]

    def test_template_rendering(self, conn, mock_salt):
        conn.send_command.return_value = {"Command": {"CommandId": "cmd-3", "Status": "Pending"}}
        with mock_salt(
            boto3_ssm,
            {"cp.get_file_str": "#!/bin/bash\necho {{ version }}\n"},
        ):
            result = boto3_ssm.run_shell_script_document(
                document_name="AWS-RunShellScript",
                salt_script_source="salt://scripts/ver.sh",
                target_type="tags",
                target_tags={"env": "dev"},
                template_context={"version": "3006"},
            )
        assert "error" not in result
        call_kwargs = conn.send_command.call_args.kwargs
        commands = call_kwargs["Parameters"]["commands"]
        assert any("echo 3006" in c for c in commands)

    def test_script_not_found(self, conn, mock_salt):
        with mock_salt(boto3_ssm, {"cp.get_file_str": None}):
            result = boto3_ssm.run_shell_script_document(
                document_name="AWS-RunShellScript",
                salt_script_source="salt://scripts/missing.sh",
                target_type="tags",
                target_tags={"env": "dev"},
            )
        assert result.get("success") is False

    def test_cloudwatch_config(self, conn, mock_salt):
        conn.send_command.return_value = {"Command": {"CommandId": "cmd-4", "Status": "Pending"}}
        with mock_salt(
            boto3_ssm,
            {"cp.get_file_str": "#!/bin/bash\necho hi\n"},
        ):
            result = boto3_ssm.run_shell_script_document(
                document_name="AWS-RunShellScript",
                salt_script_source="salt://scripts/hi.sh",
                target_type="tags",
                target_tags={"env": "dev"},
                enable_cloudwatch=True,
                cloudwatch_log_group_name="/ssm/output",
            )
        assert "error" not in result
        call_kwargs = conn.send_command.call_args.kwargs
        assert call_kwargs.get("OutputS3BucketName") == "/ssm/output"
