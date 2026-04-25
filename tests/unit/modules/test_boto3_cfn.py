"""
Unit tests for the ``boto3_cfn`` execution module.
"""

import pytest

from saltext.boto3.modules import boto3_cfn

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
        boto3_cfn: {
            "__opts__": {},
            "__context__": {},
            "__salt__": {},
        }
    }


@pytest.fixture
def conn(make_conn):
    with make_conn(boto3_cfn) as client:
        yield client


def test_convert_parameters_tuples():
    assert boto3_cfn._convert_parameters([("a", "1"), ("b", "2", True)]) == [
        {"ParameterKey": "a", "ParameterValue": "1"},
        {"ParameterKey": "b", "ParameterValue": "2", "UsePreviousValue": True},
    ]


def test_convert_parameters_passthrough_dict():
    given = [{"ParameterKey": "a", "ParameterValue": "1"}]
    assert boto3_cfn._convert_parameters(given) == given


def test_convert_parameters_none():
    assert boto3_cfn._convert_parameters(None) is None
    assert boto3_cfn._convert_parameters([]) is None


def test_convert_tags_dict():
    assert boto3_cfn._convert_tags({"k": "v"}) == [{"Key": "k", "Value": "v"}]


def test_convert_tags_list_passthrough():
    given = [{"Key": "k", "Value": "v"}]
    assert boto3_cfn._convert_tags(given) is given


def test_convert_tags_none():
    assert boto3_cfn._convert_tags(None) is None
    assert boto3_cfn._convert_tags({}) is None


def test_exists_true(conn):
    conn.describe_stacks.return_value = {"Stacks": [{"StackId": "id"}]}
    assert boto3_cfn.exists("mystack") is True
    conn.describe_stacks.assert_called_once_with(StackName="mystack")


def test_exists_false_on_client_error(conn, client_error):
    conn.describe_stacks.side_effect = client_error("ValidationError", "DescribeStacks")
    assert boto3_cfn.exists("mystack") is False


def test_describe_returns_structured(conn):
    conn.describe_stacks.return_value = {
        "Stacks": [
            {
                "StackId": "sid",
                "Description": "desc",
                "StackStatus": "CREATE_COMPLETE",
                "StackStatusReason": "ok",
                "Tags": [{"Key": "t", "Value": "1"}],
                "Outputs": [{"OutputKey": "o1", "OutputValue": "v1"}],
                "Parameters": [{"ParameterKey": "p1", "ParameterValue": "pv1"}],
            }
        ]
    }
    result = boto3_cfn.describe("mystack")
    assert result == {
        "stack": {
            "stack_id": "sid",
            "description": "desc",
            "stack_status": "CREATE_COMPLETE",
            "stack_status_reason": "ok",
            "tags": [{"Key": "t", "Value": "1"}],
            "outputs": {"o1": "v1"},
            "parameters": {"p1": "pv1"},
        }
    }


def test_describe_handles_empty_stacks(conn):
    conn.describe_stacks.return_value = {"Stacks": []}
    assert boto3_cfn.describe("mystack") is True


def test_describe_returns_false_on_client_error(conn, client_error):
    conn.describe_stacks.side_effect = client_error("ValidationError", "DescribeStacks")
    assert boto3_cfn.describe("mystack") is False


def test_create_minimal(conn):
    conn.create_stack.return_value = {"StackId": "sid"}
    result = boto3_cfn.create("mystack", template_body="{}")
    conn.create_stack.assert_called_once_with(StackName="mystack", TemplateBody="{}")
    assert result == {"StackId": "sid"}


def test_create_all_options(conn):
    conn.create_stack.return_value = {"StackId": "sid"}
    boto3_cfn.create(
        "mystack",
        template_body=None,
        template_url="https://s3/t",
        parameters=[("a", "1")],
        notification_arns=["arn:aws:sns:..."],
        disable_rollback=True,
        timeout_in_minutes=5,
        capabilities=["CAPABILITY_IAM"],
        tags={"k": "v"},
        on_failure="ROLLBACK",
        stack_policy_body="{}",
        stack_policy_url="https://s3/p",
    )
    conn.create_stack.assert_called_once_with(
        StackName="mystack",
        TemplateURL="https://s3/t",
        Parameters=[{"ParameterKey": "a", "ParameterValue": "1"}],
        NotificationARNs=["arn:aws:sns:..."],
        DisableRollback=True,
        TimeoutInMinutes=5,
        Capabilities=["CAPABILITY_IAM"],
        Tags=[{"Key": "k", "Value": "v"}],
        OnFailure="ROLLBACK",
        StackPolicyBody="{}",
        StackPolicyURL="https://s3/p",
    )


def test_create_returns_false_on_error(conn, client_error):
    conn.create_stack.side_effect = client_error("ValidationError", "CreateStack")
    assert boto3_cfn.create("mystack", template_body="{}") is False


def test_update_stack_minimal(conn):
    conn.update_stack.return_value = {"StackId": "sid"}
    result = boto3_cfn.update_stack("mystack", template_body="{}")
    conn.update_stack.assert_called_once_with(StackName="mystack", TemplateBody="{}")
    assert result == {"StackId": "sid"}


def test_update_stack_all_options(conn):
    conn.update_stack.return_value = {"StackId": "sid"}
    boto3_cfn.update_stack(
        "mystack",
        template_url="https://s3/t",
        parameters=[("a", "1", True)],
        notification_arns=["arn:aws:sns:..."],
        capabilities=["CAPABILITY_IAM"],
        tags=[{"Key": "k", "Value": "v"}],
        use_previous_template=True,
        stack_policy_during_update_body="{}",
        stack_policy_during_update_url="https://s3/du",
        stack_policy_body="{}",
        stack_policy_url="https://s3/p",
    )
    conn.update_stack.assert_called_once_with(
        StackName="mystack",
        TemplateURL="https://s3/t",
        Parameters=[{"ParameterKey": "a", "ParameterValue": "1", "UsePreviousValue": True}],
        NotificationARNs=["arn:aws:sns:..."],
        Capabilities=["CAPABILITY_IAM"],
        Tags=[{"Key": "k", "Value": "v"}],
        UsePreviousTemplate=True,
        StackPolicyDuringUpdateBody="{}",
        StackPolicyDuringUpdateURL="https://s3/du",
        StackPolicyBody="{}",
        StackPolicyURL="https://s3/p",
    )


def test_update_stack_returns_str_on_error(conn, client_error):
    conn.update_stack.side_effect = client_error("ValidationError", "UpdateStack")
    result = boto3_cfn.update_stack("mystack", template_body="{}")
    assert isinstance(result, str)
    assert "ValidationError" in result


def test_delete_success(conn):
    conn.delete_stack.return_value = {"ResponseMetadata": {}}
    result = boto3_cfn.delete("mystack")
    conn.delete_stack.assert_called_once_with(StackName="mystack")
    assert result == {"ResponseMetadata": {}}


def test_delete_returns_str_on_error(conn, client_error):
    conn.delete_stack.side_effect = client_error("ValidationError", "DeleteStack")
    result = boto3_cfn.delete("mystack")
    assert isinstance(result, str)


def test_get_template_success(conn):
    conn.get_template.return_value = {"TemplateBody": "{}"}
    result = boto3_cfn.get_template("mystack")
    conn.get_template.assert_called_once_with(StackName="mystack")
    assert result == {"TemplateBody": "{}"}


def test_get_template_error(conn, client_error):
    conn.get_template.side_effect = client_error("ValidationError", "GetTemplate")
    result = boto3_cfn.get_template("mystack")
    assert isinstance(result, str)


def test_validate_template_body(conn):
    conn.validate_template.return_value = {"Parameters": []}
    result = boto3_cfn.validate_template(template_body="{}")
    conn.validate_template.assert_called_once_with(TemplateBody="{}")
    assert result == {"Parameters": []}


def test_validate_template_url(conn):
    conn.validate_template.return_value = {}
    boto3_cfn.validate_template(template_url="https://s3/t")
    conn.validate_template.assert_called_once_with(TemplateURL="https://s3/t")


def test_validate_template_error(conn, client_error):
    conn.validate_template.side_effect = client_error("ValidationError", "ValidateTemplate")
    result = boto3_cfn.validate_template(template_body="{}")
    assert isinstance(result, str)
