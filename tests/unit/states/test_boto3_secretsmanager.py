"""Unit tests for the ``boto3_secretsmanager`` state module."""

from saltext.boto3.states import boto3_secretsmanager


def test_present_creates_secret():
    boto3_secretsmanager.__opts__ = {"test": False}
    boto3_secretsmanager.__salt__ = {
        "boto3_secretsmanager.exists": lambda *args, **kwargs: {"exists": False},
        "boto3_secretsmanager.create": lambda *args, **kwargs: {
            "created": True,
            "arn": "arn:secret",
        },
    }
    result = boto3_secretsmanager.present("dso/dev/rds", secret_data={"password": "value"})
    assert result["result"] is True
    assert result["changes"] == {"created": True, "arn": "arn:secret"}


def test_present_existing_secret_is_noop():
    boto3_secretsmanager.__opts__ = {"test": False}
    create_called = False

    def create(*args, **kwargs):
        nonlocal create_called
        create_called = True
        return {"created": True}

    boto3_secretsmanager.__salt__ = {
        "boto3_secretsmanager.exists": lambda *args, **kwargs: {"exists": True},
        "boto3_secretsmanager.create": create,
    }
    result = boto3_secretsmanager.present("dso/dev/rds", secret_string="value")
    assert result["result"] is True
    assert create_called is False
    assert not result["changes"]


def test_present_updates_only_when_requested():
    boto3_secretsmanager.__opts__ = {"test": False}
    put_calls = []
    boto3_secretsmanager.__salt__ = {
        "boto3_secretsmanager.exists": lambda *args, **kwargs: {"exists": True},
        "boto3_secretsmanager.put": lambda *args, **kwargs: put_calls.append((args, kwargs))
        or {"updated": True, "version_id": "version-2"},
    }
    result = boto3_secretsmanager.present("dso/dev/rds", secret_string="value", update=True)
    assert result["result"] is True
    assert result["changes"] == {"updated": True, "version_id": "version-2"}
    assert put_calls


def test_rotation_present_is_idempotent():
    boto3_secretsmanager.__opts__ = {"test": False}
    configure_called = False

    def configure(*args, **kwargs):
        nonlocal configure_called
        configure_called = True
        return {"configured": True}

    boto3_secretsmanager.__salt__ = {
        "boto3_secretsmanager.get_rotation": lambda *args, **kwargs: {
            "exists": True,
            "rotation_enabled": True,
            "rotation_lambda_arn": "arn:lambda:rotation",
            "rotation_rules": {"AutomaticallyAfterDays": 90},
            "owning_service": None,
        },
        "boto3_secretsmanager.configure_rotation": configure,
    }
    result = boto3_secretsmanager.rotation_present(
        "dso/dev/rds", "arn:lambda:rotation", automatically_after_days=90
    )
    assert result["result"] is True
    assert not result["changes"]
    assert configure_called is False


def test_rotation_present_configures_without_rotating_immediately():
    boto3_secretsmanager.__opts__ = {"test": False}
    calls = []
    boto3_secretsmanager.__salt__ = {
        "boto3_secretsmanager.get_rotation": lambda *args, **kwargs: {
            "exists": True,
            "rotation_enabled": False,
        },
        "boto3_secretsmanager.configure_rotation": lambda *args, **kwargs: calls.append(
            (args, kwargs)
        )
        or {"configured": True},
    }
    result = boto3_secretsmanager.rotation_present(
        "dso/dev/rds",
        "arn:lambda:rotation",
        schedule_expression="rate(90 days)",
    )
    assert result["result"] is True
    assert result["changes"]["rotation_enabled"] is True
    assert calls[0][1].get("rotate_immediately", False) is False


def test_rotation_present_test_mode_does_not_configure():
    boto3_secretsmanager.__opts__ = {"test": True}
    configure_called = False

    def configure(*args, **kwargs):
        nonlocal configure_called
        configure_called = True
        return {"configured": True}

    boto3_secretsmanager.__salt__ = {
        "boto3_secretsmanager.get_rotation": lambda *args, **kwargs: {
            "exists": True,
            "rotation_enabled": False,
        },
        "boto3_secretsmanager.configure_rotation": configure,
    }
    result = boto3_secretsmanager.rotation_present(
        "dso/dev/rds",
        "arn:lambda:rotation",
        automatically_after_days=90,
    )
    assert result["result"] is None
    assert configure_called is False


def test_rotation_present_missing_secret_fails():
    boto3_secretsmanager.__opts__ = {"test": False}
    boto3_secretsmanager.__salt__ = {
        "boto3_secretsmanager.get_rotation": lambda *args, **kwargs: {"exists": False}
    }
    result = boto3_secretsmanager.rotation_present(
        "dso/dev/rds",
        "arn:lambda:rotation",
        automatically_after_days=90,
    )
    assert result["result"] is False
    assert "does not exist" in result["comment"]


def test_rotation_present_rejects_service_managed_secret():
    configure_called = False

    def configure(*args, **kwargs):
        nonlocal configure_called
        configure_called = True
        return {"configured": True}

    boto3_secretsmanager.__opts__ = {"test": False}
    boto3_secretsmanager.__salt__ = {
        "boto3_secretsmanager.get_rotation": lambda *args, **kwargs: {
            "exists": True,
            "rotation_enabled": True,
            "owning_service": "rds",
        },
        "boto3_secretsmanager.configure_rotation": configure,
    }
    result = boto3_secretsmanager.rotation_present(
        "rds/master", "arn:lambda:rotation", automatically_after_days=90
    )
    assert result["result"] is False
    assert "managed by rds" in result["comment"]
    assert configure_called is False


def test_managed_rotation_present_configures_schedule_without_lambda():
    boto3_secretsmanager.__opts__ = {"test": False}
    calls = []
    boto3_secretsmanager.__salt__ = {
        "boto3_secretsmanager.get_rotation": lambda *args, **kwargs: {
            "exists": True,
            "rotation_enabled": True,
            "owning_service": "rds",
            "rotation_rules": {"AutomaticallyAfterDays": 7},
        },
        "boto3_secretsmanager.configure_managed_rotation": lambda *args, **kwargs: calls.append(
            (args, kwargs)
        )
        or {"configured": True},
    }
    result = boto3_secretsmanager.managed_rotation_present(
        "rds/master", automatically_after_days=90
    )
    assert result["result"] is True
    assert result["changes"] == {"rotation_rules": {"AutomaticallyAfterDays": 90}}
    assert calls[0][0] == ("rds/master",)
    assert calls[0][1]["automatically_after_days"] == 90


def test_managed_rotation_present_is_idempotent():
    boto3_secretsmanager.__opts__ = {"test": False}
    configure_called = False

    def configure(*args, **kwargs):
        nonlocal configure_called
        configure_called = True

    boto3_secretsmanager.__salt__ = {
        "boto3_secretsmanager.get_rotation": lambda *args, **kwargs: {
            "exists": True,
            "rotation_enabled": True,
            "owning_service": "rds",
            "rotation_rules": {"AutomaticallyAfterDays": 90},
        },
        "boto3_secretsmanager.configure_managed_rotation": configure,
    }
    result = boto3_secretsmanager.managed_rotation_present(
        "rds/master", automatically_after_days=90
    )
    assert result["result"] is True
    assert not result["changes"]
    assert configure_called is False


def test_managed_rotation_present_rejects_lambda_managed_secret():
    boto3_secretsmanager.__opts__ = {"test": False}
    boto3_secretsmanager.__salt__ = {
        "boto3_secretsmanager.get_rotation": lambda *args, **kwargs: {
            "exists": True,
            "rotation_enabled": True,
            "owning_service": None,
        }
    }
    result = boto3_secretsmanager.managed_rotation_present("app/db", automatically_after_days=90)
    assert result["result"] is False
    assert "not managed by an AWS service" in result["comment"]


def test_present_rejects_two_value_inputs():
    boto3_secretsmanager.__opts__ = {"test": False}
    boto3_secretsmanager.__salt__ = {}
    try:
        boto3_secretsmanager.present("dso/dev/rds", secret_string="value", secret_data={})
    except ValueError as exc:
        assert "mutually exclusive" in str(exc)
    else:
        raise AssertionError("present should reject both secret value arguments")


def test_absent_secret_is_noop():
    boto3_secretsmanager.__opts__ = {"test": False}
    delete_called = False

    def delete(*args, **kwargs):
        nonlocal delete_called
        delete_called = True

    boto3_secretsmanager.__salt__ = {
        "boto3_secretsmanager.exists": lambda *args, **kwargs: {"exists": False},
        "boto3_secretsmanager.delete": delete,
    }
    result = boto3_secretsmanager.absent("dso/dev/rds")
    assert result == {
        "name": "dso/dev/rds",
        "changes": {},
        "result": True,
        "comment": "Secret dso/dev/rds does not exist.",
    }
    assert delete_called is False


def test_absent_deletes_existing_secret():
    boto3_secretsmanager.__opts__ = {"test": False}
    delete_calls = []
    boto3_secretsmanager.__salt__ = {
        "boto3_secretsmanager.exists": lambda *args, **kwargs: {"exists": True},
        "boto3_secretsmanager.delete": lambda *args, **kwargs: delete_calls.append((args, kwargs))
        or {"deleted": True},
    }
    result = boto3_secretsmanager.absent(
        "dso/dev/rds",
        region="us-east-1",
        key="access-key",
        keyid="secret-key",
        profile="aws-profile",
    )
    assert result["result"] is True
    assert result["changes"] == {"deleted": True}
    assert result["comment"] == "Secret dso/dev/rds deleted."
    assert delete_calls == [
        (
            ("dso/dev/rds",),
            {
                "region": "us-east-1",
                "key": "access-key",
                "keyid": "secret-key",
                "profile": "aws-profile",
            },
        )
    ]


def test_absent_test_mode_does_not_delete():
    boto3_secretsmanager.__opts__ = {"test": True}
    delete_called = False

    def delete(*args, **kwargs):
        nonlocal delete_called
        delete_called = True

    boto3_secretsmanager.__salt__ = {
        "boto3_secretsmanager.exists": lambda *args, **kwargs: {"exists": True},
        "boto3_secretsmanager.delete": delete,
    }
    result = boto3_secretsmanager.absent("dso/dev/rds")
    assert result["result"] is None
    assert not result["changes"]
    assert result["comment"] == "Secret dso/dev/rds would be deleted."
    assert delete_called is False


def test_absent_returns_exists_error():
    boto3_secretsmanager.__opts__ = {"test": False}
    delete_called = False

    def delete(*args, **kwargs):
        nonlocal delete_called
        delete_called = True

    boto3_secretsmanager.__salt__ = {
        "boto3_secretsmanager.exists": lambda *args, **kwargs: {"error": "access denied"},
        "boto3_secretsmanager.delete": delete,
    }
    result = boto3_secretsmanager.absent("dso/dev/rds")
    assert result["result"] is False
    assert result["comment"] == "access denied"
    assert delete_called is False
