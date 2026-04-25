import os
from contextlib import contextmanager
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
import salt.config

try:
    from botocore.exceptions import ClientError

    HAS_BOTO3 = True
except ImportError:  # pragma: no cover
    HAS_BOTO3 = False


# ---------------------------------------------------------------------------
# Shared helpers for boto3-backed unit tests.
#
# These fixtures are the building blocks for both the execution-module and
# state-module test templates. See:
#   - tests/unit/modules/test_boto3_cloudtrail.py  (execution template)
#   - tests/unit/states/test_boto3_cloudtrail.py   (state template)
# ---------------------------------------------------------------------------


@pytest.fixture
def client_error():
    """
    Factory for ``botocore.exceptions.ClientError`` instances.

    Use in execution-module tests to drive error branches::

        conn.create_trail.side_effect = client_error("AccessDenied", "CreateTrail")
    """

    def _factory(code, operation="GenericOp", message="boom"):
        if not HAS_BOTO3:  # pragma: no cover
            pytest.skip("botocore is required for these tests.")
        return ClientError({"Error": {"Code": code, "Message": message}}, operation)

    return _factory


@pytest.fixture
def make_conn():
    """
    Factory that patches ``<module>.boto3mod.get_connection`` and yields a
    ``MagicMock`` client. Per-test ``conn`` fixtures wrap this helper::

        @pytest.fixture
        def conn(make_conn):
            with make_conn(boto3_cloudtrail) as client:
                yield client
    """

    @contextmanager
    def _patch(module):
        client = MagicMock()
        with patch.object(module.boto3mod, "get_connection", return_value=client):
            yield client

    return _patch


@pytest.fixture
def mock_salt():
    """
    Context manager for swapping a state module's ``__salt__`` and toggling
    ``__opts__["test"]``. Used by state-module tests::

        with mock_salt(
            state,
            {
                "boto3_cloudtrail.exists": {"exists": False},
                "boto3_cloudtrail.create": {"created": True},
            },
            test=False,
        ) as salt_mocks:
            ret = state.present("trail", "trail", "bucket")
            salt_mocks["boto3_cloudtrail.create"].assert_called_once()

    Values that are already ``MagicMock`` instances are used as-is (so tests
    can pre-configure ``side_effect`` for error branches). Plain values are
    wrapped as ``MagicMock(return_value=value)``.
    """

    @contextmanager
    def _ctx(state_module, mocks=None, test=False):
        salt_mocks = {}
        for key, value in (mocks or {}).items():
            if isinstance(value, MagicMock):
                salt_mocks[key] = value
            else:
                salt_mocks[key] = MagicMock(return_value=value)
        salt_patch = patch.dict(state_module.__salt__, salt_mocks, clear=True)
        opts_patch = patch.dict(state_module.__opts__, {"test": test})
        with salt_patch, opts_patch:
            yield salt_mocks

    return _ctx


@pytest.fixture
def minion_opts(tmp_path):  # pragma: no cover
    """
    Default minion configuration with relative temporary paths to not
    require root permissions.
    """
    root_dir = tmp_path / "minion"
    opts = salt.config.DEFAULT_MINION_OPTS.copy()
    opts["__role"] = "minion"
    opts["root_dir"] = str(root_dir)
    for name in ("cachedir", "pki_dir", "sock_dir", "conf_dir"):
        dirpath = root_dir / name
        dirpath.mkdir(parents=True)
        opts[name] = str(dirpath)
    opts["log_file"] = "logs/minion.log"
    opts["conf_file"] = os.path.join(opts["conf_dir"], "minion")
    return opts


@pytest.fixture
def master_opts(tmp_path):  # pragma: no cover
    """
    Default master configuration with relative temporary paths to not
    require root permissions.
    """
    root_dir = tmp_path / "master"
    opts = salt.config.master_config(None)
    opts["__role"] = "master"
    opts["root_dir"] = str(root_dir)
    for name in ("cachedir", "pki_dir", "sock_dir", "conf_dir"):
        dirpath = root_dir / name
        dirpath.mkdir(parents=True)
        opts[name] = str(dirpath)
    opts["log_file"] = "logs/master.log"
    opts["conf_file"] = os.path.join(opts["conf_dir"], "master")
    return opts


@pytest.fixture
def syndic_opts(tmp_path):  # pragma: no cover
    """
    Default master configuration with relative temporary paths to not
    require root permissions.
    """
    root_dir = tmp_path / "syndic"
    opts = salt.config.DEFAULT_MINION_OPTS.copy()
    opts["syndic_master"] = "127.0.0.1"
    opts["__role"] = "minion"
    opts["root_dir"] = str(root_dir)
    for name in ("cachedir", "pki_dir", "sock_dir", "conf_dir"):
        dirpath = root_dir / name
        dirpath.mkdir(parents=True)
        opts[name] = str(dirpath)
    opts["log_file"] = "logs/syndic.log"
    opts["conf_file"] = os.path.join(opts["conf_dir"], "syndic")
    return opts
