"""
Boto3 Common Utils
==================

Common helpers for boto3-based execution and state modules.

Execution/state modules call these helpers directly, passing their own
``__opts__`` and ``__context__``:

.. code-block:: python

    from saltext.boto3.utils import boto3mod


    def __virtual__():
        return "my_service"


    def describe():
        conn = boto3mod.get_connection(
            "ec2", opts=__opts__, context=__context__, profile="myprofile"
        )
        instance_id = boto3mod.cache_id(
            "ec2", "myinstance", opts=__opts__, context=__context__
        )

.. versionadded:: 1.0.0
"""

import hashlib
import logging

import salt.utils.stringutils
from salt.exceptions import SaltInvocationError

try:
    import boto3
    import boto3.session
    import botocore
    import botocore.exceptions

    logging.getLogger("boto3").setLevel(logging.CRITICAL)
    HAS_BOTO = True
except ImportError:
    HAS_BOTO = False


log = logging.getLogger(__name__)

__virtualname__ = "boto3"


def __virtual__():
    """
    Only load if boto3 is available. Minimum version is enforced via the
    project's ``pyproject.toml`` dependency declaration.
    """
    if HAS_BOTO:
        return __virtualname__
    return (False, "The boto3mod utility could not be loaded: boto3 is not available.")


def _option(value, opts):
    """
    Look up an option value in ``opts``.
    """
    if opts and value in opts:
        return opts[value]
    return None


def _get_profile(service, region, key, keyid, profile, opts):
    if profile:
        if isinstance(profile, str):
            _profile = _option(profile, opts) or {}
        elif isinstance(profile, dict):
            _profile = profile
        else:
            _profile = {}
        key = _profile.get("key", None)
        keyid = _profile.get("keyid", None)
        region = _profile.get("region", None)

    if not region:
        region = _option(service + ".region", opts)

    if not region:
        region = "us-east-1"
        log.info("Assuming default region %s", region)

    if not key:
        key = _option(service + ".key", opts)
    if not keyid:
        keyid = _option(service + ".keyid", opts)

    label = f"boto3_{service}:"
    if keyid:
        hash_string = region + keyid + (key or "")
        hash_string = salt.utils.stringutils.to_bytes(hash_string)
        cxkey = label + hashlib.md5(hash_string, usedforsecurity=False).hexdigest()
    else:
        cxkey = label + region

    return (cxkey, region, key, keyid)


def cache_id(
    service,
    name,
    *,
    opts,
    context,
    sub_resource=None,
    resource_id=None,
    invalidate=False,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Cache, invalidate, or retrieve an AWS resource id keyed by name.

    ``opts`` and ``context`` are required; pass ``__opts__`` and
    ``__context__`` from the calling module.

    .. code-block:: python

        boto3mod.cache_id(
            "ec2",
            "myinstance",
            opts=__opts__,
            context=__context__,
            resource_id="i-a1b2c3",
            profile="custom_profile",
        )
    """
    cxkey, _, _, _ = _get_profile(service, region, key, keyid, profile, opts)
    if sub_resource:
        cxkey = f"{cxkey}:{sub_resource}:{name}:id"
    else:
        cxkey = f"{cxkey}:{name}:id"

    if invalidate:
        if cxkey in context:
            del context[cxkey]
            return True
        if resource_id is not None and resource_id in context.values():
            stale = [k for k, v in context.items() if v == resource_id]
            for k in stale:
                del context[k]
            return True
        return False
    if resource_id:
        context[cxkey] = resource_id
        return True

    return context.get(cxkey)


def get_connection(
    service,
    *,
    opts,
    context,
    module=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Return a boto3 client for the given service, caching it in ``context``.

    ``opts`` and ``context`` are required; pass ``__opts__`` and
    ``__context__`` from the calling module.

    .. code-block:: python

        conn = boto3mod.get_connection(
            "ec2",
            opts=__opts__,
            context=__context__,
            profile="custom_profile",
        )
    """
    module = module or service

    cxkey, region, key, keyid = _get_profile(service, region, key, keyid, profile, opts)
    cxkey = cxkey + ":conn3"

    if cxkey in context:
        return context[cxkey]

    try:
        session = boto3.session.Session(
            aws_access_key_id=keyid,
            aws_secret_access_key=key,
            region_name=region,
        )
        if session is None:
            raise SaltInvocationError(f'Region "{region}" is not valid.')
        conn = session.client(module)
        if conn is None:
            raise SaltInvocationError(f'Region "{region}" is not valid.')
    except botocore.exceptions.NoCredentialsError as exc:
        raise SaltInvocationError(
            "No authentication credentials found when "
            f"attempting to make boto {service} connection to "
            f'region "{region}".'
        ) from exc
    context[cxkey] = conn
    return conn


def get_region(service, region, profile, *, opts):
    """
    Return the resolved region for a service based on the supplied
    region/profile and the calling module's ``opts``.
    """
    _, region, _, _ = _get_profile(service, region, None, None, profile, opts)
    return region


def get_error(e):
    """
    Best-effort extraction of an error message from a boto/botocore exception.
    """
    message = ""
    if e.args:
        message = e.args[0]
    return {"message": message}


def exactly_n(l, n=1):
    """
    Return True when exactly ``n`` items in ``l`` are truthy.
    """
    i = iter(l)
    return all(any(i) for _ in range(n)) and not any(i)


def exactly_one(l):
    return exactly_n(l)


def paged_call(function, *args, **kwargs):
    """
    Yield successive pages from a boto3 API call that may paginate via
    ``NextMarker`` / ``Marker`` (override with ``marker_flag`` and
    ``marker_arg`` kwargs).
    """
    marker_flag = kwargs.pop("marker_flag", "NextMarker")
    marker_arg = kwargs.pop("marker_arg", "Marker")
    while True:
        ret = function(*args, **kwargs)
        marker = ret.get(marker_flag)
        yield ret
        if not marker:
            break
        kwargs[marker_arg] = marker


def ordered(obj):
    if isinstance(obj, (list, tuple)):
        return sorted(ordered(x) for x in obj)
    if isinstance(obj, dict):
        return {str(k) if isinstance(k, str) else k: ordered(v) for k, v in obj.items()}
    if isinstance(obj, str):
        return str(obj)
    return obj


def json_objs_equal(left, right):
    """Compare two parsed JSON objects, ignoring ordering inside containers."""
    return ordered(left) == ordered(right)
