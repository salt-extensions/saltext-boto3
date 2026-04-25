"""
Connection module for Amazon ELB (Classic) using boto3.
=======================================================

    Renamed from ``boto_elb`` to ``boto3_elb`` and rewritten to use the
    boto3 ``elb`` client APIs directly via
    :py:mod:`saltext.boto3.utils.boto3mod`.  The legacy boto2 code path
    (object-style access, retry loops) has been removed.

:depends:
  - boto3 >= 1.28.0
  - botocore >= 1.31.0

:configuration: This module accepts explicit ELB credentials but can
    also utilize IAM roles assigned to the instance through Instance Profiles.
    Dynamic credentials are then automatically obtained from AWS API and no
    further configuration is necessary. More Information available at:

    .. code-block:: text

        http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html

    If IAM roles are not used you need to specify them either in the minion's
    config file or as a profile. For example, to specify them in the minion's
    config file:

.. code-block:: yaml

    elb.keyid: GKTADJGHEIQSXMKKRBJ08H
    elb.key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs

A region may also be specified in the configuration:

.. code-block:: yaml

    elb.region: us-east-1

It's also possible to specify key, keyid and region via a profile, either
as a passed in dict, or as a string to pull from pillars or minion config:

.. code-block:: yaml

    myprofile:
        keyid: GKTADJGHEIQSXMKKRBJ08H
        key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs
        region: us-east-1

.. versionadded:: 1.0.0
"""

import logging

import salt.utils.json
from salt.utils import odict

from saltext.boto3.utils import boto3mod

try:
    from botocore.exceptions import ClientError

    logging.getLogger("boto3").setLevel(logging.CRITICAL)
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

log = logging.getLogger(__name__)

__virtualname__ = "boto3_elb"


def _get_conn(service, region=None, key=None, keyid=None, profile=None):
    """
    Return a boto3 client for ``service`` using this module's dunders.
    """
    return boto3mod.get_connection(
        service,
        opts=__opts__,
        context=__context__,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )


def __virtual__():
    """
    Only load if boto3 is available.
    """
    if HAS_BOTO3:
        return __virtualname__
    return (False, "The boto3_elb module could not be loaded: boto3 is not available.")


def _error_code(exc):
    try:
        return exc.response["Error"]["Code"]
    except (AttributeError, KeyError, TypeError):
        return ""


def exists(name, region=None, key=None, keyid=None, profile=None):
    """
    Check to see if an ELB exists.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_elb.exists myelb region=us-east-1
    """
    conn = _get_conn("elb", region=region, key=key, keyid=keyid, profile=profile)
    try:
        result = conn.describe_load_balancers(LoadBalancerNames=[name])
        if result.get("LoadBalancerDescriptions"):
            return True
        log.debug("The load balancer does not exist in region %s", region)
        return False
    except ClientError as error:
        log.warning(error)
        return False


def get_all_elbs(region=None, key=None, keyid=None, profile=None):
    """
    Return all load balancers associated with an account.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_elb.get_all_elbs region=us-east-1
    """
    conn = _get_conn("elb", region=region, key=key, keyid=keyid, profile=profile)
    try:
        paginator = conn.get_paginator("describe_load_balancers")
        result = []
        for page in paginator.paginate():
            result.extend(page.get("LoadBalancerDescriptions", []))
        return result
    except ClientError as error:
        log.warning(error)
        return []


def list_elbs(region=None, key=None, keyid=None, profile=None):
    """
    Return names of all load balancers associated with an account.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_elb.list_elbs region=us-east-1
    """
    return [
        lb["LoadBalancerName"]
        for lb in get_all_elbs(region=region, key=key, keyid=keyid, profile=profile)
    ]


def _tags_for_lb(conn, name):
    try:
        resp = conn.describe_tags(LoadBalancerNames=[name])
    except ClientError as error:
        log.warning(error)
        return None
    for desc in resp.get("TagDescriptions", []):
        if desc.get("LoadBalancerName") == name:
            return {t["Key"]: t.get("Value") for t in desc.get("Tags", [])}
    return None


def get_elb_config(name, region=None, key=None, keyid=None, profile=None):
    """
    Get an ELB configuration.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_elb.get_elb_config myelb region=us-east-1
    """
    conn = _get_conn("elb", region=region, key=key, keyid=keyid, profile=profile)
    try:
        resp = conn.describe_load_balancers(LoadBalancerNames=[name])
    except ClientError as error:
        log.error("Error fetching config for ELB %s: %s", name, error)
        return {}
    descs = resp.get("LoadBalancerDescriptions", [])
    if not descs:
        return {}
    lb = descs[0]
    ret = {}
    ret["availability_zones"] = lb.get("AvailabilityZones", [])
    listeners = []
    for ld in lb.get("ListenerDescriptions", []):
        listener_info = ld.get("Listener", {})
        listener_dict = {
            "elb_port": listener_info.get("LoadBalancerPort"),
            "elb_protocol": listener_info.get("Protocol"),
            "instance_port": listener_info.get("InstancePort"),
            "instance_protocol": listener_info.get("InstanceProtocol"),
            "policies": list(ld.get("PolicyNames", [])),
        }
        if listener_info.get("SSLCertificateId"):
            listener_dict["certificate"] = listener_info["SSLCertificateId"]
        listeners.append(listener_dict)
    ret["listeners"] = listeners
    backends = []
    for backend in lb.get("BackendServerDescriptions", []):
        backends.append(
            {
                "instance_port": backend.get("InstancePort"),
                "policies": list(backend.get("PolicyNames", [])),
            }
        )
    ret["backends"] = backends
    ret["subnets"] = lb.get("Subnets", [])
    ret["security_groups"] = lb.get("SecurityGroups", [])
    ret["scheme"] = lb.get("Scheme")
    ret["dns_name"] = lb.get("DNSName")
    ret["tags"] = _tags_for_lb(conn, name)
    policies = []
    pols = lb.get("Policies", {})
    for p in pols.get("AppCookieStickinessPolicies", []):
        policies.append(p.get("PolicyName"))
    for p in pols.get("LBCookieStickinessPolicies", []):
        policies.append(p.get("PolicyName"))
    for p in pols.get("OtherPolicies", []):
        policies.append(p)
    ret["policies"] = policies
    ret["canonical_hosted_zone_name"] = lb.get("CanonicalHostedZoneName")
    ret["canonical_hosted_zone_name_id"] = lb.get("CanonicalHostedZoneNameID")
    ret["vpc_id"] = lb.get("VPCId")
    return ret


def listener_dict_to_tuple(listener):
    """
    Convert an ELB listener dict into a listener tuple used by certain parts of
    the AWS ELB API.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_elb.listener_dict_to_tuple '{"elb_port":80,"instance_port":80,"elb_protocol":"HTTP"}'
    """
    if "instance_protocol" not in listener:
        instance_protocol = listener["elb_protocol"].upper()
    else:
        instance_protocol = listener["instance_protocol"].upper()
    listener_tuple = [
        listener["elb_port"],
        listener["instance_port"],
        listener["elb_protocol"],
        instance_protocol,
    ]
    if "certificate" in listener:
        listener_tuple.append(listener["certificate"])
    return tuple(listener_tuple)


def _listener_dict_to_api(listener):
    api = {
        "Protocol": listener["elb_protocol"],
        "LoadBalancerPort": listener["elb_port"],
        "InstancePort": listener["instance_port"],
        "InstanceProtocol": listener.get("instance_protocol", listener["elb_protocol"]).upper(),
    }
    if listener.get("certificate"):
        api["SSLCertificateId"] = listener["certificate"]
    return api


def create(
    name,
    availability_zones,
    listeners,
    subnets=None,
    security_groups=None,
    scheme="internet-facing",
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Create an ELB.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_elb.create myelb '["us-east-1a"]' '[{"elb_port": 443, "elb_protocol": "HTTPS"}]'
    """
    if exists(name, region, key, keyid, profile):
        return True
    if isinstance(availability_zones, str):
        availability_zones = salt.utils.json.loads(availability_zones)
    if isinstance(listeners, str):
        listeners = salt.utils.json.loads(listeners)
    api_listeners = [_listener_dict_to_api(listener) for listener in listeners]
    conn = _get_conn("elb", region=region, key=key, keyid=keyid, profile=profile)
    kwargs = {
        "LoadBalancerName": name,
        "Listeners": api_listeners,
        "Scheme": scheme,
    }
    if availability_zones:
        kwargs["AvailabilityZones"] = availability_zones
    if subnets:
        kwargs["Subnets"] = subnets
    if security_groups:
        kwargs["SecurityGroups"] = security_groups
    try:
        lb = conn.create_load_balancer(**kwargs)
    except ClientError as error:
        log.error(
            "Failed to create ELB %s: %s: %s",
            name,
            _error_code(error),
            error.response.get("Error", {}).get("Message", ""),
            exc_info_on_loglevel=logging.DEBUG,
        )
        return False
    if lb:
        log.info("Created ELB %s", name)
        return True
    log.error("Failed to create ELB %s", name)
    return False


def delete(name, region=None, key=None, keyid=None, profile=None):
    """
    Delete an ELB.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_elb.delete myelb region=us-east-1
    """
    if not exists(name, region, key, keyid, profile):
        return True
    conn = _get_conn("elb", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.delete_load_balancer(LoadBalancerName=name)
        log.info("Deleted ELB %s.", name)
        return True
    except ClientError:
        log.error("Failed to delete ELB %s", name, exc_info_on_loglevel=logging.DEBUG)
        return False


def create_listeners(name, listeners, region=None, key=None, keyid=None, profile=None):
    """
    Create listeners on an ELB.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_elb.create_listeners myelb '[{"elb_port":443,"instance_port":80,"elb_protocol":"HTTPS","certificate":"arn:..."}]'
    """
    if isinstance(listeners, str):
        listeners = salt.utils.json.loads(listeners)
    api_listeners = [_listener_dict_to_api(listener) for listener in listeners]
    conn = _get_conn("elb", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.create_load_balancer_listeners(LoadBalancerName=name, Listeners=api_listeners)
        log.info("Created ELB listeners on %s", name)
        return True
    except ClientError as error:
        log.error(
            "Failed to create ELB listeners on %s: %s",
            name,
            error,
            exc_info_on_loglevel=logging.DEBUG,
        )
        return False


def delete_listeners(name, ports, region=None, key=None, keyid=None, profile=None):
    """
    Delete listeners on an ELB.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_elb.delete_listeners myelb '[80,443]'
    """
    if isinstance(ports, str):
        ports = salt.utils.json.loads(ports)
    conn = _get_conn("elb", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.delete_load_balancer_listeners(LoadBalancerName=name, LoadBalancerPorts=list(ports))
        log.info("Deleted ELB listeners on %s", name)
        return True
    except ClientError as error:
        log.error(
            "Failed to delete ELB listeners on %s: %s",
            name,
            error,
            exc_info_on_loglevel=logging.DEBUG,
        )
        return False


def apply_security_groups(name, security_groups, region=None, key=None, keyid=None, profile=None):
    """
    Apply security groups to ELB.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_elb.apply_security_groups myelb '["mysecgroup1"]'
    """
    if isinstance(security_groups, str):
        security_groups = salt.utils.json.loads(security_groups)
    conn = _get_conn("elb", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.apply_security_groups_to_load_balancer(
            LoadBalancerName=name, SecurityGroups=list(security_groups)
        )
        log.info("Applied security_groups on ELB %s", name)
        return True
    except ClientError as error:
        log.error("Failed to apply security_groups on ELB %s: %s", name, error)
        return False


def enable_availability_zones(
    name, availability_zones, region=None, key=None, keyid=None, profile=None
):
    """
    Enable availability zones for ELB.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_elb.enable_availability_zones myelb '["us-east-1a"]'
    """
    if isinstance(availability_zones, str):
        availability_zones = salt.utils.json.loads(availability_zones)
    conn = _get_conn("elb", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.enable_availability_zones_for_load_balancer(
            LoadBalancerName=name, AvailabilityZones=list(availability_zones)
        )
        log.info("Enabled availability_zones on ELB %s", name)
        return True
    except ClientError as error:
        log.error("Failed to enable availability_zones on ELB %s: %s", name, error)
        return False


def disable_availability_zones(
    name, availability_zones, region=None, key=None, keyid=None, profile=None
):
    """
    Disable availability zones for ELB.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_elb.disable_availability_zones myelb '["us-east-1a"]'
    """
    if isinstance(availability_zones, str):
        availability_zones = salt.utils.json.loads(availability_zones)
    conn = _get_conn("elb", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.disable_availability_zones_for_load_balancer(
            LoadBalancerName=name, AvailabilityZones=list(availability_zones)
        )
        log.info("Disabled availability_zones on ELB %s", name)
        return True
    except ClientError as error:
        log.error(
            "Failed to disable availability_zones on ELB %s: %s",
            name,
            error,
            exc_info_on_loglevel=logging.DEBUG,
        )
        return False


def attach_subnets(name, subnets, region=None, key=None, keyid=None, profile=None):
    """
    Attach ELB to subnets.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_elb.attach_subnets myelb '["mysubnet"]'
    """
    if isinstance(subnets, str):
        subnets = salt.utils.json.loads(subnets)
    conn = _get_conn("elb", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.attach_load_balancer_to_subnets(LoadBalancerName=name, Subnets=list(subnets))
        log.info("Attached ELB %s on subnets.", name)
        return True
    except ClientError as error:
        log.error(
            "Failed to attach ELB %s on subnets: %s",
            name,
            error,
            exc_info_on_loglevel=logging.DEBUG,
        )
        return False


def detach_subnets(name, subnets, region=None, key=None, keyid=None, profile=None):
    """
    Detach ELB from subnets.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_elb.detach_subnets myelb '["mysubnet"]'
    """
    if isinstance(subnets, str):
        subnets = salt.utils.json.loads(subnets)
    conn = _get_conn("elb", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.detach_load_balancer_from_subnets(LoadBalancerName=name, Subnets=list(subnets))
        log.info("Detached ELB %s from subnets.", name)
        return True
    except ClientError as error:
        log.error(
            "Failed to detach ELB %s from subnets: %s",
            name,
            error,
            exc_info_on_loglevel=logging.DEBUG,
        )
        return False


def get_attributes(name, region=None, key=None, keyid=None, profile=None):
    """
    Check to see if attributes are set on an ELB.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_elb.get_attributes myelb
    """
    conn = _get_conn("elb", region=region, key=key, keyid=keyid, profile=profile)
    try:
        resp = conn.describe_load_balancer_attributes(LoadBalancerName=name)
    except ClientError as error:
        log.error("ELB %s does not exist: %s", name, error)
        return {}
    attrs = resp.get("LoadBalancerAttributes", {})
    al = attrs.get("AccessLog", {})
    czlb = attrs.get("CrossZoneLoadBalancing", {})
    cd = attrs.get("ConnectionDraining", {})
    cs = attrs.get("ConnectionSettings", {})
    ret = odict.OrderedDict()
    ret["access_log"] = odict.OrderedDict()
    ret["cross_zone_load_balancing"] = odict.OrderedDict()
    ret["connection_draining"] = odict.OrderedDict()
    ret["connecting_settings"] = odict.OrderedDict()
    ret["access_log"]["enabled"] = al.get("Enabled", False)
    ret["access_log"]["s3_bucket_name"] = al.get("S3BucketName")
    ret["access_log"]["s3_bucket_prefix"] = al.get("S3BucketPrefix")
    ret["access_log"]["emit_interval"] = al.get("EmitInterval")
    ret["cross_zone_load_balancing"]["enabled"] = czlb.get("Enabled", False)
    ret["connection_draining"]["enabled"] = cd.get("Enabled", False)
    ret["connection_draining"]["timeout"] = cd.get("Timeout")
    ret["connecting_settings"]["idle_timeout"] = cs.get("IdleTimeout")
    return ret


def set_attributes(name, attributes, region=None, key=None, keyid=None, profile=None):
    """
    Set attributes on an ELB.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_elb.set_attributes myelb '{"access_log": {"enabled": true, "s3_bucket_name": "mybucket"}}'
    """
    al = attributes.get("access_log", {})
    czlb = attributes.get("cross_zone_load_balancing", {})
    cd = attributes.get("connection_draining", {})
    cs = attributes.get("connecting_settings", {})
    if not al and not czlb and not cd and not cs:
        log.error("No supported attributes for ELB.")
        return False
    api_attrs = {}
    if al:
        if not al.get("enabled", False):
            log.error("Access log attribute configured, but enabled config missing")
            return False
        access_log = {"Enabled": True}
        if al.get("s3_bucket_name") is not None:
            access_log["S3BucketName"] = al["s3_bucket_name"]
        if al.get("s3_bucket_prefix") is not None:
            access_log["S3BucketPrefix"] = al["s3_bucket_prefix"]
        if al.get("emit_interval") is not None:
            access_log["EmitInterval"] = int(al["emit_interval"])
        api_attrs["AccessLog"] = access_log
    if czlb:
        api_attrs["CrossZoneLoadBalancing"] = {"Enabled": bool(czlb["enabled"])}
    if cd:
        api_attrs["ConnectionDraining"] = {
            "Enabled": bool(cd["enabled"]),
            "Timeout": int(cd.get("timeout", 300)),
        }
    if cs:
        api_attrs["ConnectionSettings"] = {"IdleTimeout": int(cs.get("idle_timeout", 60))}
    conn = _get_conn("elb", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.modify_load_balancer_attributes(
            LoadBalancerName=name, LoadBalancerAttributes=api_attrs
        )
    except ClientError as error:
        log.error("Failed to set attributes on ELB %s: %s", name, error)
        return False
    log.info("Set attributes on ELB %s.", name)
    return True


def get_health_check(name, region=None, key=None, keyid=None, profile=None):
    """
    Get the health check configured for this ELB.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_elb.get_health_check myelb
    """
    conn = _get_conn("elb", region=region, key=key, keyid=keyid, profile=profile)
    try:
        resp = conn.describe_load_balancers(LoadBalancerNames=[name])
    except ClientError:
        log.error("ELB %s not found.", name, exc_info_on_loglevel=logging.DEBUG)
        return {}
    descs = resp.get("LoadBalancerDescriptions", [])
    if not descs:
        return {}
    hc = descs[0].get("HealthCheck", {})
    ret = odict.OrderedDict()
    ret["interval"] = hc.get("Interval")
    ret["target"] = hc.get("Target")
    ret["healthy_threshold"] = hc.get("HealthyThreshold")
    ret["timeout"] = hc.get("Timeout")
    ret["unhealthy_threshold"] = hc.get("UnhealthyThreshold")
    return ret


def set_health_check(name, health_check, region=None, key=None, keyid=None, profile=None):
    """
    Set a health check on an ELB.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_elb.set_health_check myelb '{"target": "HTTP:80/"}'
    """
    hc = {}
    mapping = {
        "interval": "Interval",
        "target": "Target",
        "healthy_threshold": "HealthyThreshold",
        "timeout": "Timeout",
        "unhealthy_threshold": "UnhealthyThreshold",
    }
    for src, dst in mapping.items():
        if src in health_check and health_check[src] is not None:
            hc[dst] = health_check[src]
    conn = _get_conn("elb", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.configure_health_check(LoadBalancerName=name, HealthCheck=hc)
        log.info("Configured health check on ELB %s", name)
        return True
    except ClientError:
        log.exception("Failed to configure health check on ELB %s", name)
        return False


def register_instances(name, instances, region=None, key=None, keyid=None, profile=None):
    """
    Register instances with an ELB.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_elb.register_instances myelb instance_id
    """
    if isinstance(instances, str):
        instances = [instances]
    instances = list(instances)
    conn = _get_conn("elb", region=region, key=key, keyid=keyid, profile=profile)
    try:
        resp = conn.register_instances_with_load_balancer(
            LoadBalancerName=name,
            Instances=[{"InstanceId": i} for i in instances],
        )
    except ClientError as error:
        log.warning(error)
        return False
    registered_ids = {i.get("InstanceId") for i in resp.get("Instances", [])}
    register_failures = set(instances).difference(registered_ids)
    if register_failures:
        log.warning(
            "Instance(s): %s not registered with ELB %s.",
            list(register_failures),
            name,
        )
        return False
    return True


def deregister_instances(name, instances, region=None, key=None, keyid=None, profile=None):
    """
    Deregister instances with an ELB.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_elb.deregister_instances myelb instance_id
    """
    if isinstance(instances, str):
        instances = [instances]
    instances = list(instances)
    conn = _get_conn("elb", region=region, key=key, keyid=keyid, profile=profile)
    try:
        resp = conn.deregister_instances_from_load_balancer(
            LoadBalancerName=name,
            Instances=[{"InstanceId": i} for i in instances],
        )
    except ClientError as error:
        if _error_code(error) == "InvalidInstance":
            log.warning(
                "One or more of instance(s) %s are not part of ELB %s. "
                "deregister_instances not performed.",
                instances,
                name,
            )
            return None
        log.warning(error)
        return False
    remaining_ids = {i.get("InstanceId") for i in resp.get("Instances", [])}
    deregister_failures = set(instances).intersection(remaining_ids)
    if deregister_failures:
        log.warning(
            "Instance(s): %s not deregistered from ELB %s.",
            list(deregister_failures),
            name,
        )
        return False
    return True


def set_instances(name, instances, test=False, region=None, key=None, keyid=None, profile=None):
    """
    Set the instances assigned to an ELB to exactly the list given.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_elb.set_instances myelb region=us-east-1 instances="[instance_id,instance_id]"
    """
    ret = True
    current = {i["instance_id"] for i in get_instance_health(name, region, key, keyid, profile)}
    desired = set(instances)
    add = desired - current
    remove = current - desired
    if test:
        return bool(add or remove)
    if remove:
        if deregister_instances(name, list(remove), region, key, keyid, profile) is False:
            ret = False
    if add:
        if register_instances(name, list(add), region, key, keyid, profile) is False:
            ret = False
    return ret


def get_instance_health(name, region=None, key=None, keyid=None, profile=None, instances=None):
    """
    Get a list of instances and their health state.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_elb.get_instance_health myelb
    """
    conn = _get_conn("elb", region=region, key=key, keyid=keyid, profile=profile)
    kwargs = {"LoadBalancerName": name}
    if instances:
        kwargs["Instances"] = [{"InstanceId": i} for i in instances]
    try:
        resp = conn.describe_instance_health(**kwargs)
    except ClientError as error:
        log.debug(error)
        return []
    ret = []
    for state in resp.get("InstanceStates", []):
        ret.append(
            {
                "instance_id": state.get("InstanceId"),
                "description": state.get("Description"),
                "state": state.get("State"),
                "reason_code": state.get("ReasonCode"),
            }
        )
    return ret


def create_policy(
    name,
    policy_name,
    policy_type,
    policy,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Create an ELB policy.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_elb.create_policy myelb mypolicy LBCookieStickinessPolicyType '{"CookieExpirationPeriod": 3600}'
    """
    if not exists(name, region, key, keyid, profile):
        return False
    attributes = []
    if isinstance(policy, dict):
        for attr_name, attr_value in policy.items():
            attributes.append({"AttributeName": attr_name, "AttributeValue": str(attr_value)})
    conn = _get_conn("elb", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.create_load_balancer_policy(
            LoadBalancerName=name,
            PolicyName=policy_name,
            PolicyTypeName=policy_type,
            PolicyAttributes=attributes,
        )
        log.info("Created policy %s on ELB %s", policy_name, name)
        return True
    except ClientError as error:
        log.error(
            "Failed to create policy %s on ELB %s: %s",
            policy_name,
            name,
            error,
            exc_info_on_loglevel=logging.DEBUG,
        )
        return False


def delete_policy(name, policy_name, region=None, key=None, keyid=None, profile=None):
    """
    Delete an ELB policy.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_elb.delete_policy myelb mypolicy
    """
    if not exists(name, region, key, keyid, profile):
        return True
    conn = _get_conn("elb", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.delete_load_balancer_policy(LoadBalancerName=name, PolicyName=policy_name)
        log.info("Deleted policy %s on ELB %s", policy_name, name)
        return True
    except ClientError as error:
        log.error(
            "Failed to delete policy %s on ELB %s: %s",
            policy_name,
            name,
            error,
            exc_info_on_loglevel=logging.DEBUG,
        )
        return False


def set_listener_policy(name, port, policies=None, region=None, key=None, keyid=None, profile=None):
    """
    Set the policies of an ELB listener.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_elb.set_listener_policy myelb 443 "[policy1,policy2]"
    """
    if not exists(name, region, key, keyid, profile):
        return True
    if policies is None:
        policies = []
    conn = _get_conn("elb", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.set_load_balancer_policies_of_listener(
            LoadBalancerName=name,
            LoadBalancerPort=port,
            PolicyNames=list(policies),
        )
        log.info("Set policies %s on ELB %s listener %s", policies, name, port)
    except ClientError as error:
        log.info(
            "Failed to set policy %s on ELB %s listener %s: %s",
            policies,
            name,
            port,
            error,
            exc_info_on_loglevel=logging.DEBUG,
        )
        return False
    return True


def set_backend_policy(name, port, policies=None, region=None, key=None, keyid=None, profile=None):
    """
    Set the policies of an ELB backend server.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_elb.set_backend_policy myelb 443 "[policy1,policy2]"
    """
    if not exists(name, region, key, keyid, profile):
        return True
    if policies is None:
        policies = []
    conn = _get_conn("elb", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.set_load_balancer_policies_for_backend_server(
            LoadBalancerName=name,
            InstancePort=port,
            PolicyNames=list(policies),
        )
        log.info("Set policies %s on ELB %s backend server %s", policies, name, port)
    except ClientError as error:
        log.info(
            "Failed to set policy %s on ELB %s backend server %s: %s",
            policies,
            name,
            port,
            error,
            exc_info_on_loglevel=logging.DEBUG,
        )
        return False
    return True


def set_tags(name, tags, region=None, key=None, keyid=None, profile=None):
    """
    Add tags on an ELB.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_elb.set_tags my-elb-name "{'Tag1': 'Value', 'Tag2': 'Another Value'}"
    """
    if not exists(name, region, key, keyid, profile):
        return False
    conn = _get_conn("elb", region=region, key=key, keyid=keyid, profile=profile)
    tag_list = []
    for k, v in tags.items():
        entry = {"Key": k}
        if v is not None:
            entry["Value"] = v
        tag_list.append(entry)
    try:
        conn.add_tags(LoadBalancerNames=[name], Tags=tag_list)
        return True
    except ClientError as error:
        log.error("Failed to add tags to ELB %s: %s", name, error)
        return False


def delete_tags(name, tags, region=None, key=None, keyid=None, profile=None):
    """
    Delete tags on an ELB.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_elb.delete_tags my-elb-name ['TagToRemove1', 'TagToRemove2']
    """
    if not exists(name, region, key, keyid, profile):
        return False
    conn = _get_conn("elb", region=region, key=key, keyid=keyid, profile=profile)
    try:
        conn.remove_tags(LoadBalancerNames=[name], Tags=[{"Key": t} for t in tags])
        return True
    except ClientError as error:
        log.error("Failed to remove tags from ELB %s: %s", name, error)
        return False
