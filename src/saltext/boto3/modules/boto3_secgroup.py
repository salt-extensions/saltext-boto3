"""
Connection module for Amazon EC2 Security Groups using boto3.
=============================================================

    Renamed from ``boto_secgroup`` to ``boto3_secgroup`` and rewritten to use
    the boto3 EC2 client API directly via
    :py:mod:`saltext.boto3.utils.boto3mod`. The legacy boto2 code path has
    been removed.

:depends:
  - boto3 >= 1.28.0
  - botocore >= 1.31.0

:configuration: This module accepts explicit Security Group credentials but can
    also utilize IAM roles assigned to the instance through Instance Profiles.
    Dynamic credentials are then automatically obtained from AWS API and no
    further configuration is necessary. More Information available at:

    .. code-block:: text

        http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html

    If IAM roles are not used you need to specify them either in the minion's
    config file or as a profile. For example, to specify them in the minion's
    config file:

.. code-block:: yaml

    secgroup.keyid: GKTADJGHEIQSXMKKRBJ08H
    secgroup.key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs

A region may also be specified in the configuration:

.. code-block:: yaml

    secgroup.region: us-east-1

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

from salt.exceptions import CommandExecutionError
from salt.exceptions import SaltInvocationError
from salt.utils import odict

from saltext.boto3.utils import boto3mod

try:
    from botocore.exceptions import ClientError

    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

log = logging.getLogger(__name__)

__virtualname__ = "boto3_secgroup"


def __virtual__():
    """
    Only load if boto3 is available.
    """
    if HAS_BOTO3:
        return __virtualname__
    return (False, "The boto3_secgroup module could not be loaded: boto3 is not available.")


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


def _client_error_code(exc):
    if isinstance(exc, ClientError):
        return exc.response.get("Error", {}).get("Code")
    return None


def _tags_dict(tags):
    return {t["Key"]: t["Value"] for t in (tags or [])}


def _vpc_name_to_id(
    vpc_id=None, vpc_name=None, region=None, key=None, keyid=None, profile=None
):  # pylint: disable=unused-argument
    data = __salt__["boto3_vpc.get_id"](
        name=vpc_name, region=region, key=key, keyid=keyid, profile=profile
    )
    return data.get("id")


def _get_group(
    conn=None,
    name=None,
    vpc_id=None,
    vpc_name=None,
    group_id=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Return a single security group dict (boto3 shape) or ``None``.
    """
    if vpc_name and vpc_id:
        raise SaltInvocationError("The params 'vpc_id' and 'vpc_name' are mutually exclusive.")
    if vpc_name:
        try:
            vpc_id = _vpc_name_to_id(
                vpc_name=vpc_name,
                region=region,
                key=key,
                keyid=keyid,
                profile=profile,
            )
        except ClientError as exc:
            log.debug(exc)
            return None
        if not vpc_id:
            return None
    if conn is None:
        conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
    if group_id:
        try:
            resp = conn.describe_security_groups(GroupIds=[group_id])
        except ClientError as exc:
            log.debug(exc)
            return None
        groups = resp.get("SecurityGroups", [])
        return groups[0] if len(groups) == 1 else None
    if not name:
        return None
    filters = [{"Name": "group-name", "Values": [name]}]
    if vpc_id:
        filters.append({"Name": "vpc-id", "Values": [vpc_id]})
    try:
        resp = conn.describe_security_groups(Filters=filters)
    except ClientError as exc:
        log.debug(exc)
        return None
    groups = resp.get("SecurityGroups", [])
    if vpc_id:
        return groups[0] if len(groups) == 1 else None
    # No vpc_id filter: prefer EC2-Classic (no VpcId) group when multiple exist.
    for g in groups:
        if not g.get("VpcId"):
            return g
    if len(groups) > 1:
        raise CommandExecutionError("Security group belongs to more VPCs, specify the VPC ID!")
    if len(groups) == 1:
        return groups[0]
    return None


def _parse_ip_permissions(permissions):
    """
    Convert boto3 IpPermissions to a list of legacy-style split-rule dicts.
    """
    rules = []
    for perm in permissions or []:
        proto = perm.get("IpProtocol")
        from_port = perm.get("FromPort")
        to_port = perm.get("ToPort")

        def _base(proto=proto, from_port=from_port, to_port=to_port):
            r = odict.OrderedDict()
            r["ip_protocol"] = proto
            if from_port is not None:
                r["from_port"] = int(from_port)
            if to_port is not None:
                r["to_port"] = int(to_port)
            return r

        for ipr in perm.get("IpRanges") or []:
            rule = _base()
            rule["cidr_ip"] = ipr["CidrIp"]
            rules.append(rule)
        for ipr in perm.get("Ipv6Ranges") or []:
            rule = _base()
            rule["cidr_ipv6"] = ipr["CidrIpv6"]
            rules.append(rule)
        for pair in perm.get("UserIdGroupPairs") or []:
            rule = _base()
            if pair.get("GroupName"):
                rule["source_group_name"] = pair["GroupName"]
            if pair.get("UserId"):
                rule["source_group_owner_id"] = pair["UserId"]
            if pair.get("GroupId"):
                rule["source_group_group_id"] = pair["GroupId"]
            rules.append(rule)
        if not (perm.get("IpRanges") or perm.get("Ipv6Ranges") or perm.get("UserIdGroupPairs")):
            rules.append(_base())
    return rules


def _build_permission(
    ip_protocol=None,
    from_port=None,
    to_port=None,
    cidr_ip=None,
    source_group_name=None,
    source_group_owner_id=None,
    source_group_group_id=None,
):
    perm = {"IpProtocol": str(ip_protocol) if ip_protocol is not None else "-1"}
    if from_port is not None:
        perm["FromPort"] = int(from_port)
    if to_port is not None:
        perm["ToPort"] = int(to_port)
    if cidr_ip:
        if isinstance(cidr_ip, (list, tuple)):
            perm["IpRanges"] = [{"CidrIp": c} for c in cidr_ip]
        else:
            perm["IpRanges"] = [{"CidrIp": cidr_ip}]
    if source_group_group_id or source_group_name:
        pair = {}
        if source_group_group_id:
            pair["GroupId"] = source_group_group_id
        if source_group_name:
            pair["GroupName"] = source_group_name
        if source_group_owner_id:
            pair["UserId"] = source_group_owner_id
        perm["UserIdGroupPairs"] = [pair]
    return perm


def exists(
    name=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
    vpc_id=None,
    vpc_name=None,
    group_id=None,
):
    """
    Check to see if a security group exists.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_secgroup.exists mysecgroup
    """
    conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
    group = _get_group(
        conn=conn,
        name=name,
        vpc_id=vpc_id,
        vpc_name=vpc_name,
        group_id=group_id,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )
    return bool(group)


def _split_rules(rules):
    """
    Split rules with combined grants into individual rules.

    Kept for API-compat with callers that used the legacy helper.
    """
    split = []
    for rule in rules:
        ip_protocol = rule.get("ip_protocol")
        to_port = rule.get("to_port")
        from_port = rule.get("from_port")
        grants = rule.get("grants")
        if not grants:
            split.append(rule)
            continue
        for grant in grants:
            _rule = {
                "ip_protocol": ip_protocol,
                "to_port": to_port,
                "from_port": from_port,
            }
            for k, v in grant.items():
                _rule[k] = v
            split.append(_rule)
    return split


def get_all_security_groups(
    groupnames=None,
    group_ids=None,
    filters=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Return a list of all Security Groups matching the given criteria and
    filters.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_secgroup.get_all_security_groups filters='{group-name: mygroup}'
    """
    conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
    if isinstance(groupnames, str):
        groupnames = [groupnames]
    if isinstance(group_ids, str):
        group_ids = [group_ids]
    kwargs = {}
    if groupnames:
        kwargs["GroupNames"] = groupnames
    if group_ids:
        kwargs["GroupIds"] = group_ids
    if filters:
        kwargs["Filters"] = [
            {"Name": k, "Values": v if isinstance(v, (list, tuple)) else [v]}
            for k, v in filters.items()
        ]
    try:
        resp = conn.describe_security_groups(**kwargs)
    except ClientError as exc:
        log.debug(exc)
        return []
    out = []
    for g in resp.get("SecurityGroups", []):
        entry = {
            "description": g.get("Description"),
            "id": g.get("GroupId"),
            "name": g.get("GroupName"),
            "owner_id": g.get("OwnerId"),
            "region": region,
            "rules": _parse_ip_permissions(g.get("IpPermissions")),
            "rules_egress": _parse_ip_permissions(g.get("IpPermissionsEgress")),
            "tags": _tags_dict(g.get("Tags")),
            "vpc_id": g.get("VpcId"),
        }
        out.append(entry)
    return out


def get_group_id(name, vpc_id=None, vpc_name=None, region=None, key=None, keyid=None, profile=None):
    """
    Get a Group ID given a Group Name or Group Name and VPC ID

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_secgroup.get_group_id mysecgroup
    """
    if name.startswith("sg-"):
        log.debug("group %s is a group id. get_group_id not called.", name)
        return name
    conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
    group = _get_group(
        conn=conn,
        name=name,
        vpc_id=vpc_id,
        vpc_name=vpc_name,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )
    if group is None:
        return None
    return group.get("GroupId")


def convert_to_group_ids(
    groups, vpc_id=None, vpc_name=None, region=None, key=None, keyid=None, profile=None
):
    """
    Given a list of security groups and a vpc_id, convert all entries to
    security group ids.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_secgroup.convert_to_group_ids mysecgroup vpc-89yhh7h
    """
    log.debug("security group contents %s pre-conversion", groups)
    group_ids = []
    for group in groups:
        group_id = get_group_id(
            name=group,
            vpc_id=vpc_id,
            vpc_name=vpc_name,
            region=region,
            key=key,
            keyid=keyid,
            profile=profile,
        )
        if not group_id:
            if __opts__.get("test"):
                log.warning(
                    "Security Group `%s` could not be resolved to an ID. This may "
                    "cause a failure when not running in test mode.",
                    group,
                )
                return []
            raise CommandExecutionError(
                f"Could not resolve Security Group name {group} to a Group ID"
            )
        group_ids.append(str(group_id))
    log.debug("security group contents %s post-conversion", group_ids)
    return group_ids


def get_config(
    name=None,
    group_id=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
    vpc_id=None,
    vpc_name=None,
):
    """
    Get the configuration for a security group.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_secgroup.get_config mysecgroup
    """
    conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
    sg = _get_group(
        conn=conn,
        name=name,
        vpc_id=vpc_id,
        vpc_name=vpc_name,
        group_id=group_id,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )
    if not sg:
        return None
    ret = odict.OrderedDict()
    ret["name"] = sg.get("GroupName")
    ret["group_id"] = sg.get("GroupId")
    ret["owner_id"] = sg.get("OwnerId")
    ret["description"] = sg.get("Description")
    ret["tags"] = _tags_dict(sg.get("Tags"))
    ret["rules"] = _parse_ip_permissions(sg.get("IpPermissions"))
    ret["rules_egress"] = _parse_ip_permissions(sg.get("IpPermissionsEgress"))
    return ret


def create(
    name,
    description,
    vpc_id=None,
    vpc_name=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Create a security group.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_secgroup.create mysecgroup 'My Security Group'
    """
    conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
    if not vpc_id and vpc_name:
        try:
            vpc_id = _vpc_name_to_id(
                vpc_name=vpc_name,
                region=region,
                key=key,
                keyid=keyid,
                profile=profile,
            )
        except ClientError as exc:
            log.debug(exc)
            return False
        if not vpc_id:
            return False
    kwargs = {"GroupName": name, "Description": description}
    if vpc_id:
        kwargs["VpcId"] = vpc_id
    try:
        resp = conn.create_security_group(**kwargs)
    except ClientError as exc:
        log.error("Failed to create security group %s: %s", name, exc)
        return False
    if resp.get("GroupId"):
        log.info("Created security group %s.", name)
        return True
    log.error("Failed to create security group %s.", name)
    return False


def delete(
    name=None,
    group_id=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
    vpc_id=None,
    vpc_name=None,
):
    """
    Delete a security group.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_secgroup.delete mysecgroup
    """
    conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
    group = _get_group(
        conn=conn,
        name=name,
        vpc_id=vpc_id,
        vpc_name=vpc_name,
        group_id=group_id,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )
    if not group:
        log.debug("Security group not found.")
        return False
    try:
        conn.delete_security_group(GroupId=group["GroupId"])
    except ClientError as exc:
        log.error("Failed to delete security group %s: %s", name, exc)
        return False
    log.info("Deleted security group %s with id %s.", group.get("GroupName"), group["GroupId"])
    return True


def _authorize_or_revoke(
    name=None,
    source_group_name=None,
    source_group_owner_id=None,
    ip_protocol=None,
    from_port=None,
    to_port=None,
    cidr_ip=None,
    group_id=None,
    source_group_group_id=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
    vpc_id=None,
    vpc_name=None,
    egress=False,
    revoke=False,
):
    conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
    group = _get_group(
        conn=conn,
        name=name,
        vpc_id=vpc_id,
        vpc_name=vpc_name,
        group_id=group_id,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )
    if not group:
        log.error("Failed to %s rule: security group not found.", "revoke" if revoke else "add")
        return False
    perm = _build_permission(
        ip_protocol=ip_protocol,
        from_port=from_port,
        to_port=to_port,
        cidr_ip=cidr_ip,
        source_group_name=source_group_name,
        source_group_owner_id=source_group_owner_id,
        source_group_group_id=source_group_group_id,
    )
    op = None
    try:
        if revoke:
            if egress:
                op = conn.revoke_security_group_egress
            else:
                op = conn.revoke_security_group_ingress
        else:
            if egress:
                op = conn.authorize_security_group_egress
            else:
                op = conn.authorize_security_group_ingress
        op(GroupId=group["GroupId"], IpPermissions=[perm])
    except ClientError as exc:
        code = _client_error_code(exc)
        if not revoke and code == "InvalidPermission.Duplicate":
            return True
        log.error(
            "Failed to %s rule on security group %s with id %s: %s",
            "remove" if revoke else "add",
            group.get("GroupName"),
            group["GroupId"],
            exc,
        )
        return False
    log.info(
        "%s rule on security group %s with id %s.",
        "Removed" if revoke else "Added",
        group.get("GroupName"),
        group["GroupId"],
    )
    return True


def authorize(
    name=None,
    source_group_name=None,
    source_group_owner_id=None,
    ip_protocol=None,
    from_port=None,
    to_port=None,
    cidr_ip=None,
    group_id=None,
    source_group_group_id=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
    vpc_id=None,
    vpc_name=None,
    egress=False,
):
    """
    Add a new rule to an existing security group.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_secgroup.authorize mysecgroup ip_protocol=tcp from_port=80 to_port=80 cidr_ip='10.0.0.0/8'
    """
    return _authorize_or_revoke(
        name=name,
        source_group_name=source_group_name,
        source_group_owner_id=source_group_owner_id,
        ip_protocol=ip_protocol,
        from_port=from_port,
        to_port=to_port,
        cidr_ip=cidr_ip,
        group_id=group_id,
        source_group_group_id=source_group_group_id,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
        vpc_id=vpc_id,
        vpc_name=vpc_name,
        egress=egress,
        revoke=False,
    )


def revoke(
    name=None,
    source_group_name=None,
    source_group_owner_id=None,
    ip_protocol=None,
    from_port=None,
    to_port=None,
    cidr_ip=None,
    group_id=None,
    source_group_group_id=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
    vpc_id=None,
    vpc_name=None,
    egress=False,
):
    """
    Remove a rule from an existing security group.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_secgroup.revoke mysecgroup ip_protocol=tcp from_port=80 to_port=80 cidr_ip='10.0.0.0/8'
    """
    return _authorize_or_revoke(
        name=name,
        source_group_name=source_group_name,
        source_group_owner_id=source_group_owner_id,
        ip_protocol=ip_protocol,
        from_port=from_port,
        to_port=to_port,
        cidr_ip=cidr_ip,
        group_id=group_id,
        source_group_group_id=source_group_group_id,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
        vpc_id=vpc_id,
        vpc_name=vpc_name,
        egress=egress,
        revoke=True,
    )


def set_tags(
    tags,
    name=None,
    group_id=None,
    vpc_name=None,
    vpc_id=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Sets tags on a security group.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_secgroup.set_tags "{'TAG1': 'Value1'}" security_group_name vpc_id=vpc-13435
    """
    if not isinstance(tags, dict):
        raise SaltInvocationError("Tags must be a dict of tagname:tagvalue")
    conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
    secgrp = _get_group(
        conn=conn,
        name=name,
        vpc_id=vpc_id,
        vpc_name=vpc_name,
        group_id=group_id,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )
    if not secgrp:
        raise SaltInvocationError("The security group could not be found")
    try:
        conn.create_tags(
            Resources=[secgrp["GroupId"]],
            Tags=[{"Key": k, "Value": v} for k, v in tags.items()],
        )
    except ClientError as exc:
        log.error("Failed to set tags on %s: %s", secgrp["GroupId"], exc)
        return False
    return True


def delete_tags(
    tags,
    name=None,
    group_id=None,
    vpc_name=None,
    vpc_id=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Deletes tags from a security group.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_secgroup.delete_tags ['TAG1','TAG2'] security_group_name vpc_id=vpc-13435
    """
    if not isinstance(tags, list):
        raise SaltInvocationError(
            "Tags must be a list of tagnames to remove from the security group"
        )
    conn = _get_conn("ec2", region=region, key=key, keyid=keyid, profile=profile)
    secgrp = _get_group(
        conn=conn,
        name=name,
        vpc_id=vpc_id,
        vpc_name=vpc_name,
        group_id=group_id,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )
    if not secgrp:
        raise SaltInvocationError("The security group could not be found")
    try:
        conn.delete_tags(
            Resources=[secgrp["GroupId"]],
            Tags=[{"Key": t} for t in tags],
        )
    except ClientError as exc:
        log.error("Failed to delete tags on %s: %s", secgrp["GroupId"], exc)
        return False
    return True
