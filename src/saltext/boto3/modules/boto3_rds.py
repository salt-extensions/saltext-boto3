"""
Connection module for Amazon RDS using boto3.
=============================================

    Renamed from ``boto_rds`` to ``boto3_rds`` and rewritten to use the
    boto3 ``rds`` client APIs directly via
    :py:mod:`saltext.boto3.utils.boto3mod`.  The legacy boto2 code path
    (object-style access, retry loops) has been removed.

:depends:
  - boto3 >= 1.28.0
  - botocore >= 1.31.0

:configuration: This module accepts explicit rds credentials but can
    also utilize IAM roles assigned to the instance through Instance Profiles.
    Dynamic credentials are then automatically obtained from AWS API and no
    further configuration is necessary. More Information available at:

    .. code-block:: text

        http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html

    If IAM roles are not used you need to specify them either in the minion's
    config file or as a profile. For example, to specify them in the minion's
    config file:

.. code-block:: yaml

    rds.keyid: GKTADJGHEIQSXMKKRBJ08H
    rds.key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs

A region may also be specified in the configuration:

.. code-block:: yaml

    rds.region: us-east-1

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
import time

from salt.exceptions import SaltInvocationError
from salt.utils import odict

from saltext.boto3.utils import boto3mod

log = logging.getLogger(__name__)

try:
    from botocore.exceptions import ClientError

    logging.getLogger("boto3").setLevel(logging.CRITICAL)
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

boto3_param_map = {
    "allocated_storage": ("AllocatedStorage", int),
    "allow_major_version_upgrade": ("AllowMajorVersionUpgrade", bool),
    "apply_immediately": ("ApplyImmediately", bool),
    "auto_minor_version_upgrade": ("AutoMinorVersionUpgrade", bool),
    "availability_zone": ("AvailabilityZone", str),
    "backup_retention_period": ("BackupRetentionPeriod", int),
    "ca_certificate_identifier": ("CACertificateIdentifier", str),
    "character_set_name": ("CharacterSetName", str),
    "copy_tags_to_snapshot": ("CopyTagsToSnapshot", bool),
    "db_cluster_identifier": ("DBClusterIdentifier", str),
    "db_instance_class": ("DBInstanceClass", str),
    "db_name": ("DBName", str),
    "db_parameter_group_name": ("DBParameterGroupName", str),
    "db_port_number": ("DBPortNumber", int),
    "db_security_groups": ("DBSecurityGroups", list),
    "db_subnet_group_name": ("DBSubnetGroupName", str),
    "domain": ("Domain", str),
    "domain_iam_role_name": ("DomainIAMRoleName", str),
    "engine": ("Engine", str),
    "engine_version": ("EngineVersion", str),
    "iops": ("Iops", int),
    "kms_key_id": ("KmsKeyId", str),
    "license_model": ("LicenseModel", str),
    "master_user_password": ("MasterUserPassword", str),
    "master_username": ("MasterUsername", str),
    "monitoring_interval": ("MonitoringInterval", int),
    "monitoring_role_arn": ("MonitoringRoleArn", str),
    "multi_az": ("MultiAZ", bool),
    "name": ("DBInstanceIdentifier", str),
    "new_db_instance_identifier": ("NewDBInstanceIdentifier", str),
    "option_group_name": ("OptionGroupName", str),
    "port": ("Port", int),
    "preferred_backup_window": ("PreferredBackupWindow", str),
    "preferred_maintenance_window": ("PreferredMaintenanceWindow", str),
    "promotion_tier": ("PromotionTier", int),
    "publicly_accessible": ("PubliclyAccessible", bool),
    "storage_encrypted": ("StorageEncrypted", bool),
    "storage_type": ("StorageType", str),
    "tags": ("Tags", list),
    "tde_credential_arn": ("TdeCredentialArn", str),
    "tde_credential_password": ("TdeCredentialPassword", str),
    "vpc_security_group_ids": ("VpcSecurityGroupIds", list),
}


__virtualname__ = "boto3_rds"


def __virtual__():
    """
    Only load if boto3 is available.
    """
    if HAS_BOTO3:
        return __virtualname__
    return (False, "The boto3_rds module could not be loaded: boto3 is not available.")


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


def exists(
    name, tags=None, region=None, key=None, keyid=None, profile=None
):  # pylint: disable=unused-argument
    """
    Check to see if an RDS exists.

    name (str):
        Name of the RDS instance to check.

    tags (list):
        Optional list of tags to filter the RDS instance by.

    region (str):
        AWS region where the RDS instance is located.

    key (str):
        AWS access key ID.

    keyid (str):
        AWS secret access key.

    profile (str):
        AWS profile to use for the connection.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_rds.exists myrds region=us-east-1
    """
    conn = _get_conn("rds", region=region, key=key, keyid=keyid, profile=profile)

    try:
        rds = conn.describe_db_instances(DBInstanceIdentifier=name)
        return {"exists": bool(rds)}
    except ClientError as e:
        return {"error": boto3mod.get_error(e)}


def option_group_exists(
    name, tags=None, region=None, key=None, keyid=None, profile=None
):  # pylint: disable=unused-argument
    """
    Check to see if an RDS option group exists.

    name (str):
        Name of the RDS option group to check.

    tags (list):
        Optional list of tags to filter the RDS option group by.

    region (str):
        AWS region where the RDS option group is located.

    key (str):
        AWS access key ID.

    keyid (str):
        AWS secret access key.

    profile (str):
        AWS profile to use for the connection.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_rds.option_group_exists myoptiongr \
            region=us-east-1
    """
    conn = _get_conn("rds", region=region, key=key, keyid=keyid, profile=profile)

    try:
        rds = conn.describe_option_groups(OptionGroupName=name)
        return {"exists": bool(rds)}
    except ClientError as e:
        return {"error": boto3mod.get_error(e)}


def parameter_group_exists(
    name, tags=None, region=None, key=None, keyid=None, profile=None
):  # pylint: disable=unused-argument
    """
    Check to see if an RDS parameter group exists.

    name (str):
        Name of the RDS parameter group to check.

    tags (list):
        Optional list of tags to filter the RDS parameter group by.

    region (str):
        AWS region where the RDS parameter group is located.

    key (str):
        AWS access key ID.

    keyid (str):
        AWS secret access key.

    profile (str):
        AWS profile to use for the connection.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_rds.parameter_group_exists myparametergroup \
            region=us-east-1
    """
    conn = _get_conn("rds", region=region, key=key, keyid=keyid, profile=profile)

    try:
        rds = conn.describe_db_parameter_groups(DBParameterGroupName=name)
        return {"exists": bool(rds), "error": None}
    except ClientError as e:
        resp = {}
        if e.response["Error"]["Code"] == "DBParameterGroupNotFound":
            resp["exists"] = False
        resp["error"] = boto3mod.get_error(e)
        return resp


def subnet_group_exists(
    name, tags=None, region=None, key=None, keyid=None, profile=None
):  # pylint: disable=unused-argument
    """
    Check to see if an RDS subnet group exists.

    .. versionchanged:: 1.1.0
        Updated exception handling to properly check for the
        existence of the subnet group.

    name (str):
        Name of the RDS subnet group to check.

    tags (list):
        Optional list of tags to filter the RDS subnet group by.

    region (str):
        AWS region where the RDS subnet group is located.

    key (str):
        AWS access key ID.

    keyid (str):
        AWS secret access key.

    profile (str):
        AWS profile to use for the connection.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_rds.subnet_group_exists my-param-group /
            region=us-east-1
    """
    try:
        conn = _get_conn("rds", region=region, key=key, keyid=keyid, profile=profile)
        if not conn:
            return {"exists": bool(conn)}

        rds = conn.describe_db_subnet_groups(DBSubnetGroupName=name)
        return {"exists": bool(rds)}
    except ClientError as e:
        if e.response["Error"]["Code"] == "DBSubnetGroupNotFoundFault":
            return {"exists": False}
        return {"error": boto3mod.get_error(e)}


def create(
    name,
    allocated_storage,
    db_instance_class,
    engine,
    master_username,
    master_user_password,
    db_name=None,
    db_security_groups=None,
    vpc_security_group_ids=None,
    vpc_security_groups=None,
    availability_zone=None,
    db_subnet_group_name=None,
    preferred_maintenance_window=None,
    db_parameter_group_name=None,
    backup_retention_period=None,
    preferred_backup_window=None,
    port=None,
    multi_az=None,
    engine_version=None,
    auto_minor_version_upgrade=None,
    license_model=None,
    iops=None,
    option_group_name=None,
    character_set_name=None,
    publicly_accessible=None,
    wait_status=None,
    tags=None,
    db_cluster_identifier=None,
    storage_type=None,
    tde_credential_arn=None,
    tde_credential_password=None,
    storage_encrypted=None,
    kms_key_id=None,
    domain=None,
    copy_tags_to_snapshot=None,
    monitoring_interval=None,
    monitoring_role_arn=None,
    domain_iam_role_name=None,
    region=None,
    promotion_tier=None,
    key=None,
    keyid=None,
    profile=None,
):  # pylint: disable=too-many-arguments,too-many-locals,unused-argument
    """
    Create an RDS Instance

    name (str):
        Name of the RDS instance to create.

    allocated_storage (int):
        The amount of storage (in gibibytes) to allocate for the RDS instance.

    db_instance_class (str):
        The compute and memory capacity of the RDS instance.

    engine (str):
        The database engine to use for the RDS instance.

    master_username (str):
        The master username for the RDS instance.

    master_user_password (str):
        The master user password for the RDS instance.

    db_name (str):
        The name of the database to create when the RDS instance is created.

    db_security_groups (list):
        A list of DB security groups to associate with the RDS instance.

    vpc_security_group_ids (list):
        A list of VPC security group IDs to associate with the RDS instance.

    vpc_security_groups (list):
        A list of VPC security group names to associate with the RDS instance.

    availability_zone (str):
        The availability zone where the RDS instance will be created.

    db_subnet_group_name (str):
        The DB subnet group to use for the RDS instance.

    preferred_maintenance_window (str):
        The preferred maintenance window for the RDS instance.

    db_parameter_group_name (str):
        The DB parameter group to associate with the RDS instance.

    backup_retention_period (int):
        The number of days to retain backups for the RDS instance.

    preferred_backup_window (str):
        The preferred backup window for the RDS instance.

    port (int):
        The port number on which the RDS instance accepts connections.

    multi_az (bool):
        Specifies if the RDS instance is a Multi-AZ deployment.

    engine_version (str):
        The version of the database engine to use for the RDS instance.

    auto_minor_version_upgrade (bool):
        Indicates whether minor engine upgrades are applied automatically to the RDS instance.

    license_model (str):
        The license model for the RDS instance.

    iops (int):
        The amount of provisioned IOPS for the RDS instance.

    option_group_name (str):
        The option group to associate with the RDS instance.

    character_set_name (str):
        The character set to associate with the RDS instance.

    publicly_accessible (bool):
        Specifies if the RDS instance is publicly accessible.

    wait_status (str):
        The status to wait for after creating the RDS instance. Valid values are "available", "modifying", "backing-up".

    tags (list):
        A list of tags to associate with the RDS instance.

    db_cluster_identifier (str):
        The DB cluster identifier for the RDS instance.

    storage_type (str):
        The storage type to use for the RDS instance.

    tde_credential_arn (str):
        The ARN of the TDE credential for the RDS instance.

    tde_credential_password (str):
        The password for the TDE credential.

    storage_encrypted (bool):
        Specifies if the storage for the RDS instance is encrypted.

    kms_key_id (str):
        The KMS key ID to use for the RDS instance.

    domain (str):
        The Active Directory domain to associate with the RDS instance.

    copy_tags_to_snapshot (bool):
        Specifies if tags are copied to snapshots of the RDS instance.

    monitoring_interval (int):
        The interval, in seconds, for Enhanced Monitoring metrics.

    monitoring_role_arn (str):
        The ARN of the IAM role for Enhanced Monitoring.

    domain_iam_role_name (str):
        The name of the IAM role to use for the Active Directory domain.

    region (str):
        AWS region where the RDS instance will be created.

    promotion_tier (int):
        The promotion tier for the RDS instance.

    key (str):
        AWS access key ID.

    keyid (str):
        AWS secret access key.

    profile (str):
        AWS profile to use for the connection.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_rds.create myrds 10 db.t2.micro MySQL \
            sqlusr sqlpassw

    """
    if not allocated_storage:
        raise SaltInvocationError("allocated_storage is required")
    if not db_instance_class:
        raise SaltInvocationError("db_instance_class is required")
    if not engine:
        raise SaltInvocationError("engine is required")
    if not master_username:
        raise SaltInvocationError("master_username is required")
    if not master_user_password:
        raise SaltInvocationError("master_user_password is required")
    if availability_zone and multi_az:
        raise SaltInvocationError(
            "availability_zone and multi_az are mutually exclusive arguments."
        )
    if wait_status:
        wait_stati = ["available", "modifying", "backing-up"]
        if wait_status not in wait_stati:
            raise SaltInvocationError(f"wait_status can be one of: {wait_stati}")
    if vpc_security_groups:
        v_tmp = __salt__["boto3_secgroup.convert_to_group_ids"](
            groups=vpc_security_groups,
            region=region,
            key=key,
            keyid=keyid,
            profile=profile,
        )
        vpc_security_group_ids = vpc_security_group_ids + v_tmp if vpc_security_group_ids else v_tmp

    try:
        conn = _get_conn("rds", region=region, key=key, keyid=keyid, profile=profile)
        if not conn:
            return {"results": bool(conn)}

        kwargs = {}
        boto_params = set(boto3_param_map.keys())
        keys = set(locals().keys())
        tags = _tag_doc(tags)

        for param_key in keys.intersection(boto_params):
            val = locals()[param_key]
            if val is not None:
                mapped = boto3_param_map[param_key]
                kwargs[mapped[0]] = mapped[1](val)

        # Validation doesn't want parameters that are None
        # https://github.com/boto/boto3/issues/400
        kwargs = {k: v for k, v in kwargs.items() if v is not None}

        rds = conn.create_db_instance(**kwargs)

        if not rds:
            return {"created": False}
        if not wait_status:
            return {
                "created": True,
                "message": f"RDS instance {name} created.",
            }

        while True:
            jmespath = "DBInstances[*].DBInstanceStatus"
            status = describe_db_instances(
                name=name,
                jmespath=jmespath,
                region=region,
                key=key,
                keyid=keyid,
                profile=profile,
            )
            if status:
                stat = status[0]
            else:
                # Whoops, something is horribly wrong...
                return {
                    "created": False,
                    "error": (
                        "RDS instance {} should have been created but"
                        " now I can't find it.".format(name)
                    ),
                }
            if stat == wait_status:
                return {
                    "created": True,
                    "message": f"RDS instance {name} created (current status {stat})",
                }
            time.sleep(10)
            log.info("Instance status after 10 seconds is: %s", stat)

    except ClientError as e:
        return {"error": boto3mod.get_error(e)}


def create_read_replica(
    name,
    source_name,
    db_instance_class=None,
    availability_zone=None,
    port=None,
    auto_minor_version_upgrade=None,
    iops=None,
    option_group_name=None,
    publicly_accessible=None,
    tags=None,
    db_subnet_group_name=None,
    storage_type=None,
    copy_tags_to_snapshot=None,
    monitoring_interval=None,
    monitoring_role_arn=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):  # pylint: disable=too-many-arguments,unused-argument
    """
    Create an RDS read replica

    name (str):
        The name of the read replica to create.

    source_name (str):
        The name of the source DB instance from which to create the read replica.

    db_instance_class (str):
        The compute and memory capacity of the read replica.

    availability_zone (str):
        The availability zone in which to create the read replica.

    port (int):
        The port number on which the read replica accepts connections.

    auto_minor_version_upgrade (bool):
        Indicates whether minor engine upgrades are applied automatically to the read replica.

    iops (int):
        The amount of Provisioned IOPS (input/output operations per second) to be initially allocated for the read replica.

    option_group_name (str):
        The option group to associate with the read replica.

    publicly_accessible (bool):
        Specifies whether the read replica is publicly accessible.

    tags (list):
        A list of tags to associate with the read replica.

    db_subnet_group_name (str):
        The DB subnet group to associate with the read replica.

    storage_type (str):
        The storage type to be associated with the read replica.

    copy_tags_to_snapshot (bool):
        Specifies whether to copy tags from the read replica to snapshots of the read replica.

    monitoring_interval (int):
        The interval, in seconds, between points when Enhanced Monitoring metrics are collected for the read replica.

    monitoring_role_arn (str):
        The ARN for the IAM role that permits RDS to send Enhanced Monitoring metrics to CloudWatch Logs for the read replica.

    region (str):
        The AWS region in which to create the read replica.

    key (str):
        The AWS access key ID.

    keyid (str):
        The AWS secret access key.

    profile (str):
        The AWS profile to use.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_rds.create_read_replica \
            replicaname source_name

    """
    res = __salt__["boto3_rds.exists"](source_name, tags, region, key, keyid, profile)
    if not res.get("exists"):
        return {
            "exists": bool(res),
            "message": f"RDS instance source {source_name} does not exists.",
        }

    res = __salt__["boto3_rds.exists"](name, tags, region, key, keyid, profile)
    if res.get("exists"):
        return {
            "exists": bool(res),
            "message": f"RDS replica instance {name} already exists.",
        }

    try:
        conn = _get_conn("rds", region=region, key=key, keyid=keyid, profile=profile)
        kwargs = {}
        for key in ("OptionGroupName", "MonitoringRoleArn"):
            if locals()[key] is not None:
                kwargs[key] = str(locals()[key])

        for key in ("MonitoringInterval", "Iops", "Port"):
            if locals()[key] is not None:
                kwargs[key] = int(locals()[key])

        for key in ("CopyTagsToSnapshot", "AutoMinorVersionUpgrade"):
            if locals()[key] is not None:
                kwargs[key] = bool(locals()[key])

        taglist = _tag_doc(tags)

        rds_replica = conn.create_db_instance_read_replica(
            DBInstanceIdentifier=name,
            SourceDBInstanceIdentifier=source_name,
            DBInstanceClass=db_instance_class,
            AvailabilityZone=availability_zone,
            PubliclyAccessible=publicly_accessible,
            Tags=taglist,
            DBSubnetGroupName=db_subnet_group_name,
            StorageType=storage_type,
            **kwargs,
        )

        return {"exists": bool(rds_replica)}
    except ClientError as e:
        return {"error": boto3mod.get_error(e)}


def create_option_group(
    name,
    engine_name,
    major_engine_version,
    option_group_description,
    tags=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Create an RDS option group

    name (str):
        The name of the option group.

    engine_name (str):
        The name of the database engine.

    major_engine_version (str):
        The major version of the database engine.

    option_group_description (str):
        The description of the option group.

    tags (list):
        A list of tags to associate with the option group.

    region (str):
        The AWS region in which to create the option group.

    key (str):
        The AWS access key ID.

    keyid (str):
        The AWS secret access key.

    profile (str):
        The AWS profile to use.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_rds.create_option_group my-opt-group mysql 5.6 \
            "group description"

    """
    res = __salt__["boto3_rds.option_group_exists"](name, tags, region, key, keyid, profile)
    if res.get("exists"):
        return {"exists": bool(res)}

    try:
        conn = _get_conn("rds", region=region, key=key, keyid=keyid, profile=profile)
        if not conn:
            return {"results": bool(conn)}

        taglist = _tag_doc(tags)
        rds = conn.create_option_group(
            OptionGroupName=name,
            EngineName=engine_name,
            MajorEngineVersion=major_engine_version,
            OptionGroupDescription=option_group_description,
            Tags=taglist,
        )

        return {"exists": bool(rds)}
    except ClientError as e:
        return {"error": boto3mod.get_error(e)}


def create_parameter_group(
    name,
    db_parameter_group_family,
    description,
    tags=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Create an RDS parameter group

    name (str):
        The name of the parameter group.

    db_parameter_group_family (str):
        The database engine family for the parameter group.

    description (str):
        The description of the parameter group.

    tags (list):
        A list of tags to associate with the parameter group.

    region (str):
        The AWS region in which to create the parameter group.

    key (str):
        The AWS access key ID.

    keyid (str):
        The AWS secret access key.

    profile (str):
        The AWS profile to use.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_rds.create_parameter_group my-param-group \
            mysql5.6 "group description"

    """
    res = __salt__["boto3_rds.parameter_group_exists"](name, tags, region, key, keyid, profile)
    if res.get("exists"):
        return {"exists": bool(res)}

    try:
        conn = _get_conn("rds", region=region, key=key, keyid=keyid, profile=profile)
        if not conn:
            return {"results": bool(conn)}

        taglist = _tag_doc(tags)
        rds = conn.create_db_parameter_group(
            DBParameterGroupName=name,
            DBParameterGroupFamily=db_parameter_group_family,
            Description=description,
            Tags=taglist,
        )
        if not rds:
            return {
                "created": False,
                "message": f"Failed to create RDS parameter group {name}",
            }

        return {
            "exists": bool(rds),
            "message": f"Created RDS parameter group {name}",
        }
    except ClientError as e:
        return {"error": boto3mod.get_error(e)}


def create_subnet_group(
    name,
    description,
    subnet_ids,
    tags=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Create an RDS subnet group

    name (str):
        The name of the subnet group.

    description (str):
        The description of the subnet group.

    subnet_ids (list):
        A list of subnet IDs to include in the subnet group.

    tags (list):
        A list of tags to associate with the subnet group.

    region (str):
        The AWS region in which to create the subnet group.

    key (str):
        The AWS access key ID.

    keyid (str):
        The AWS secret access key.

    profile (str):
        The AWS profile to use.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_rds.create_subnet_group my-subnet-group \
            "group description" '[subnet-12345678, subnet-87654321]' \
            region=us-east-1

    """
    res = __salt__["boto3_rds.subnet_group_exists"](name, tags, region, key, keyid, profile)
    if res.get("exists"):
        return {"exists": bool(res)}

    try:
        conn = _get_conn("rds", region=region, key=key, keyid=keyid, profile=profile)
        if not conn:
            return {"results": bool(conn)}

        taglist = _tag_doc(tags)
        rds = conn.create_db_subnet_group(
            DBSubnetGroupName=name,
            DBSubnetGroupDescription=description,
            SubnetIds=subnet_ids,
            Tags=taglist,
        )

        return {"created": bool(rds)}
    except ClientError as e:
        return {"error": boto3mod.get_error(e)}


def update_parameter_group(
    name,
    parameters,
    apply_method="pending-reboot",
    tags=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Update an RDS parameter group.

    name (str):
        The name of the parameter group.

    parameters (dict):
        A dictionary of parameters to update in the parameter group.

    apply_method (str):
        The method to apply the parameter changes. Valid values are "immediate" and "pending-reboot".

    tags (list):
        A list of tags to associate with the parameter group.

    region (str):
        The AWS region in which the parameter group exists.

    key (str):
        The AWS access key ID.

    keyid (str):
        The AWS secret access key.

    profile (str):
        The AWS profile to use.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_rds.update_parameter_group my-param-group \
            parameters='{"back_log":1, "binlog_cache_size":4096}' \
            region=us-east-1
    """

    res = __salt__["boto3_rds.parameter_group_exists"](name, tags, region, key, keyid, profile)
    if not res.get("exists"):
        return {
            "exists": bool(res),
            "message": f"RDS parameter group {name} does not exist.",
        }

    param_list = []
    for key, value in parameters.items():
        item = odict.OrderedDict()
        item.update({"ParameterName": key})
        item.update({"ApplyMethod": apply_method})
        if isinstance(value, bool):
            item.update({"ParameterValue": "on" if value else "off"})
        else:
            item.update({"ParameterValue": str(value)})
        param_list.append(item)

    if not param_list:
        return {"results": False}

    try:
        conn = _get_conn("rds", region=region, key=key, keyid=keyid, profile=profile)
        if not conn:
            return {"results": bool(conn)}

        res = conn.modify_db_parameter_group(DBParameterGroupName=name, Parameters=param_list)
        return {"results": bool(res)}
    except ClientError as e:
        return {"error": boto3mod.get_error(e)}


def describe(name, tags=None, region=None, key=None, keyid=None, profile=None):
    """
    Return RDS instance details.

    name (str):
        The name of the RDS instance.

    tags (list):
        A list of tags to filter the RDS instances.

    region (str):
        The AWS region in which the RDS instance exists.

    key (str):
        The AWS access key ID.

    keyid (str):
        The AWS secret access key.

    profile (str):
        The AWS profile to use.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_rds.describe myrds

    """
    res = __salt__["boto3_rds.exists"](name, tags, region, key, keyid, profile)
    if not res.get("exists"):
        return {
            "exists": bool(res),
            "message": f"RDS instance {name} does not exist.",
        }

    try:
        conn = _get_conn("rds", region=region, key=key, keyid=keyid, profile=profile)
        if not conn:
            return {"results": bool(conn)}

        rds = conn.describe_db_instances(DBInstanceIdentifier=name)
        rds = [i for i in rds.get("DBInstances", []) if i.get("DBInstanceIdentifier") == name].pop(
            0
        )

        if rds:
            keys = (
                "DBInstanceIdentifier",
                "DBInstanceClass",
                "Engine",
                "DBInstanceStatus",
                "DBName",
                "AllocatedStorage",
                "PreferredBackupWindow",
                "BackupRetentionPeriod",
                "AvailabilityZone",
                "PreferredMaintenanceWindow",
                "LatestRestorableTime",
                "EngineVersion",
                "AutoMinorVersionUpgrade",
                "LicenseModel",
                "Iops",
                "CharacterSetName",
                "PubliclyAccessible",
                "StorageType",
                "TdeCredentialArn",
                "DBInstancePort",
                "DBClusterIdentifier",
                "StorageEncrypted",
                "KmsKeyId",
                "DbiResourceId",
                "CACertificateIdentifier",
                "CopyTagsToSnapshot",
                "MonitoringInterval",
                "MonitoringRoleArn",
                "PromotionTier",
                "DomainMemberships",
            )
            return {"rds": {k: rds.get(k) for k in keys}}
        else:
            return {"rds": None}
    except ClientError as e:
        return {"error": boto3mod.get_error(e)}
    except IndexError:
        return {"rds": None}


def describe_db_instances(
    name=None,
    filters=None,
    jmespath="DBInstances",
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Return a detailed listing of some, or all, DB Instances visible in the
    current scope.  Arbitrary subelements or subsections of the returned dataset
    can be selected by passing in a valid JMSEPath filter as well.

    name (str):
        The name of the DB instance to describe. If not provided, all DB instances will be described.

    filters (list):
        A list of filters to apply to the DB instances.

    jmespath (str):
        A JMESPath expression to filter the returned data.

    region (str):
        The AWS region in which the DB instance exists.

    key (str):
        The AWS access key ID.

    keyid (str):
        The AWS secret access key.

    profile (str):
        The AWS profile to use.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_rds.describe_db_instances \
            jmespath='DBInstances[*].DBInstanceIdentifier'

    """
    conn = _get_conn("rds", region=region, key=key, keyid=keyid, profile=profile)
    pag = conn.get_paginator("describe_db_instances")
    args = {}
    if name:
        args["DBInstanceIdentifier"] = name
    if filters:
        args["Filters"] = filters
    pit = pag.paginate(**args)
    pit = pit.search(jmespath) if jmespath else pit
    try:
        return list(pit)
    except ClientError as e:
        code = getattr(e, "response", {}).get("Error", {}).get("Code")
        if code != "DBInstanceNotFound":
            log.error(boto3mod.get_error(e))
    return []


def describe_db_subnet_groups(
    name=None,
    filters=None,
    jmespath="DBSubnetGroups",
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Return a detailed listing of some, or all, DB Subnet Groups visible in the
    current scope.  Arbitrary subelements or subsections of the returned dataset
    can be selected by passing in a valid JMSEPath filter as well.

    name (str):
        The name of the DB subnet group to describe. If not provided, all DB subnet groups will be described.

    filters (list):
        A list of filters to apply to the DB subnet groups.

    jmespath (str):
        A JMESPath expression to filter the returned data.

    region (str):
        The AWS region in which the DB subnet group exists.

    key (str):
        The AWS access key ID.

    keyid (str):
        The AWS secret access key.

    profile (str):
        The AWS profile to use.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_rds.describe_db_subnet_groups

    """
    conn = _get_conn("rds", region=region, key=key, keyid=keyid, profile=profile)
    pag = conn.get_paginator("describe_db_subnet_groups")
    args = {}
    if name:
        args["DBSubnetGroupName"] = name
    if filters:
        args["Filters"] = filters
    pit = pag.paginate(**args)
    pit = pit.search(jmespath) if jmespath else pit
    return list(pit)


def get_endpoint(name, tags=None, region=None, key=None, keyid=None, profile=None):
    """
    Return the endpoint of an RDS instance.

    name (str):
        The name of the RDS instance.

    tags (list):
        A list of tags to filter the RDS instances.

    region (str):
        The AWS region in which the RDS instance exists.

    key (str):
        The AWS access key ID.

    keyid (str):
        The AWS secret access key.

    profile (str):
        The AWS profile to use.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_rds.get_endpoint myrds

    """
    endpoint = False
    res = __salt__["boto3_rds.exists"](name, tags, region, key, keyid, profile)
    if res.get("exists"):
        try:
            conn = _get_conn("rds", region=region, key=key, keyid=keyid, profile=profile)
            if conn:
                rds = conn.describe_db_instances(DBInstanceIdentifier=name)

                if rds and "Endpoint" in rds["DBInstances"][0]:
                    endpoint = rds["DBInstances"][0]["Endpoint"]["Address"]
                    return endpoint

        except ClientError as e:
            return {"error": boto3mod.get_error(e)}

    return endpoint


def delete(
    name,
    skip_final_snapshot=None,
    final_db_snapshot_identifier=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
    tags=None,
    wait_for_deletion=True,
    timeout=180,
):
    """
    Delete an RDS instance.

    name (str):
        The name of the RDS instance to delete.

    skip_final_snapshot (bool):
        Whether to skip the creation of a final DB snapshot before deletion.

    final_db_snapshot_identifier (str):
        The identifier for the final DB snapshot if skip_final_snapshot is False.

    region (str):
        The AWS region in which the RDS instance exists.

    key (str):
        The AWS access key ID.

    keyid (str):
        The AWS secret access key.

    profile (str):
        The AWS profile to use.

    tags (list):
        A list of tags to filter the RDS instances.

    wait_for_deletion (bool):
        Whether to wait for the RDS instance to be completely deleted.

    timeout (int):
        The maximum time to wait for deletion, in seconds.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_rds.delete myrds skip_final_snapshot=True \
            region=us-east-1
    """
    if timeout == 180 and not skip_final_snapshot:
        timeout = 420

    if not skip_final_snapshot and not final_db_snapshot_identifier:
        raise SaltInvocationError(
            "At least one of the following must"
            " be specified: skip_final_snapshot"
            " final_db_snapshot_identifier"
        )

    try:
        conn = _get_conn("rds", region=region, key=key, keyid=keyid, profile=profile)
        if not conn:
            return {"deleted": bool(conn)}

        kwargs = {}
        if locals()["skip_final_snapshot"] is not None:
            kwargs["SkipFinalSnapshot"] = bool(locals()["skip_final_snapshot"])

        if locals()["final_db_snapshot_identifier"] is not None:
            kwargs["FinalDBSnapshotIdentifier"] = str(locals()["final_db_snapshot_identifier"])

        res = conn.delete_db_instance(DBInstanceIdentifier=name, **kwargs)

        if not wait_for_deletion:
            return {
                "deleted": bool(res),
                "message": f"Deleted RDS instance {name}.",
            }

        start_time = time.time()
        while True:
            res = __salt__["boto3_rds.exists"](
                name=name,
                tags=tags,
                region=region,
                key=key,
                keyid=keyid,
                profile=profile,
            )
            if not res.get("exists"):
                return {
                    "deleted": bool(res),
                    "message": f"Deleted RDS instance {name} completely.",
                }

            if time.time() - start_time > timeout:
                raise SaltInvocationError(
                    "RDS instance {} has not been "
                    "deleted completely after {} "
                    "seconds".format(name, timeout)
                )
            log.info(
                "Waiting up to %s seconds for RDS instance %s to be deleted.",
                timeout,
                name,
            )
            time.sleep(10)
    except ClientError as e:
        return {"error": boto3mod.get_error(e)}


def delete_option_group(name, region=None, key=None, keyid=None, profile=None):
    """
    Delete an RDS option group.

    name (str):
        The name of the RDS option group to delete.

    region (str):
        The AWS region in which the RDS option group exists.

    key (str):
        The AWS access key ID.

    keyid (str):
        The AWS secret access key.

    profile (str):
        The AWS profile to use.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_rds.delete_option_group my-opt-group \
            region=us-east-1
    """
    try:
        conn = _get_conn("rds", region=region, key=key, keyid=keyid, profile=profile)
        if not conn:
            return {"deleted": bool(conn)}

        res = conn.delete_option_group(OptionGroupName=name)
        if not res:
            return {
                "deleted": bool(res),
                "message": f"Failed to delete RDS option group {name}.",
            }

        return {
            "deleted": bool(res),
            "message": f"Deleted RDS option group {name}.",
        }
    except ClientError as e:
        return {"error": boto3mod.get_error(e)}


def delete_parameter_group(name, region=None, key=None, keyid=None, profile=None):
    """
    Delete an RDS parameter group.

    name (str):
        The name of the RDS parameter group to delete.

    region (str):
        The AWS region in which the RDS parameter group exists.

    key (str):
        The AWS access key ID.

    keyid (str):
        The AWS secret access key.

    profile (str):
        The AWS profile to use.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_rds.delete_parameter_group my-param-group \
            region=us-east-1
    """
    try:
        conn = _get_conn("rds", region=region, key=key, keyid=keyid, profile=profile)
        if not conn:
            return {"results": bool(conn)}

        r = conn.delete_db_parameter_group(DBParameterGroupName=name)
        return {
            "deleted": bool(r),
            "message": f"Deleted RDS parameter group {name}.",
        }
    except ClientError as e:
        return {"error": boto3mod.get_error(e)}


def delete_subnet_group(name, region=None, key=None, keyid=None, profile=None):
    """
    Delete an RDS subnet group.

    name (str):
        The name of the RDS subnet group to delete.

    region (str):
        The AWS region in which the RDS subnet group exists.

    key (str):
        The AWS access key ID.

    keyid (str):
        The AWS secret access key.

    profile (str):
        The AWS profile to use.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_rds.delete_subnet_group my-subnet-group \
            region=us-east-1
    """
    try:
        conn = _get_conn("rds", region=region, key=key, keyid=keyid, profile=profile)
        if not conn:
            return {"results": bool(conn)}

        r = conn.delete_db_subnet_group(DBSubnetGroupName=name)
        return {
            "deleted": bool(r),
            "message": f"Deleted RDS subnet group {name}.",
        }
    except ClientError as e:
        return {"error": boto3mod.get_error(e)}


def describe_parameter_group(
    name,
    Filters=None,
    MaxRecords=None,
    Marker=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):  # pylint: disable=unused-argument
    """
    Returns a list of `DBParameterGroup` descriptions.

    name (str):
        The name of the RDS parameter group to describe.

    Filters (list, optional):
        A list of filters to apply to the description.

    MaxRecords (int, optional):
        The maximum number of records to return.

    Marker (str, optional):
        The marker for pagination.

    region (str):
        The AWS region in which the RDS parameter group exists.

    key (str):
        The AWS access key ID.

    keyid (str):
        The AWS secret access key.

    profile (str):
        The AWS profile to use.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_rds.describe_parameter_group \
            name-of-parameter-group region=us-east-1

    """
    res = __salt__["boto3_rds.parameter_group_exists"](
        name, tags=None, region=region, key=key, keyid=keyid, profile=profile
    )
    if not res.get("exists"):
        return {"exists": bool(res)}

    try:
        conn = _get_conn("rds", region=region, key=key, keyid=keyid, profile=profile)
        if not conn:
            return {"results": bool(conn)}

        kwargs = {}
        for key in ("Marker", "Filters"):
            if locals()[key] is not None:
                kwargs[key] = str(locals()[key])

        if locals()["MaxRecords"] is not None:
            kwargs["MaxRecords"] = int(locals()["MaxRecords"])

        info = conn.describe_db_parameter_groups(DBParameterGroupName=name, **kwargs)

        if not info:
            return {
                "results": bool(info),
                "message": f"Failed to get RDS description for group {name}.",
            }

        return {
            "results": bool(info),
            "message": f"Got RDS descrition for group {name}.",
        }
    except ClientError as e:
        return {"error": boto3mod.get_error(e)}


def describe_parameters(
    name,
    Source=None,
    MaxRecords=None,
    Marker=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):  # pylint: disable=unused-argument
    """
    Returns a list of `DBParameterGroup` parameters.

    name (str):
        The name of the RDS parameter group whose parameters are to be described.

    Source (str, optional):
        The source of the parameters to return. Valid values are `user`, `system`, or `engine-default`.

    MaxRecords (int, optional):
        The maximum number of records to return.

    Marker (str, optional):
        The marker for pagination.

    region (str):
        The AWS region in which the RDS parameter group exists.

    key (str):
        The AWS access key ID.

    keyid (str):
        The AWS secret access key.

    profile (str):
        The AWS profile to use.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_rds.describe_parameters name-of-parameter-group

    """
    res = __salt__["boto3_rds.parameter_group_exists"](
        name, tags=None, region=region, key=key, keyid=keyid, profile=profile
    )
    if not res.get("exists"):
        return {
            "result": False,
            "message": f"Parameter group {name} does not exist",
        }

    try:
        conn = _get_conn("rds", region=region, key=key, keyid=keyid, profile=profile)
        if not conn:
            return {
                "result": False,
                "message": "Could not establish a connection to RDS",
            }

        kwargs = {}
        kwargs.update({"DBParameterGroupName": name})
        for key in ("Marker", "Source"):
            if locals()[key] is not None:
                kwargs[key] = str(locals()[key])

        if locals()["MaxRecords"] is not None:
            kwargs["MaxRecords"] = int(locals()["MaxRecords"])

        pag = conn.get_paginator("describe_db_parameters")
        pit = pag.paginate(**kwargs)

        keys = [
            "ParameterName",
            "ParameterValue",
            "Description",
            "Source",
            "ApplyType",
            "DataType",
            "AllowedValues",
            "IsModifieable",
            "MinimumEngineVersion",
            "ApplyMethod",
        ]

        parameters = odict.OrderedDict()
        ret = {"result": True}

        for p in pit:
            for result in p["Parameters"]:
                data = odict.OrderedDict()
                for k in keys:
                    data[k] = result.get(k)

                parameters[result.get("ParameterName")] = data

        ret["parameters"] = parameters
        return ret
    except ClientError as e:
        return {"error": boto3mod.get_error(e)}


def modify_db_instance(
    name,
    allocated_storage=None,
    allow_major_version_upgrade=None,
    apply_immediately=None,
    auto_minor_version_upgrade=None,
    backup_retention_period=None,
    ca_certificate_identifier=None,
    character_set_name=None,
    copy_tags_to_snapshot=None,
    db_cluster_identifier=None,
    db_instance_class=None,
    db_name=None,
    db_parameter_group_name=None,
    db_port_number=None,
    db_security_groups=None,
    db_subnet_group_name=None,
    domain=None,
    domain_iam_role_name=None,
    engine_version=None,
    iops=None,
    kms_key_id=None,
    license_model=None,
    master_user_password=None,
    monitoring_interval=None,
    monitoring_role_arn=None,
    multi_az=None,
    new_db_instance_identifier=None,
    option_group_name=None,
    preferred_backup_window=None,
    preferred_maintenance_window=None,
    promotion_tier=None,
    publicly_accessible=None,
    storage_encrypted=None,
    storage_type=None,
    tde_credential_arn=None,
    tde_credential_password=None,
    vpc_security_group_ids=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):  # pylint: disable=too-many-arguments,unused-argument
    """
    Modify settings for a DB instance.

    name (str):
        The name of the RDS DB instance to modify.

    allocated_storage (int, optional):
        The new allocated storage size for the DB instance, in gigabytes.

    allow_major_version_upgrade (bool, optional):
        Indicates whether major version upgrades are allowed for the DB instance.

    apply_immediately (bool, optional):
        Specifies whether the modifications should be applied immediately.

    auto_minor_version_upgrade (bool, optional):
        Indicates whether minor version upgrades are applied automatically to the DB instance.

    backup_retention_period (int, optional):
        The number of days to retain backups for the DB instance.

    ca_certificate_identifier (str, optional):
        The identifier of the CA certificate for the DB instance.

    character_set_name (str, optional):
        The character set name for the DB instance.

    copy_tags_to_snapshot (bool, optional):
        Indicates whether to copy tags to the DB instance snapshot.

    db_cluster_identifier (str, optional):
        The DB cluster identifier to associate with the DB instance.

    db_instance_class (str, optional):
        The compute and memory capacity of the DB instance, for example, db.m4.large.

    db_name (str, optional):
        The name of the database to create when the DB instance is created.

    db_parameter_group_name (str, optional):
        The name of the DB parameter group to associate with the DB instance.

    db_port_number (int, optional):
        The port number on which the DB instance accepts connections.

    db_security_groups (list, optional):
        A list of DB security groups to associate with the DB instance.

    db_subnet_group_name (str, optional):
        The name of the DB subnet group to associate with the DB instance.

    domain (str, optional):
        The Active Directory domain to associate with the DB instance.

    domain_iam_role_name (str, optional):
        The name of the IAM role to associate with the Active Directory domain for the DB instance.

    engine_version (str, optional):
        The version of the database engine to use for the DB instance.

    iops (int, optional):
        The amount of provisioned IOPS for the DB instance.

    kms_key_id (str, optional):
        The AWS KMS key identifier for the DB instance.

    license_model (str, optional):
        The license model for the DB instance.

    master_user_password (str, optional):
        The password for the master database user.

    monitoring_interval (int, optional):
        The interval, in seconds, between points when Enhanced Monitoring metrics are collected for the DB instance.

    monitoring_role_arn (str, optional):
        The ARN for the IAM role that permits RDS to send Enhanced Monitoring metrics to CloudWatch Logs.

    multi_az (bool, optional):
        Indicates whether the DB instance is a Multi-AZ deployment.

    new_db_instance_identifier (str, optional):
        The new DB instance identifier for the DB instance.

    option_group_name (str, optional):
        The name of the option group to associate with the DB instance.

    preferred_backup_window (str, optional):
        The daily time range during which automated backups are created for the DB instance.

    preferred_maintenance_window (str, optional):
        The weekly time range during which system maintenance can occur for the DB instance.

    promotion_tier (int, optional):
        The promotion tier of the DB instance.

    publicly_accessible (bool, optional):
        Indicates whether the DB instance is publicly accessible.

    storage_encrypted (bool, optional):
        Indicates whether the DB instance is encrypted.

    storage_type (str, optional):
        The storage type for the DB instance.

    tde_credential_arn (str, optional):
        The ARN for the IAM role that permits RDS to use TDE encryption for the DB instance.

    tde_credential_password (str, optional):
        The password for the TDE encryption credential.

    vpc_security_group_ids (list, optional):
        A list of VPC security group IDs to associate with the DB instance.

    region (str, optional):
        The AWS region in which the RDS DB instance exists.

    key (str, optional):
        The AWS access key ID.

    keyid (str, optional):
        The AWS secret access key.

    profile (str, optional):
        The AWS profile to use.

    CLI Example:

    .. code-block:: bash

        salt myminion boto3_rds.modify_db_instance name-of-db-instance \
            db_instance_identifier region=us-east-1

    """
    res = __salt__["boto3_rds.exists"](
        name, tags=None, region=region, key=key, keyid=keyid, profile=profile
    )
    if not res.get("exists"):
        return {
            "modified": False,
            "message": f"RDS db instance {name} does not exist.",
        }

    try:
        conn = _get_conn("rds", region=region, key=key, keyid=keyid, profile=profile)
        if not conn:
            return {"modified": False}

        kwargs = {}
        excluded = {"name"}
        boto_params = set(boto3_param_map.keys())
        keys = set(locals().keys())
        for key in keys.intersection(boto_params).difference(excluded):
            val = locals()[key]
            if val is not None:
                mapped = boto3_param_map[key]
                kwargs[mapped[0]] = mapped[1](val)

        info = conn.modify_db_instance(DBInstanceIdentifier=name, **kwargs)

        if not info:
            return {
                "modified": bool(info),
                "message": f"Failed to modify RDS db instance {name}.",
            }

        return {
            "modified": bool(info),
            "message": f"Modified RDS db instance {name}.",
            "results": dict(info),
        }
    except ClientError as e:
        return {"error": boto3mod.get_error(e)}


def _tag_doc(tags):
    taglist = []
    if tags is not None:
        for k, v in tags.items():
            if str(k).startswith("__"):
                continue
            taglist.append({"Key": str(k), "Value": str(v)})
    return taglist
