"""
Manage RDSs using boto3.
========================

    Renamed from ``boto_rds`` to ``boto3_rds`` and updated to call the
    refactored ``boto3_rds`` execution module.

Create and destroy RDS instances. Be aware that this interacts with Amazon's
services, and so may incur charges.

:depends:
  - boto3 >= 1.28.0
  - botocore >= 1.31.0

This module uses ``boto3``, which can be installed via package, or pip.

This module accepts explicit rds credentials but can also utilize
IAM roles assigned to the instance through Instance Profiles. Dynamic
credentials are then automatically obtained from AWS API and no further
configuration is necessary. More information available `here
<http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html>`_.

If IAM roles are not used you need to specify them either in a pillar file or
in the minion's config file:

.. code-block:: yaml

    rds.keyid: GKTADJGHEIQSXMKKRBJ08H
    rds.key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs

It's also possible to specify ``key``, ``keyid`` and ``region`` via a profile,
either passed in as a dict, or as a string to pull from pillars or minion
config:

.. code-block:: yaml

    myprofile:
        keyid: GKTADJGHEIQSXMKKRBJ08H
        key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs
            region: us-east-1

.. code-block:: yaml

    Ensure myrds RDS exists:
      boto3_rds.present:
        - name: myrds
        - allocated_storage: 5
        - storage_type: standard
        - db_instance_class: db.t2.micro
        - engine: MySQL
        - master_username: myuser
        - master_user_password: mypass
        - region: us-east-1
        - keyid: GKTADJGHEIQSXMKKRBJ08H
        - key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs
        - tags:
            key: value

.. code-block:: yaml

    Ensure parameter group exists:
        create-parameter-group:
          boto3_rds.parameter_present:
            - name: myparametergroup
            - db_parameter_group_family: mysql5.6
            - description: "parameter group family"
            - parameters:
              - binlog_cache_size: 32768
              - binlog_checksum: CRC32
            - region: eu-west-1

.. versionadded:: 1.0.0
"""

import json
import logging
import os

import salt.utils.data
from salt.exceptions import SaltInvocationError

log = logging.getLogger(__name__)


def __virtual__():
    """
    Only load if boto3_rds module is available.
    """
    if "boto3_rds.exists" in __salt__:
        return "boto3_rds"
    return (
        False,
        "The boto3_rds state module could not be loaded: boto3_rds execution module is unavailable.",
    )


def present(
    name,
    allocated_storage,
    db_instance_class,
    engine,
    master_username,
    master_user_password=None,
    db_name=None,
    storage_type=None,
    db_security_groups=None,
    vpc_security_group_ids=None,
    vpc_security_groups=None,
    availability_zone=None,
    db_subnet_group_name=None,
    preferred_maintenance_window=None,
    db_parameter_group_name=None,
    db_cluster_identifier=None,
    tde_credential_arn=None,
    tde_credential_password=None,
    storage_encrypted=None,
    kms_keyid=None,
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
    copy_tags_to_snapshot=None,
    region=None,
    domain=None,
    key=None,
    keyid=None,
    monitoring_interval=None,
    monitoring_role_arn=None,
    domain_iam_role_name=None,
    promotion_tier=None,
    profile=None,
    manage_master_user_password=None,
    master_user_secret_kms_key_id=None,
    apply_immediately=False,
):  # pylint: disable=too-many-arguments
    """
    Ensure RDS instance exists.

    name (str):
        Name of the RDS state definition.

    allocated_storage (int):
        The amount of storage (in gigabytes) to be initially allocated for the
        database instance.

    db_instance_class (str):
        The compute and memory capacity of the Amazon RDS DB instance.

    engine (str):
        The name of the database engine to be used for this instance. Supported
        engine types are: MySQL, mariadb, oracle-se1, oracle-se, oracle-ee, sqlserver-ee,
        sqlserver-se, sqlserver-ex, sqlserver-web, postgres and aurora. For more
        information, please see the ``engine`` argument in the Boto3 RDS
        `create_db_instance`_ documentation.

    master_username (str):
        The name of master user for the client DB instance.

    master_user_password (str):
        The password for the master database user. Required unless
        ``manage_master_user_password`` is true. Can be any printable ASCII
        character except "/", '"', or "@".

    manage_master_user_password (bool, optional):
        If true, RDS generates and manages the master password in Secrets
        Manager. Mutually exclusive with ``master_user_password``. On an
        existing instance, this enables service-managed credentials.

    master_user_secret_kms_key_id (str, optional):
        KMS key used to encrypt an RDS-managed master secret. Only valid when
        ``manage_master_user_password`` is true. AWS does not allow changing
        this key after service-managed credentials are enabled.

    apply_immediately (bool, optional):
        Whether to immediately apply the transition on an existing instance.
        Defaults to ``False``.

    db_name (str, optional):
        The meaning of this parameter differs according to the database engine you use.
        See the Boto3 RDS documentation to determine the appropriate value for your configuration.
        https://docs.aws.amazon.com/boto3/latest/reference/services/rds/client/create_db_instance.html

    storage_type (str, optional):
        Specifies the storage type to be associated with the DB instance.
        Options are standard, gp2 and io1. If you specify io1, you must also include
        a value for the Iops parameter.

    db_security_groups (list, optional):
        A list of DB security groups to associate with this DB instance.

    vpc_security_group_ids (list, optional):
        A list of EC2 VPC security group IDs to associate with this DB instance.

    vpc_security_groups (list, optional):
        A list of EC2 VPC security groups (IDs or Name tags) to associate with this DB instance.

    availability_zone (str, optional):
        The EC2 Availability Zone that the database instance will be created
        in.

    db_subnet_group_name (str, optional):
        A DB subnet group to associate with this DB instance.

    preferred_maintenance_window (str, optional):
        The weekly time range (in UTC) during which system maintenance can
        occur.

    db_parameter_group_name (str, optional):
        A DB parameter group to associate with this DB instance.

    db_cluster_identifier (str, optional):
        If the DB instance is a member of a DB cluster, contains the name of
        the DB cluster that the DB instance is a member of.

    tde_credential_arn (str, optional):
        The ARN from the Key Store with which the instance is associated for
        TDE encryption.

    tde_credential_password (str, optional):
        The password to use for TDE encryption if an encryption key is not used.

    storage_encrypted (bool, optional):
        Specifies whether the DB instance is encrypted.

    kms_keyid (str, optional):
        If storage_encrypted is true, the KMS key identifier for the encrypted
        DB instance.

    backup_retention_period (int, optional):
        The number of days for which automated backups are retained.

    preferred_backup_window (str, optional):
        The daily time range during which automated backups are created if
        automated backups are enabled.

    port (int, optional):
        The port number on which the database accepts connections.

    multi_az (bool, optional):
        Specifies if the DB instance is a Multi-AZ deployment. You cannot set
        the AvailabilityZone parameter if the MultiAZ parameter is set to true.

    engine_version (str, optional):
        The version number of the database engine to use.

    auto_minor_version_upgrade (bool, optional):
        Indicates that minor engine upgrades will be applied automatically to
        the DB instance during the maintenance window.

    license_model (str, optional):
        License model information for this DB instance.

    iops (int, optional):
        The amount of Provisioned IOPS (input/output operations per second) to
        be initially allocated for the DB instance.

    option_group_name (str, optional):
        Indicates that the DB instance should be associated with the specified
        option group.

    character_set_name (str, optional):
        For supported engines, indicates that the DB instance should be
        associated with the specified CharacterSet.

    publicly_accessible (bool, optional):
        Specifies the accessibility options for the DB instance. A value of
        true specifies an Internet-facing instance with a publicly resolvable
        DNS name, which resolves to a public IP address. A value of false
        specifies an internal instance with a DNS name that resolves to a
        private IP address.

    wait_status (str, optional):
        Wait for the RDS instance to reach a desired status before finishing
        the state. Available states: available, modifying, backing-up

    tags (dict, optional):
        A dict of tags.

    copy_tags_to_snapshot (bool, optional):
        Specifies whether tags are copied from the DB instance to snapshots of
        the DB instance.

    region (str, optional):
        Region to connect to.

    domain (str, optional):
        The identifier of the Active Directory Domain.

    key (str, optional):
        AWS secret key to be used.

    keyid (str, optional):
        AWS access key to be used.

    monitoring_interval (int, optional):
        The interval, in seconds, between points when Enhanced Monitoring
        metrics are collected for the DB instance.

    monitoring_role_arn (str, optional):
        The ARN for the IAM role that permits RDS to send Enhanced Monitoring
        metrics to CloudWatch Logs.

    domain_iam_role_name (str, optional):
        Specify the name of the IAM role to be used when making API calls to
        the Directory Service.

    promotion_tier (int, optional):
        A value that specifies the order in which an Aurora Replica is
        promoted to the primary instance after a failure of the existing
        primary instance. For more information, see Fault Tolerance for an
        Aurora DB Cluster .

    profile (dict or str, optional):
        A dict with region, key and keyid, or a pillar key (string) that
        contains a dict with region, key and keyid.

    .. _create_db_instance: https://docs.aws.amazon.com/boto3/latest/reference/services/rds/client/create_db_instance.html

    Example:

    .. code-block:: yaml

        ensure-present:
          boto3_rds.present:
            - name: example
            - allocated_storage: 20
            - engine: mysql
            - master_username: admin
            - manage_master_user_password: True
            - db_name: exampledb
    """
    if manage_master_user_password and master_user_password is not None:
        raise SaltInvocationError(
            "master_user_password and manage_master_user_password are mutually exclusive"
        )
    if manage_master_user_password is not True and not master_user_password:
        raise SaltInvocationError("master_user_password is required")
    if master_user_secret_kms_key_id and not manage_master_user_password:
        raise SaltInvocationError(
            "master_user_secret_kms_key_id requires manage_master_user_password"
        )

    ret = {"name": name, "result": True, "comment": "", "changes": {}}

    r = __salt__["boto3_rds.exists"](name, tags, region, key, keyid, profile)

    if not r.get("exists"):
        if __opts__["test"]:
            ret["comment"] = f"RDS instance {name} would be created."
            ret["result"] = None
            return ret

        r = __salt__["boto3_rds.create"](
            name,
            allocated_storage,
            db_instance_class,
            engine,
            master_username,
            master_user_password,
            db_name,
            db_security_groups,
            vpc_security_group_ids,
            vpc_security_groups,
            availability_zone,
            db_subnet_group_name,
            preferred_maintenance_window,
            db_parameter_group_name,
            backup_retention_period,
            preferred_backup_window,
            port,
            multi_az,
            engine_version,
            auto_minor_version_upgrade,
            license_model,
            iops,
            option_group_name,
            character_set_name,
            publicly_accessible,
            wait_status,
            tags,
            db_cluster_identifier,
            storage_type,
            tde_credential_arn,
            tde_credential_password,
            storage_encrypted,
            kms_keyid,
            domain,
            copy_tags_to_snapshot,
            monitoring_interval,
            monitoring_role_arn,
            domain_iam_role_name,
            region,
            promotion_tier,
            key,
            keyid,
            profile,
            manage_master_user_password=manage_master_user_password,
            master_user_secret_kms_key_id=master_user_secret_kms_key_id,
        )

        if not r.get("created"):
            ret["result"] = False
            ret["comment"] = "Failed to create RDS instance {}.".format(r["error"]["message"])
            return ret

        ret["changes"]["old"] = {"instance": None}
        ret["changes"]["new"] = {
            "instance": __salt__["boto3_rds.describe_db_instances"](
                name=name,
                jmespath="DBInstances[0]",
                region=region,
                key=key,
                keyid=keyid,
                profile=profile,
            )
        }
        ret["comment"] = f"RDS instance {name} created."
    elif manage_master_user_password is not None:
        return master_user_password_managed(
            name,
            manage=manage_master_user_password,
            kms_key_id=master_user_secret_kms_key_id,
            master_user_password=master_user_password if not manage_master_user_password else None,
            apply_immediately=apply_immediately,
            region=region,
            key=key,
            keyid=keyid,
            profile=profile,
        )
    else:
        ret["comment"] = f"RDS instance {name} exists."
    return ret


def master_user_password_managed(
    name,
    manage=True,
    kms_key_id=None,
    master_user_password=None,
    apply_immediately=False,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """Enable or disable RDS-managed master credentials in Secrets Manager.

    When ``manage=True`` (default), RDS generates and manages the master
    password via Secrets Manager. When ``manage=False``, the instance is
    transitioned back to a manually-supplied ``master_user_password``.

    This is an explicit transition for an existing DB instance. It changes the
    master password and should not be applied to application credentials.

    Example:

    .. code-block:: yaml

        manage_rds_master_credentials:
          boto3_rds.master_user_password_managed:
            - name: dhs-dso-dev-rds
            - kms_key_id: arn:aws:kms:us-east-1:123456789012:key/example
            - apply_immediately: False
    """
    ret = {"name": name, "changes": {}, "result": True, "comment": ""}
    instances = __salt__["boto3_rds.describe_db_instances"](
        name=name,
        jmespath="DBInstances",
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )
    if isinstance(instances, dict) and instances.get("error"):
        ret["result"] = False
        ret["comment"] = instances["error"]
        return ret
    if not instances:
        ret["result"] = False
        ret["comment"] = f"RDS instance {name} does not exist."
        return ret

    instance = instances[0]
    current_secret = instance.get("MasterUserSecret") or {}
    currently_managed = bool(current_secret)

    if manage:
        if currently_managed:
            current_kms_key_id = current_secret.get("KmsKeyId")
            if kms_key_id and current_kms_key_id != kms_key_id:
                ret["result"] = False
                ret["comment"] = (
                    f"RDS instance {name} already manages its master password with a different KMS key; "
                    "AWS does not allow changing that key."
                )
                return ret
            ret["comment"] = f"RDS instance {name} already uses RDS-managed master credentials."
            return ret

        if __opts__["test"]:
            ret["result"] = None
            ret["comment"] = f"RDS instance {name} would enable RDS-managed master credentials."
            return ret

        modified = __salt__["boto3_rds.modify_db_instance"](
            name=name,
            manage_master_user_password=True,
            master_user_secret_kms_key_id=kms_key_id,
            apply_immediately=apply_immediately,
            region=region,
            key=key,
            keyid=keyid,
            profile=profile,
        )
        if not modified.get("modified"):
            ret["result"] = False
            ret["comment"] = modified.get("error") or modified.get(
                "message", f"Failed to enable RDS-managed master credentials for {name}."
            )
            return ret

        ret["changes"] = {
            "master_user_password_management": {"old": "manual", "new": "rds-managed"}
        }
        ret["comment"] = f"Enabled RDS-managed master credentials for {name}."
        return ret
    else:
        if not currently_managed:
            ret["comment"] = f"RDS instance {name} already uses a manual master password."
            return ret

        if __opts__["test"]:
            ret["result"] = None
            ret["comment"] = f"RDS instance {name} would disable RDS-managed master credentials."
            return ret

        modified = __salt__["boto3_rds.modify_db_instance"](
            name=name,
            manage_master_user_password=False,
            master_user_password=master_user_password,
            apply_immediately=apply_immediately,
            region=region,
            key=key,
            keyid=keyid,
            profile=profile,
        )
        if not modified.get("modified"):
            ret["result"] = False
            ret["comment"] = modified.get("error") or modified.get(
                "message", f"Failed to disable RDS-managed master credentials for {name}."
            )
            return ret

        ret["changes"] = {
            "master_user_password_management": {"old": "rds-managed", "new": "manual"}
        }
        ret["comment"] = f"Disabled RDS-managed master credentials for {name}."
        return ret


def master_user_secret_ready(
    name,
    timeout=300,
    poll_interval=5,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """Wait until RDS exposes an active managed master secret.

    RDS creates the secret asynchronously after instance creation or migration.
    Delegates the polling to the ``boto3_rds`` execution module so this state
    function returns quickly in test mode.

    Example:

    .. code-block:: yaml

        wait-for-rds-secret:
          boto3_rds.master_user_secret_ready:
            - name: myrds
            - timeout: 120
    """
    ret = {"name": name, "changes": {}, "result": True, "comment": ""}

    if __opts__["test"]:
        secrets = __salt__["boto3_rds.describe_db_instances"](
            name=name,
            jmespath="DBInstances[*].MasterUserSecret",
            region=region,
            key=key,
            keyid=keyid,
            profile=profile,
        )
        if isinstance(secrets, dict) and secrets.get("error"):
            ret["result"] = False
            ret["comment"] = secrets["error"]
            return ret
        secret = (secrets[0] if secrets else None) or {}
        status = secret.get("SecretStatus")
        if secret.get("SecretArn") and status == "active":
            ret["comment"] = f"RDS-managed master secret for {name} is active."
            return ret
        ret["result"] = None
        ret["comment"] = (
            f"RDS-managed master secret for {name} is not active yet (status: {status or 'missing'})."
        )
        return ret

    result = __salt__["boto3_rds.master_user_secret_ready"](
        name=name,
        timeout=timeout,
        poll_interval=poll_interval,
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )
    if result.get("error"):
        ret["result"] = False
        ret["comment"] = result["error"]
    elif not result.get("ready"):
        ret["result"] = False
        ret["comment"] = result.get(
            "message", f"RDS-managed master secret for {name} is not ready."
        )
    else:
        ret["comment"] = result.get("message", f"RDS-managed master secret for {name} is active.")
    return ret


def replica_present(
    name,
    source,
    db_instance_class=None,
    availability_zone=None,
    port=None,
    auto_minor_version_upgrade=None,
    iops=None,
    option_group_name=None,
    publicly_accessible=None,
    tags=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
    db_parameter_group_name=None,
):
    """
    Ensure RDS replica exists.

    name (str):
        The name of the RDS read replica.

    source (str):
        The name of the source DB instance from which to create the read replica.

    db_instance_class (str, optional):
        The compute and memory capacity of the read replica.

    availability_zone (str, optional):
        The Availability Zone where the read replica will be created.

    port (int, optional):
        The port number on which the database accepts connections.

    auto_minor_version_upgrade (bool, optional):
        Indicates whether minor engine upgrades are applied automatically to the read replica.

    iops (int, optional):
        The amount of Provisioned IOPS (input/output operations per second) to be initially allocated for the read replica.

    option_group_name (str, optional):
        The option group to associate with the read replica.

    publicly_accessible (bool, optional):
        Specifies whether the read replica is publicly accessible.

    tags (dict, optional):
        A dictionary of tags to assign to the read replica.

    region (str, optional):
        The AWS region where the read replica will be created.

    key (str, optional):
        The AWS access key to use.

    keyid (str, optional):
        The AWS secret key to use.

    profile (dict or str, optional):
        The AWS profile to use.

    db_parameter_group_name (str, optional):
        The name of the DB parameter group to associate with the read replica.

    Example:

    .. code-block:: yaml

        Ensure myrds replica RDS exists:
          boto3_rds.create_replica:
            - name: myreplica
            - source: mydb
    """
    ret = {"name": name, "result": True, "comment": "", "changes": {}}
    replica_exists = __salt__["boto3_rds.exists"](name, tags, region, key, keyid, profile)
    if not replica_exists.get("exists"):
        if __opts__["test"]:
            ret["comment"] = f"RDS read replica {name} is set to be created "
            ret["result"] = None
            return ret
        created = __salt__["boto3_rds.create_read_replica"](
            name,
            source,
            db_instance_class,
            availability_zone,
            port,
            auto_minor_version_upgrade,
            iops,
            option_group_name,
            publicly_accessible,
            tags,
            region,
            key,
            keyid,
            profile,
        )
        if created:
            ret["comment"] = f"RDS replica {name} created."
            ret["changes"]["old"] = {"instance": None}
            ret["changes"]["new"] = {
                "instance": __salt__["boto3_rds.describe_db_instances"](
                    name=name,
                    jmespath="DBInstances[0]",
                    region=region,
                    key=key,
                    keyid=keyid,
                    profile=profile,
                )
            }
        else:
            ret["result"] = False
            ret["comment"] = f"Failed to create RDS replica {name}."
    else:
        jmespath = "DBInstances[0].DBParameterGroups[0].DBParameterGroupName"
        pmg_name = __salt__["boto3_rds.describe_db_instances"](
            name=name,
            jmespath=jmespath,
            region=region,
            key=key,
            keyid=keyid,
            profile=profile,
        )
        pmg_name = pmg_name[0] if pmg_name else None
        if pmg_name != db_parameter_group_name:
            modified = __salt__["boto3_rds.modify_db_instance"](
                name=name,
                db_parameter_group_name=db_parameter_group_name,
                region=region,
                key=key,
                keyid=keyid,
                profile=profile,
            )
            if not modified:
                ret["result"] = False
                ret["comment"] = f"Failed to update parameter group of {name} RDS instance."
            ret["changes"]["old"] = pmg_name
            ret["changes"]["new"] = db_parameter_group_name
        ret["result"] = True
        ret["comment"] = f"RDS replica {name} exists."
    return ret


def subnet_group_present(
    name,
    description,
    subnet_ids=None,
    subnet_names=None,
    tags=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Ensure DB subnet group exists.

    name (str):
        The name for the DB subnet group. This value is stored as a lowercase string.

    subnet_ids (list, optional):
        A list of the EC2 Subnet IDs for the DB subnet group.
        Either subnet_ids or subnet_names must be provided.

    subnet_names (list, optional):
        A list of The EC2 Subnet names for the DB subnet group.
        Either subnet_ids or subnet_names must be provided.

    description (str):
        Subnet group description.

    tags (dict, optional):
        A dict of tags.

    region (str, optional):
        Region to connect to.

    key (str, optional):
        Secret key to be used.

    keyid (str, optional):
        Access key to be used.

    profile (dict or str, optional):
        A dict with region, key and keyid, or a pillar key (string) that
        contains a dict with region, key and keyid.

    Example:

    .. code-block:: yaml

        ensure-subnet-group-present:
          boto3_rds.subnet_group_present:
            - name: example
            - description: "Example subnet group"
            - subnet_ids:
              - subnet-12345678
              - subnet-87654321
            - tags:
                Environment: "dev"
    """
    if not salt.utils.data.exactly_one((subnet_ids, subnet_names)):
        raise SaltInvocationError(
            "One (but not both) of subnet_ids or subnet_names must be provided."
        )

    ret = {"name": name, "result": True, "comment": "", "changes": {}}

    if not subnet_ids:
        subnet_ids = []

    if subnet_names:
        for i in subnet_names:
            r = __salt__["boto3_vpc.get_resource_id"](
                "subnet", name=i, region=region, key=key, keyid=keyid, profile=profile
            )

            if "error" in r:
                ret["comment"] = "Error looking up subnet ids: {}".format(r["error"]["message"])
                ret["result"] = False
                return ret
            if r["id"] is None:
                ret["comment"] = f"Subnet {i} does not exist."
                ret["result"] = False
                return ret
            subnet_ids.append(r["id"])

    exists = __salt__["boto3_rds.subnet_group_exists"](
        name=name, tags=tags, region=region, key=key, keyid=keyid, profile=profile
    )
    if not exists.get("exists"):
        if __opts__["test"]:
            ret["comment"] = f"Subnet group {name} is set to be created."
            ret["result"] = None
            return ret
        created = __salt__["boto3_rds.create_subnet_group"](
            name=name,
            description=description,
            subnet_ids=subnet_ids,
            tags=tags,
            region=region,
            key=key,
            keyid=keyid,
            profile=profile,
        )

        if not created:
            ret["result"] = False
            ret["comment"] = f"Failed to create {name} subnet group."
            return ret
        ret["changes"]["old"] = None
        ret["changes"]["new"] = name
        ret["comment"] = f"Subnet {name} created."
        return ret
    else:
        ret["comment"] = f"Subnet {name} present."

        return ret


def absent(
    name,
    skip_final_snapshot=None,
    final_db_snapshot_identifier=None,
    tags=None,
    wait_for_deletion=True,
    timeout=180,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Ensure RDS instance is absent.

    name (str):
        Name of the RDS instance.

    skip_final_snapshot (bool, optional):
        Whether a final db snapshot is created before the instance is deleted.
        If True, no snapshot is created.
        If False, a snapshot is created before deleting the instance.

    final_db_snapshot_identifier (str, optional):
        If a final snapshot is requested, this is the identifier used for that
        snapshot.

    tags (dict, optional):
        A dict of tags.

    wait_for_deletion (bool, optional):
        Wait for the RDS instance to be deleted completely before finishing
        the state.

    timeout (int, optional):
        The amount of time that can pass before raising an Exception.

    region (str, optional):
        Region to connect to.

    key (str, optional):
        Secret key to be used.

    keyid (str, optional):
        Access key to be used.

    profile (dict or str, optional):
        A dict with region, key and keyid, or a pillar key (string) that
        contains a dict with region, key and keyid.

    Example:

    .. code-block:: yaml

        ensure-absent:
          boto3_rds.absent:
            - name: example
    """
    ret = {"name": name, "result": True, "comment": "", "changes": {}}

    current = __salt__["boto3_rds.describe_db_instances"](
        name=name, region=region, key=key, keyid=keyid, profile=profile
    )
    if not current:
        ret["result"] = True
        ret["comment"] = f"{name} RDS already absent."
        return ret

    if __opts__["test"]:
        ret["comment"] = f"RDS {name} would be removed."
        ret["result"] = None
        return ret
    deleted = __salt__["boto3_rds.delete"](
        name,
        skip_final_snapshot,
        final_db_snapshot_identifier,
        region,
        key,
        keyid,
        profile,
        tags,
        wait_for_deletion,
        timeout,
    )
    if not deleted:
        ret["result"] = False
        ret["comment"] = f"Failed to delete {name} RDS."
        return ret
    ret["changes"]["old"] = {"instance": current[0]}
    ret["changes"]["new"] = {"instance": None}
    ret["comment"] = f"RDS {name} deleted."
    return ret


def subnet_group_absent(name, tags=None, region=None, key=None, keyid=None, profile=None):
    """
    Ensure an RDS DB subnet group is absent.

    name (str):
        The name of the RDS DB subnet group to ensure is absent.

    tags (dict, optional):
        Tags associated with the RDS DB subnet group.

    region (str, optional):
        Region to connect to.

    key (str, optional):
        Secret key to be used.

    keyid (str, optional):
        Access key to be used.

    profile (dict or str, optional):
        A dict with region, key and keyid, or a pillar key (string) that
        contains a dict with region, key and keyid.

    Example:

    .. code-block:: yaml

        ensure-subnet-group-absent:
          boto3_rds.subnet_group_absent:
            - name: example
    """
    ret = {"name": name, "result": True, "comment": "", "changes": {}}

    exists = __salt__["boto3_rds.subnet_group_exists"](
        name=name, tags=tags, region=region, key=key, keyid=keyid, profile=profile
    )
    if not exists:
        ret["result"] = True
        ret["comment"] = f"{name} RDS subnet group does not exist."
        return ret

    if __opts__["test"]:
        ret["comment"] = f"RDS subnet group {name} is set to be removed."
        ret["result"] = None
        return ret
    deleted = __salt__["boto3_rds.delete_subnet_group"](name, region, key, keyid, profile)
    if not deleted:
        ret["result"] = False
        ret["comment"] = f"Failed to delete {name} RDS subnet group."
        return ret
    ret["changes"]["old"] = name
    ret["changes"]["new"] = None
    ret["comment"] = f"RDS subnet group {name} deleted."
    return ret


def endpoint_secret_present(
    name,
    db_instance_name,
    host_key="host",
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Ensure a Secrets Manager JSON secret records the instance's current endpoint.

    The endpoint is only known after the instance is available, so this is
    normally declared after :py:func:`present` with a ``require`` on it.

    name (str):
        The Secrets Manager secret ID holding the JSON credentials document.

    db_instance_name (str):
        Name of the RDS instance whose endpoint should be published.

    host_key (str, optional):
        Key within the JSON document to write the endpoint to. Defaults to ``host``.

    region (str, optional):
        Region to connect to.

    key (str, optional):
        AWS secret key to be used.

    keyid (str, optional):
        AWS access key to be used.

    profile (dict or str, optional):
        A dict with region, key and keyid, or a pillar key (string) that
        contains a dict with region, key and keyid.

    Example:

    .. code-block:: yaml

        publish-endpoint:
          boto3_rds.endpoint_secret_present:
            - name: myapp/dev/rds
            - db_instance_name: myrds
            - region: us-east-1
    """
    ret = {"name": name, "result": True, "comment": "", "changes": {}}

    if "boto3_secretsmanager.get" not in __salt__:
        ret["result"] = False
        ret["comment"] = "The boto3_secretsmanager execution module is unavailable."
        return ret

    endpoint = __salt__["boto3_rds.get_endpoint"](
        db_instance_name, region=region, key=key, keyid=keyid, profile=profile
    )
    if not endpoint or isinstance(endpoint, dict):
        ret["result"] = False
        ret["comment"] = f"RDS endpoint for {db_instance_name} is not available."
        return ret

    current = __salt__["boto3_secretsmanager.get"](
        name, region=region, key=key, keyid=keyid, profile=profile
    )
    if current.get("error"):
        ret["result"] = False
        ret["comment"] = current["error"]
        return ret
    if not current.get("exists"):
        ret["result"] = False
        ret["comment"] = f"Secrets Manager secret {name} does not exist."
        return ret

    try:
        secret_data = json.loads(current.get("secret_string") or "{}")
    except (TypeError, ValueError) as exc:
        ret["result"] = False
        ret["comment"] = f"Secret {name} is not valid JSON: {exc}"
        return ret

    if secret_data.get(host_key) == endpoint:
        ret["comment"] = f"Secret {name} already records the current endpoint."
        return ret

    if __opts__["test"]:
        ret["result"] = None
        ret["comment"] = f"Secret {name} {host_key} would be set to {endpoint}."
        return ret

    old_host = secret_data.get(host_key)
    secret_data[host_key] = endpoint
    updated = __salt__["boto3_secretsmanager.put"](
        name,
        json.dumps(secret_data, separators=(",", ":"), sort_keys=True),
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )
    if not updated.get("updated"):
        ret["result"] = False
        ret["comment"] = updated.get("error", f"Failed to update secret {name}.")
        return ret

    ret["changes"] = {host_key: {"old": old_host, "new": endpoint}}
    ret["comment"] = f"Secret {name} endpoint updated."
    return ret


def parameter_present(
    name,
    db_parameter_group_family,
    description,
    parameters=None,
    apply_method="pending-reboot",
    tags=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Ensure DB parameter group exists and update parameters.

    name (str):
        The name for the parameter group.

    db_parameter_group_family (str):
        The DB parameter group family name. A
        DB parameter group can be associated with one and only one DB
        parameter group family, and can be applied only to a DB instance
        running a database engine and engine version compatible with that
        DB parameter group family.

    description (str):
        Parameter group description.

    parameters (dict, optional):
        The DB parameters that need to be changed of type dictionary.

    apply_method (str, optional):
        The `apply-immediate` method can be used only for dynamic
        parameters; the `pending-reboot` method can be used with MySQL
        and Oracle DB instances for either dynamic or static
        parameters. For Microsoft SQL Server DB instances, the
        `pending-reboot` method can be used only for static
        parameters.

    tags (dict, optional):
        A dict of tags.

    region (str, optional):
        Region to connect to.

    key (str, optional):
        Secret key to be used.

    keyid (str, optional):
        Access key to be used.

    profile (dict or str, optional):
        A dict with region, key and keyid, or a pillar key (string) that
        contains a dict with region, key and keyid.

    Example:

    .. code-block:: yaml

        ensure-parameter-present:
          boto3_rds.parameter_present:
            - name: example
            - db_parameter_group_family: mysql5.7
            - description: "Example parameter group"
            parameters:
              max_connections: 100
            - apply_method: pending-reboot
            - tags:
              Environment: "dev"
    """
    ret = {"name": name, "result": True, "comment": "", "changes": {}}
    res = __salt__["boto3_rds.parameter_group_exists"](
        name=name, tags=tags, region=region, key=key, keyid=keyid, profile=profile
    )
    if not res.get("exists"):
        if __opts__["test"]:
            ret["comment"] = f"Parameter group {name} is set to be created."
            ret["result"] = None
            return ret
        created = __salt__["boto3_rds.create_parameter_group"](
            name=name,
            db_parameter_group_family=db_parameter_group_family,
            description=description,
            tags=tags,
            region=region,
            key=key,
            keyid=keyid,
            profile=profile,
        )
        if not created:
            ret["result"] = False
            ret["comment"] = f"Failed to create {name} parameter group."
            return ret
        ret["changes"]["New Parameter Group"] = name
        ret["comment"] = f"Parameter group {name} created."
    else:
        ret["comment"] = f"Parameter group {name} present."
    if parameters is not None:
        params = {}
        changed = {}
        for items in parameters:
            for k, value in items.items():
                if isinstance(value, bool):
                    params[k] = "on" if value else "off"
                else:
                    params[k] = str(value)
        log.debug("Parameters from user are : %s.", params)
        options = __salt__["boto3_rds.describe_parameters"](
            name=name, region=region, key=key, keyid=keyid, profile=profile
        )
        if not options.get("result"):
            ret["result"] = False
            ret["comment"] = os.linesep.join(
                [ret["comment"], f"Faled to get parameters for group  {name}."]
            )
            return ret
        for parameter in options["parameters"].values():
            if parameter["ParameterName"] in params and params.get(
                parameter["ParameterName"]
            ) != str(parameter["ParameterValue"]):
                log.debug(
                    "Values that are being compared for %s are %s:%s.",
                    parameter["ParameterName"],
                    params.get(parameter["ParameterName"]),
                    parameter["ParameterValue"],
                )
                changed[parameter["ParameterName"]] = params.get(parameter["ParameterName"])
        if len(changed) > 0:
            if __opts__["test"]:
                ret["comment"] = os.linesep.join(
                    [
                        ret["comment"],
                        f"Parameters {changed} for group {name} are set to be changed.",
                    ]
                )
                ret["result"] = None
                return ret
            update = __salt__["boto3_rds.update_parameter_group"](
                name,
                parameters=changed,
                apply_method=apply_method,
                tags=tags,
                region=region,
                key=key,
                keyid=keyid,
                profile=profile,
            )
            if "error" in update:
                ret["result"] = False
                ret["comment"] = os.linesep.join(
                    [
                        ret["comment"],
                        f"Failed to change parameters {changed} for group {name}:",
                        update["error"]["message"],
                    ]
                )
                return ret
            ret["changes"]["Parameters"] = changed
            ret["comment"] = os.linesep.join(
                [
                    ret["comment"],
                    f"Parameters {changed} for group {name} are changed.",
                ]
            )
        else:
            ret["comment"] = os.linesep.join(
                [
                    ret["comment"],
                    f"Parameters {params} for group {name} are present.",
                ]
            )
    return ret
