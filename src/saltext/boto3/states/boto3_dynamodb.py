"""
Manage DynamoDB Tables using boto3.
===================================

    Renamed from ``boto_dynamodb`` to ``boto3_dynamodb`` and updated to call the
    refactored ``boto3_dynamodb`` execution module.

Create and destroy DynamoDB tables. Be aware that this interacts with Amazon's
services, and so may incur charges.

:depends:
  - boto3 >= 1.28.0
  - botocore >= 1.31.0

This module uses boto3, which can be installed via package, or pip.

This module accepts explicit DynamoDB credentials but can also utilize
IAM roles assigned to the instance through Instance Profiles. Dynamic
credentials are then automatically obtained from AWS API and no further
configuration is necessary. More Information available at:

.. code-block:: text

    http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html

If IAM roles are not used you need to specify them either in the minion's config file
or as a profile. For example, to specify them in the minion's config file:

.. code-block:: yaml

    dynamodb.keyid: GKTADJGHEIQSXMKKRBJ08H
    dynamodb.key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs

It's also possible to specify key, keyid and region via a profile, either
as a passed in dict, or as a string to pull from pillars or minion config:

.. code-block:: yaml

    myprofile:
        keyid: GKTADJGHEIQSXMKKRBJ08H
        key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs
        region: us-east-1

.. code-block:: yaml

    ensure-present:
      boto3_dynamodb.present:
        - name: example
        - region: us-east-1
        - profile: myprofile

.. versionadded:: 1.0.0
"""

import copy
import datetime
import logging
import math

from salt.utils import dictupdate

log = logging.getLogger(__name__)

__virtualname__ = "boto3_dynamodb"

_ALARM_KEY_MAP = {
    "metric": "MetricName",
    "namespace": "Namespace",
    "statistic": "Statistic",
    "comparison": "ComparisonOperator",
    "threshold": "Threshold",
    "period": "Period",
    "evaluation_periods": "EvaluationPeriods",
    "unit": "Unit",
    "description": "AlarmDescription",
    "alarm_actions": "AlarmActions",
    "insufficient_data_actions": "InsufficientDataActions",
    "ok_actions": "OKActions",
    "dimensions": "Dimensions",
}


class GsiNotUpdatableError(Exception):
    """Raised when a global secondary index cannot be updated."""


def __virtual__():
    """
    Only load if the boto3_dynamodb execution module is available.
    """
    if "boto3_dynamodb.exists" in __salt__:
        return __virtualname__
    return (
        False,
        "boto3_dynamodb state module could not be loaded: "
        "boto3_dynamodb execution module is not available.",
    )


def present(
    name=None,
    table_name=None,
    region=None,
    key=None,
    keyid=None,
    profile=None,
    read_capacity_units=None,
    write_capacity_units=None,
    alarms=None,
    alarms_from_pillar="boto3_dynamodb_alarms",
    hash_key=None,
    hash_key_data_type=None,
    range_key=None,
    range_key_data_type=None,
    local_indexes=None,
    global_indexes=None,
    backup_configs_from_pillars="boto3_dynamodb_backup_configs",
):
    """
    Ensure the DynamoDB table exists and matches the specified configuration.

    Example:

    .. code-block:: yaml

        ensure-present:
          boto3_dynamodb.present:
            - name: example

    """
    ret = {"name": name, "result": True, "comment": "", "changes": {}}
    if table_name:
        ret["warnings"] = [
            "boto3_dynamodb.present: `table_name` is deprecated. Please use `name` instead."
        ]
        ret["name"] = table_name
        name = table_name

    comments = []
    changes_old = {}
    changes_new = {}

    table_exists = __salt__["boto3_dynamodb.exists"](name, region, key, keyid, profile)
    if not table_exists:
        if __opts__["test"]:
            ret["result"] = None
            ret["comment"] = f"DynamoDB table {name} would be created."
            return ret
        is_created = __salt__["boto3_dynamodb.create_table"](
            name,
            region,
            key,
            keyid,
            profile,
            read_capacity_units,
            write_capacity_units,
            hash_key,
            hash_key_data_type,
            range_key,
            range_key_data_type,
            local_indexes,
            global_indexes,
        )
        if not is_created:
            ret["result"] = False
            ret["comment"] = f"Failed to create table {name}"
            _add_changes(ret, changes_old, changes_new)
            return ret

        comments.append(f"DynamoDB table {name} was successfully created")
        changes_new["table"] = name
        changes_new["read_capacity_units"] = read_capacity_units
        changes_new["write_capacity_units"] = write_capacity_units
        changes_new["hash_key"] = hash_key
        changes_new["hash_key_data_type"] = hash_key_data_type
        changes_new["range_key"] = range_key
        changes_new["range_key_data_type"] = range_key_data_type
        changes_new["local_indexes"] = local_indexes
        changes_new["global_indexes"] = global_indexes
    else:
        comments.append(f"DynamoDB table {name} exists")

    description = __salt__["boto3_dynamodb.describe"](name, region, key, keyid, profile)
    provisioned_throughput = description.get("Table", {}).get("ProvisionedThroughput", {})
    current_write_capacity_units = provisioned_throughput.get("WriteCapacityUnits")
    current_read_capacity_units = provisioned_throughput.get("ReadCapacityUnits")
    throughput_matches = (
        current_write_capacity_units == write_capacity_units
        and current_read_capacity_units == read_capacity_units
    )
    if not throughput_matches:
        if __opts__["test"]:
            ret["result"] = None
            comments.append(f"DynamoDB table {name} is set to be updated.")
        else:
            is_updated = __salt__["boto3_dynamodb.update"](
                name,
                throughput={
                    "read": read_capacity_units,
                    "write": write_capacity_units,
                },
                region=region,
                key=key,
                keyid=keyid,
                profile=profile,
            )
            if not is_updated:
                ret["result"] = False
                ret["comment"] = f"Failed to update table {name}"
                _add_changes(ret, changes_old, changes_new)
                return ret

            comments.append(f"DynamoDB table {name} was successfully updated")
            changes_old["read_capacity_units"] = (current_read_capacity_units,)
            changes_old["write_capacity_units"] = (current_write_capacity_units,)
            changes_new["read_capacity_units"] = (read_capacity_units,)
            changes_new["write_capacity_units"] = (write_capacity_units,)
    else:
        comments.append(f"DynamoDB table {name} throughput matches")

    provisioned_indexes = description.get("Table", {}).get("GlobalSecondaryIndexes", [])

    _ret = _global_indexes_present(
        provisioned_indexes,
        global_indexes,
        changes_old,
        changes_new,
        comments,
        name,
        region,
        key,
        keyid,
        profile,
    )
    if not _ret["result"]:
        comments.append(_ret["comment"])
        ret["result"] = _ret["result"]
        if ret["result"] is False:
            ret["comment"] = ",\n".join(comments)
            _add_changes(ret, changes_old, changes_new)
            return ret

    _ret = _alarms_present(
        name,
        alarms,
        alarms_from_pillar,
        write_capacity_units,
        read_capacity_units,
        region,
        key,
        keyid,
        profile,
    )
    ret["changes"] = dictupdate.update(ret["changes"], _ret["changes"])
    comments.append(_ret["comment"])
    if not _ret["result"]:
        ret["result"] = _ret["result"]
        if ret["result"] is False:
            ret["comment"] = ",\n".join(comments)
            _add_changes(ret, changes_old, changes_new)
            return ret

    datapipeline_configs = copy.deepcopy(__salt__["pillar.get"](backup_configs_from_pillars, []))
    for config in datapipeline_configs:
        datapipeline_ret = _ensure_backup_datapipeline_present(
            name=name,
            schedule_name=config["name"],
            period=config["period"],
            utc_hour=config["utc_hour"],
            s3_base_location=config["s3_base_location"],
        )
        if datapipeline_ret["result"] in [True, None]:
            ret["result"] = datapipeline_ret["result"]
            comments.append(datapipeline_ret["comment"])
            if datapipeline_ret.get("changes"):
                ret["changes"]["backup_datapipeline_{}".format(config["name"])] = (
                    datapipeline_ret.get("changes"),
                )
        else:
            ret["comment"] = ",\n".join([ret["comment"], datapipeline_ret["comment"]])
            _add_changes(ret, changes_old, changes_new)
            return ret

    ret["comment"] = ",\n".join(comments)
    _add_changes(ret, changes_old, changes_new)
    return ret


def _add_changes(ret, changes_old, changes_new):
    if changes_old:
        ret["changes"]["old"] = changes_old
    if changes_new:
        ret["changes"]["new"] = changes_new


def _global_indexes_present(
    provisioned_indexes,
    global_indexes,
    changes_old,
    changes_new,
    comments,
    name,
    region,
    key,
    keyid,
    profile,
):
    """Handle global secondary indexes for ``present``."""
    ret = {"result": True}
    if provisioned_indexes:
        provisioned_gsi_config = {index["IndexName"]: index for index in provisioned_indexes}
    else:
        provisioned_gsi_config = {}
    provisioned_index_names = set(provisioned_gsi_config.keys())

    gsi_config = {}
    if global_indexes:
        for index in global_indexes:
            index_config = next(iter(index.values()))
            index_name = None
            for entry in index_config:
                if list(entry.keys()) == ["name"]:
                    index_name = next(iter(entry.values()))
            if not index_name:
                ret["result"] = False
                ret["comment"] = f"Index name not found for table {name}"
                return ret
            gsi_config[index_name] = index

    (
        existing_index_names,
        new_index_names,
        index_names_to_be_deleted,
    ) = _partition_index_names(provisioned_index_names, set(gsi_config.keys()))

    if index_names_to_be_deleted:
        ret["result"] = False
        ret["comment"] = (
            "Deletion of GSIs ({}) is not supported! Please do this "
            "manually in the AWS console.".format(", ".join(index_names_to_be_deleted))
        )
        return ret
    if len(new_index_names) > 1:
        ret["result"] = False
        ret["comment"] = (
            "Creation of multiple GSIs ({}) is not supported due to API "
            "limitations. Please create them one at a time.".format(new_index_names)
        )
        return ret

    if new_index_names:
        index_name = next(iter(new_index_names))
        _add_global_secondary_index(
            ret,
            name,
            index_name,
            changes_old,
            changes_new,
            comments,
            gsi_config,
            region,
            key,
            keyid,
            profile,
        )
        if not ret["result"]:
            return ret

    if existing_index_names:
        _update_global_secondary_indexes(
            ret,
            changes_old,
            changes_new,
            comments,
            existing_index_names,
            provisioned_gsi_config,
            gsi_config,
            name,
            region,
            key,
            keyid,
            profile,
        )
        if not ret["result"]:
            return ret

    if "global_indexes" not in changes_old and "global_indexes" not in changes_new:
        comments.append("All global secondary indexes match")
    return ret


def _partition_index_names(provisioned_index_names, index_names):
    """Return three disjoint sets: existing, new, to-be-deleted."""
    existing_index_names = set()
    new_index_names = set()
    for name in index_names:
        if name in provisioned_index_names:
            existing_index_names.add(name)
        else:
            new_index_names.add(name)
    index_names_to_be_deleted = provisioned_index_names - existing_index_names
    return existing_index_names, new_index_names, index_names_to_be_deleted


def _add_global_secondary_index(
    ret,
    name,
    index_name,
    changes_old,
    changes_new,
    comments,
    gsi_config,
    region,
    key,
    keyid,
    profile,
):  # pylint: disable=unused-argument
    """Create a GSI, updating ``ret`` on failure or test mode."""
    if __opts__["test"]:
        ret["result"] = None
        ret["comment"] = f"Dynamo table {name} will have a GSI added: {index_name}"
        return
    changes_new.setdefault("global_indexes", {})
    success = __salt__["boto3_dynamodb.create_global_secondary_index"](
        name,
        __salt__["boto3_dynamodb.extract_index"](gsi_config[index_name], global_index=True),
        region=region,
        key=key,
        keyid=keyid,
        profile=profile,
    )

    if success:
        comments.append(f"Created GSI {index_name}")
        changes_new["global_indexes"][index_name] = gsi_config[index_name]
    else:
        ret["result"] = False
        ret["comment"] = f"Failed to create GSI {index_name}"


def _update_global_secondary_indexes(
    ret,
    changes_old,
    changes_new,
    comments,
    existing_index_names,
    provisioned_gsi_config,
    gsi_config,
    name,
    region,
    key,
    keyid,
    profile,
):
    """Update GSI provisioned throughput, updating ``ret`` on failure or test mode."""
    try:
        provisioned_throughputs, index_updates = _determine_gsi_updates(
            existing_index_names, provisioned_gsi_config, gsi_config
        )
    except GsiNotUpdatableError as e:
        ret["result"] = False
        ret["comment"] = str(e)
        return

    if index_updates:
        if __opts__["test"]:
            ret["result"] = None
            ret["comment"] = "Dynamo table {} will have GSIs updated: {}".format(
                name, ", ".join(index_updates.keys())
            )
            return
        changes_old.setdefault("global_indexes", {})
        changes_new.setdefault("global_indexes", {})
        success = __salt__["boto3_dynamodb.update_global_secondary_index"](
            name,
            index_updates,
            region=region,
            key=key,
            keyid=keyid,
            profile=profile,
        )

        if success:
            comments.append(f"Updated GSIs with new throughputs {index_updates}")
            for index_name in index_updates:
                changes_old["global_indexes"][index_name] = provisioned_throughputs[index_name]
                changes_new["global_indexes"][index_name] = index_updates[index_name]
        else:
            ret["result"] = False
            ret["comment"] = f"Failed to update GSI throughputs {index_updates}"


def _determine_gsi_updates(existing_index_names, provisioned_gsi_config, gsi_config):
    provisioned_throughputs = {}
    index_updates = {}
    for index_name in existing_index_names:
        current_config = provisioned_gsi_config[index_name]
        new_config = __salt__["boto3_dynamodb.extract_index"](
            gsi_config[index_name], global_index=True
        )

        for k in new_config:
            if k.startswith("_"):
                continue
            if k in current_config and k != "ProvisionedThroughput":
                new_value = new_config[k]
                current_value = current_config[k]
                if k == "Projection":
                    if new_value["ProjectionType"] != current_value["ProjectionType"]:
                        raise GsiNotUpdatableError("GSI projection types do not match")
                    if set(new_value.get("NonKeyAttributes", [])) != set(
                        current_value.get("NonKeyAttributes", [])
                    ):
                        raise GsiNotUpdatableError(
                            "NonKeyAttributes do not match for GSI projection"
                        )
                elif new_value != current_value:
                    raise GsiNotUpdatableError(
                        f"GSI property {k} cannot be updated for index {index_name}"
                    )

        current_throughput = current_config.get("ProvisionedThroughput") or {}
        current_read = current_throughput.get("ReadCapacityUnits")
        current_write = current_throughput.get("WriteCapacityUnits")
        provisioned_throughputs[index_name] = {
            "read": current_read,
            "write": current_write,
        }
        new_throughput = new_config.get("ProvisionedThroughput") or {}
        new_read = new_throughput.get("ReadCapacityUnits")
        new_write = new_throughput.get("WriteCapacityUnits")
        if current_read != new_read or current_write != new_write:
            index_updates[index_name] = {"read": new_read, "write": new_write}

    return provisioned_throughputs, index_updates


def _translate_alarm_attrs(attrs):
    """
    Convert legacy snake_case alarm attributes to the PascalCase keys expected
    by ``boto3_cloudwatch_alarm.present``. Unknown keys are passed through.
    """
    translated = {}
    for k, v in attrs.items():
        translated[_ALARM_KEY_MAP.get(k, k)] = v
    return translated


def _alarms_present(
    name,
    alarms,
    alarms_from_pillar,
    write_capacity_units,
    read_capacity_units,
    region,
    key,
    keyid,
    profile,
):
    """Ensure cloudwatch alarms are set for the given table."""
    tmp = copy.deepcopy(__salt__["config.option"](alarms_from_pillar, {}))
    if alarms:
        tmp = dictupdate.update(tmp, alarms)
    merged_return_value = {"name": name, "result": True, "comment": "", "changes": {}}
    for _, info in tmp.items():
        info["name"] = name + " " + info["name"]
        info["attributes"]["description"] = name + " " + info["attributes"]["description"]
        info["attributes"]["dimensions"] = [{"Name": "TableName", "Value": name}]
        if (
            info["attributes"]["metric"] == "ConsumedWriteCapacityUnits"
            and "threshold" not in info["attributes"]
        ):
            info["attributes"]["threshold"] = math.ceil(
                write_capacity_units * info["attributes"]["threshold_percent"]
            )
            del info["attributes"]["threshold_percent"]
            info["attributes"]["threshold"] *= info["attributes"]["period"]
        if (
            info["attributes"]["metric"] == "ConsumedReadCapacityUnits"
            and "threshold" not in info["attributes"]
        ):
            info["attributes"]["threshold"] = math.ceil(
                read_capacity_units * info["attributes"]["threshold_percent"]
            )
            del info["attributes"]["threshold_percent"]
            info["attributes"]["threshold"] *= info["attributes"]["period"]

        translated_attrs = _translate_alarm_attrs(info["attributes"])
        kwargs = {
            "name": info["name"],
            "attributes": translated_attrs,
            "region": region,
            "key": key,
            "keyid": keyid,
            "profile": profile,
        }
        results = __states__["boto3_cloudwatch_alarm.present"](**kwargs)
        if not results["result"]:
            merged_return_value["result"] = results["result"]
        if results.get("changes", {}) != {}:
            merged_return_value["changes"][info["name"]] = results["changes"]
        if "comment" in results:
            merged_return_value["comment"] += results["comment"]
    return merged_return_value


def _ensure_backup_datapipeline_present(name, schedule_name, period, utc_hour, s3_base_location):
    kwargs = {
        "name": f"{name}-{schedule_name}-backup",
        "pipeline_objects": {
            "DefaultSchedule": {
                "name": schedule_name,
                "fields": {
                    "period": period,
                    "type": "Schedule",
                    "startDateTime": _next_datetime_with_utc_hour(name, utc_hour).isoformat(),
                },
            },
        },
        "parameter_values": {
            "myDDBTableName": name,
            "myOutputS3Loc": f"{s3_base_location}/{name}/",
        },
    }
    return __states__["boto3_datapipeline.present"](**kwargs)


def _get_deterministic_value_for_table_name(table_name, max_value):
    """Return a deterministic hash of the table name modulo max_value."""
    return hash(table_name) % max_value


def _next_datetime_with_utc_hour(table_name, utc_hour):
    """Return the next future UTC datetime whose hour matches ``utc_hour``."""
    today = datetime.date.today()
    start_date_time = datetime.datetime(
        year=today.year,
        month=today.month,
        day=today.day,
        hour=utc_hour,
        minute=_get_deterministic_value_for_table_name(table_name, 60),
        second=_get_deterministic_value_for_table_name(table_name, 60),
    )

    if start_date_time < datetime.datetime.utcnow():
        start_date_time += datetime.timedelta(days=1)

    return start_date_time


def absent(name, region=None, key=None, keyid=None, profile=None):
    """
    Ensure the DynamoDB table does not exist.

    Example:

    .. code-block:: yaml

        ensure-absent:
          boto3_dynamodb.absent:
            - name: example

    """
    ret = {"name": name, "result": True, "comment": "", "changes": {}}
    exists = __salt__["boto3_dynamodb.exists"](name, region, key, keyid, profile)
    if not exists:
        ret["comment"] = f"DynamoDB table {name} does not exist"
        return ret

    if __opts__["test"]:
        ret["comment"] = f"DynamoDB table {name} is set to be deleted"
        ret["result"] = None
        return ret

    is_deleted = __salt__["boto3_dynamodb.delete"](name, region, key, keyid, profile)
    if is_deleted:
        ret["comment"] = f"Deleted DynamoDB table {name}"
        ret["changes"].setdefault("old", f"Table {name} exists")
        ret["changes"].setdefault("new", f"Table {name} deleted")
    else:
        ret["comment"] = f"Failed to delete DynamoDB table {name}"
        ret["result"] = False
    return ret
