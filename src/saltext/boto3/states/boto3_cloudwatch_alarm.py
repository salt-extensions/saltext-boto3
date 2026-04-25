"""
Manage CloudWatch alarms using boto3.
=====================================

    Renamed from ``boto_cloudwatch_alarm`` to ``boto3_cloudwatch_alarm`` and updated to call the
    refactored ``boto3_cloudwatch_alarm`` execution module.

Create and destroy CloudWatch alarms. Be aware that this interacts with
Amazon's services, and so may incur charges.

:depends:
  - boto3 >= 1.28.0
  - botocore >= 1.31.0

This module uses boto3, which can be installed via package, or pip.

This module accepts explicit CloudWatch credentials but can also utilize
IAM roles assigned to the instance through Instance Profiles. Dynamic
credentials are then automatically obtained from AWS API and no further
configuration is necessary. More Information available at:

.. code-block:: text

    http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html

If IAM roles are not used you need to specify them either in the minion's config file
or as a profile. For example, to specify them in the minion's config file:

.. code-block:: yaml

    cloudwatch.keyid: GKTADJGHEIQSXMKKRBJ08H
    cloudwatch.key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs

It's also possible to specify key, keyid and region via a profile, either
as a passed in dict, or as a string to pull from pillars or minion config:

.. code-block:: yaml

    myprofile:
        keyid: GKTADJGHEIQSXMKKRBJ08H
        key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs
        region: us-east-1

.. code-block:: yaml

    my test alarm:
      boto3_cloudwatch_alarm.present:
        - name: my test alarm
        - attributes:
            MetricName: ApproximateNumberOfMessagesVisible
            Namespace: AWS/SQS
            Statistic: Average
            ComparisonOperator: GreaterThanOrEqualToThreshold
            Threshold: 20000.0
            Period: 60
            EvaluationPeriods: 1
            AlarmDescription: test alarm via salt
            Dimensions:
              - Name: QueueName
                Value: the-sqs-queue-name
            AlarmActions:
              - arn:aws:sns:us-east-1:1111111:myalerting-action

.. versionadded:: 1.0.0
"""

import salt.utils.data

__virtualname__ = "boto3_cloudwatch_alarm"


def __virtual__():
    """
    Only load if the boto3_cloudwatch execution module is available.
    """
    if "boto3_cloudwatch.get_alarm" in __salt__:
        return __virtualname__
    return (
        False,
        "The boto3_cloudwatch_alarm state module could not be loaded: "
        "boto3_cloudwatch exec module unavailable.",
    )


def present(name, attributes, region=None, key=None, keyid=None, profile=None):
    """
    Ensure the cloudwatch alarm exists.

    name
        Name of the alarm.

    attributes
        A dict of boto3 CloudWatch alarm attributes (``MetricName``,
        ``Namespace``, ``Statistic``, ``ComparisonOperator``, ``Threshold``,
        ``Period``, ``EvaluationPeriods``, ``Unit``, ``AlarmDescription``,
        ``Dimensions``, ``AlarmActions``, ``InsufficientDataActions``,
        ``OKActions``).

    region
        Region to connect to.

    key
        Secret key to be used.

    keyid
        Access key to be used.

    profile
        A dict with region, key and keyid, or a pillar key (string)
        that contains a dict with region, key and keyid.

    Example:

    .. code-block:: yaml

        ensure-present:
          boto3_cloudwatch_alarm.present:
            - name: example

    """
    ret = {"name": name, "result": True, "comment": "", "changes": {}}
    alarm_details = __salt__["boto3_cloudwatch.get_alarm"](
        name, region=region, key=key, keyid=keyid, profile=profile
    )

    # Convert scaling_policy:... entries into ARNs
    for k in ("AlarmActions", "InsufficientDataActions", "OKActions"):
        if k in attributes:
            attributes[k] = __salt__["boto3_cloudwatch.convert_to_arn"](
                attributes[k], region=region, key=key, keyid=keyid, profile=profile
            )

    difference = []
    if alarm_details:
        for k, v in attributes.items():
            if k not in alarm_details:
                difference.append(f"{k}={v} (new)")
                continue
            v = salt.utils.data.decode(v)
            v2 = salt.utils.data.decode(alarm_details[k])
            if v == v2:
                continue
            if isinstance(v, str) and v == v2:
                continue
            if isinstance(v, float) and v == float(v2):
                continue
            if isinstance(v, int) and v == int(v2):
                continue
            if isinstance(v, list) and sorted(v, key=str) == sorted(v2, key=str):
                continue
            difference.append(f"{k}='{v}' was: '{v2}'")
    else:
        difference.append("new alarm")

    create_or_update_alarm_args = {
        "Name": name,
        "region": region,
        "key": key,
        "keyid": keyid,
        "profile": profile,
    }
    create_or_update_alarm_args.update(attributes)

    if alarm_details:
        if not difference:
            ret["comment"] = f"alarm {name} present and matching"
            return ret
        if __opts__["test"]:
            ret["comment"] = f"alarm {name} is to be created/updated."
            ret["result"] = None
            return ret
        result = __salt__["boto3_cloudwatch.create_or_update_alarm"](**create_or_update_alarm_args)
        if result:
            ret["changes"]["diff"] = difference
        else:
            ret["result"] = False
            ret["comment"] = f"Failed to create {name} alarm"
    else:
        if __opts__["test"]:
            ret["comment"] = f"alarm {name} is to be created/updated."
            ret["result"] = None
            return ret
        result = __salt__["boto3_cloudwatch.create_or_update_alarm"](**create_or_update_alarm_args)
        if result:
            ret["changes"]["new"] = attributes
        else:
            ret["result"] = False
            ret["comment"] = f"Failed to create {name} alarm"
    return ret


def absent(name, region=None, key=None, keyid=None, profile=None):
    """
    Ensure the named cloudwatch alarm is deleted.

    name
        Name of the alarm.

    region
        Region to connect to.

    key
        Secret key to be used.

    keyid
        Access key to be used.

    profile
        A dict with region, key and keyid, or a pillar key (string)
        that contains a dict with region, key and keyid.

    Example:

    .. code-block:: yaml

        ensure-absent:
          boto3_cloudwatch_alarm.absent:
            - name: example

    """
    ret = {"name": name, "result": True, "comment": "", "changes": {}}

    is_present = __salt__["boto3_cloudwatch.get_alarm"](
        name, region=region, key=key, keyid=keyid, profile=profile
    )

    if is_present:
        if __opts__["test"]:
            ret["comment"] = f"alarm {name} is set to be removed."
            ret["result"] = None
            return ret
        deleted = __salt__["boto3_cloudwatch.delete_alarm"](
            name, region=region, key=key, keyid=keyid, profile=profile
        )
        if deleted:
            ret["changes"]["old"] = name
            ret["changes"]["new"] = None
        else:
            ret["result"] = False
            ret["comment"] = f"Failed to delete {name} alarm."
    else:
        ret["comment"] = f"{name} does not exist in {region}."

    return ret
