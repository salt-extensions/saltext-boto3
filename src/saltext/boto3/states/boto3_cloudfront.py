"""
Manage CloudFront distributions using boto3.
============================================

    Renamed from ``boto_cloudfront`` to ``boto3_cloudfront`` and updated to call the
    refactored ``boto3_cloudfront`` execution module.

Create, update and destroy CloudFront distributions.

:depends:
  - boto3 >= 1.28.0
  - botocore >= 1.31.0

This module uses boto3, which can be installed via package, or pip.

This module accepts explicit CloudFront credentials but can also utilize
IAM roles assigned to the instance through Instance Profiles. Dynamic
credentials are then automatically obtained from AWS API and no further
configuration is necessary. More Information available at:

.. code-block:: text

    http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html

If IAM roles are not used you need to specify them either in the minion's config file
or as a profile. For example, to specify them in the minion's config file:

.. code-block:: yaml

    cloudfront.keyid: GKTADJGHEIQSXMKKRBJ08H
    cloudfront.key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs

It's also possible to specify key, keyid and region via a profile, either
as a passed in dict, or as a string to pull from pillars or minion config:

.. code-block:: yaml

    myprofile:
        keyid: GKTADJGHEIQSXMKKRBJ08H
        key: askdjghsdfjkghWupUjasdflkdfklgjsdfjajkghs
        region: us-east-1

.. code-block:: yaml

    Manage my_distribution CloudFront distribution:
        boto3_cloudfront.present:
          - name: my_distribution
          - config:
              Comment: 'partial config shown, most parameters elided'
              Enabled: True
          - tags:
              testing_key: testing_value

.. versionadded:: 1.0.0
"""

import difflib
import logging

import salt.utils.dictdiffer
import salt.utils.yaml

log = logging.getLogger(__name__)

__virtualname__ = "boto3_cloudfront"


def __virtual__():
    """
    Only load if the boto3_cloudfront execution module is available.
    """
    if "boto3_cloudfront.get_distribution" not in __salt__:
        return (
            False,
            "The boto3_cloudfront state module could not be loaded: "
            "boto3_cloudfront exec module unavailable.",
        )
    return __virtualname__


def present(
    name,
    config,
    tags,
    region=None,
    key=None,
    keyid=None,
    profile=None,
):
    """
    Ensure the CloudFront distribution is present.

    name (string)
        Name of the CloudFront distribution

    config (dict)
        Configuration for the distribution

    tags (dict)
        Tags to associate with the distribution

    region (string)
        Region to connect to

    key (string)
        Secret key to use

    keyid (string)
        Access key to use

    profile (dict or string)
        A dict with region, key, and keyid, or a pillar key (string) that
        contains such a dict.

    Example:

    .. code-block:: yaml

        Manage my_distribution CloudFront distribution:
            boto3_cloudfront.present:
              - name: my_distribution
              - config:
                  Comment: 'partial config shown, most parameters elided'
                  Enabled: True
              - tags:
                  testing_key: testing_value
    """
    ret = {"name": name, "comment": "", "changes": {}}

    res = __salt__["boto3_cloudfront.get_distribution"](
        name, region=region, key=key, keyid=keyid, profile=profile
    )
    if "error" in res:
        ret["result"] = False
        ret["comment"] = f"Error checking distribution {name}: {res['error']}"
        return ret

    old = res["result"]
    if old is None:
        if __opts__["test"]:
            ret["result"] = None
            ret["comment"] = f"Distribution {name} set for creation."
            ret["changes"] = {"old": None, "new": name}
            return ret

        res = __salt__["boto3_cloudfront.create_distribution"](
            name, config, tags, region=region, key=key, keyid=keyid, profile=profile
        )
        if "error" in res:
            ret["result"] = False
            ret["comment"] = f"Error creating distribution {name}: {res['error']}"
            return ret

        ret["result"] = True
        ret["comment"] = f"Created distribution {name}."
        ret["changes"] = {"old": None, "new": name}
        return ret

    full_config_old = {
        "config": old["distribution"]["DistributionConfig"],
        "tags": old["tags"],
    }
    full_config_new = {"config": config, "tags": tags}
    diffed_config = salt.utils.dictdiffer.deep_diff(full_config_old, full_config_new)

    def _yaml_safe_dump(attrs):
        dumper = salt.utils.yaml.get_dumper("IndentedSafeOrderedDumper")
        return salt.utils.yaml.dump(attrs, default_flow_style=False, Dumper=dumper)

    changes_diff = "".join(
        difflib.unified_diff(
            _yaml_safe_dump(full_config_old).splitlines(True),
            _yaml_safe_dump(full_config_new).splitlines(True),
        )
    )

    any_changes = bool("old" in diffed_config or "new" in diffed_config)
    if not any_changes:
        ret["result"] = True
        ret["comment"] = f"Distribution {name} has correct config."
        return ret

    if __opts__["test"]:
        ret["result"] = None
        ret["comment"] = "\n".join([f"Distribution {name} set for new config:", changes_diff])
        ret["changes"] = {"diff": changes_diff}
        return ret

    res = __salt__["boto3_cloudfront.update_distribution"](
        name, config, tags, region=region, key=key, keyid=keyid, profile=profile
    )
    if "error" in res:
        ret["result"] = False
        ret["comment"] = f"Error updating distribution {name}: {res['error']}"
        return ret

    ret["result"] = True
    ret["comment"] = f"Updated distribution {name}."
    ret["changes"] = {"diff": changes_diff}
    return ret
