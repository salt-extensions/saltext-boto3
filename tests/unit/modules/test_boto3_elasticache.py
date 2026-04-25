"""
Unit tests for the ``boto3_elasticache`` execution module.
"""

import pytest

from saltext.boto3.modules import boto3_elasticache

try:
    import botocore  # pylint: disable=unused-import

    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

pytestmark = [
    pytest.mark.skip_on_fips_enabled_platform,
    pytest.mark.skipif(HAS_BOTO3 is False, reason="The boto3 module must be installed."),
]


@pytest.fixture
def configure_loader_modules():
    return {
        boto3_elasticache: {
            "__opts__": {},
            "__context__": {},
            "__salt__": {},
        }
    }


@pytest.fixture
def conn(make_conn):
    with make_conn(boto3_elasticache) as client:
        yield client


def test_describe_cache_clusters_found(conn):
    conn.describe_cache_clusters.return_value = {
        "CacheClusters": [{"CacheClusterId": "c1", "CacheClusterStatus": "available"}]
    }
    result = boto3_elasticache.describe_cache_clusters(name="c1")
    assert result == [{"CacheClusterId": "c1", "CacheClusterStatus": "available"}]
    kwargs = conn.describe_cache_clusters.call_args.kwargs
    assert kwargs["CacheClusterId"] == "c1"


def test_describe_cache_clusters_client_error(conn, client_error):
    conn.describe_cache_clusters.side_effect = client_error("NotFound", "DescribeCacheClusters")
    assert boto3_elasticache.describe_cache_clusters(name="x") is None


def test_cache_cluster_exists_true(conn):
    conn.describe_cache_clusters.return_value = {"CacheClusters": [{"CacheClusterId": "c1"}]}
    assert boto3_elasticache.cache_cluster_exists("c1") is True


def test_cache_cluster_exists_false(conn, client_error):
    conn.describe_cache_clusters.side_effect = client_error("NotFound", "DescribeCacheClusters")
    assert boto3_elasticache.cache_cluster_exists("nope") is False


def test_create_cache_cluster_no_wait(conn):
    result = boto3_elasticache.create_cache_cluster(
        "c1", wait=0, Engine="redis", CacheNodeType="cache.t2.micro", NumCacheNodes=1
    )
    assert result is True
    kwargs = conn.create_cache_cluster.call_args.kwargs
    assert kwargs["CacheClusterId"] == "c1"
    assert kwargs["Engine"] == "redis"


def test_create_cache_cluster_bad_wait():
    # create_cache_cluster raises a plain Exception (not SaltInvocationError) for bad wait.
    with pytest.raises(Exception):  # pylint: disable=broad-except
        boto3_elasticache.create_cache_cluster("c1", wait="abc")


def test_create_cache_cluster_client_error(conn, client_error):
    conn.create_cache_cluster.side_effect = client_error("Bad", "CreateCacheCluster")
    assert boto3_elasticache.create_cache_cluster("c1", wait=0, Engine="redis") is False


def test_modify_cache_cluster_no_wait(conn):
    result = boto3_elasticache.modify_cache_cluster("c1", wait=0, NumCacheNodes=2)
    assert result is True
    kwargs = conn.modify_cache_cluster.call_args.kwargs
    assert kwargs["CacheClusterId"] == "c1"
    assert kwargs["NumCacheNodes"] == 2


def test_delete_cache_cluster_no_wait(conn):
    result = boto3_elasticache.delete_cache_cluster("c1", wait=0)
    assert result is True
    conn.delete_cache_cluster.assert_called_once()
    kwargs = conn.delete_cache_cluster.call_args.kwargs
    assert kwargs["CacheClusterId"] == "c1"


def test_delete_cache_cluster_client_error(conn, client_error):
    conn.delete_cache_cluster.side_effect = client_error("NotFound", "DeleteCacheCluster")
    assert boto3_elasticache.delete_cache_cluster("c1", wait=0) is False


def test_describe_replication_groups(conn):
    conn.describe_replication_groups.return_value = {
        "ReplicationGroups": [{"ReplicationGroupId": "r1"}]
    }
    result = boto3_elasticache.describe_replication_groups(name="r1")
    assert result == [{"ReplicationGroupId": "r1"}]


def test_replication_group_exists_true(conn):
    conn.describe_replication_groups.return_value = {
        "ReplicationGroups": [{"ReplicationGroupId": "r1"}]
    }
    assert boto3_elasticache.replication_group_exists("r1") is True


def test_replication_group_exists_false(conn, client_error):
    conn.describe_replication_groups.side_effect = client_error(
        "NotFound", "DescribeReplicationGroups"
    )
    assert boto3_elasticache.replication_group_exists("r1") is False


def test_describe_cache_subnet_groups(conn):
    conn.describe_cache_subnet_groups.return_value = {
        "CacheSubnetGroups": [{"CacheSubnetGroupName": "g1"}]
    }
    assert boto3_elasticache.describe_cache_subnet_groups(name="g1") == [
        {"CacheSubnetGroupName": "g1"}
    ]


def test_cache_subnet_group_exists_true(conn):
    conn.describe_cache_subnet_groups.return_value = {
        "CacheSubnetGroups": [{"CacheSubnetGroupName": "g1"}]
    }
    assert boto3_elasticache.cache_subnet_group_exists("g1") is True


def test_list_cache_subnet_groups(conn):
    conn.describe_cache_subnet_groups.return_value = {
        "CacheSubnetGroups": [
            {"CacheSubnetGroupName": "a"},
            {"CacheSubnetGroupName": "b"},
        ]
    }
    result = boto3_elasticache.list_cache_subnet_groups()
    assert result == ["a", "b"]


def test_describe_cache_security_groups(conn):
    conn.describe_cache_security_groups.return_value = {
        "CacheSecurityGroups": [{"CacheSecurityGroupName": "sg1"}]
    }
    assert boto3_elasticache.describe_cache_security_groups(name="sg1") == [
        {"CacheSecurityGroupName": "sg1"}
    ]


def test_cache_security_group_exists_false(conn):
    conn.describe_cache_security_groups.return_value = {"CacheSecurityGroups": []}
    assert boto3_elasticache.cache_security_group_exists("sg1") is False


def test_create_cache_security_group(conn):
    result = boto3_elasticache.create_cache_security_group("sg1", Description="My sg")
    assert result is True
    kwargs = conn.create_cache_security_group.call_args.kwargs
    assert kwargs["CacheSecurityGroupName"] == "sg1"
    assert kwargs["Description"] == "My sg"


def test_delete_cache_security_group(conn):
    assert boto3_elasticache.delete_cache_security_group("sg1") is True
    kwargs = conn.delete_cache_security_group.call_args.kwargs
    assert kwargs["CacheSecurityGroupName"] == "sg1"


def test_delete_cache_security_group_error(conn, client_error):
    conn.delete_cache_security_group.side_effect = client_error(
        "NotFound", "DeleteCacheSecurityGroup"
    )
    assert boto3_elasticache.delete_cache_security_group("sg1") is False


def test_list_tags_for_resource(conn):
    conn.list_tags_for_resource.return_value = {"TagList": [{"Key": "a", "Value": "b"}]}
    result = boto3_elasticache.list_tags_for_resource("arn:aws:elasticache:...")
    assert result == [{"Key": "a", "Value": "b"}]


def test_describe_cache_parameter_groups(conn):
    conn.describe_cache_parameter_groups.return_value = {
        "CacheParameterGroups": [{"CacheParameterGroupName": "p1"}]
    }
    assert boto3_elasticache.describe_cache_parameter_groups(name="p1") == [
        {"CacheParameterGroupName": "p1"}
    ]


def test_create_cache_parameter_group(conn):
    assert (
        boto3_elasticache.create_cache_parameter_group(
            "p1",
            CacheParameterGroupFamily="redis3.2",
            Description="d",
        )
        is True
    )
    kwargs = conn.create_cache_parameter_group.call_args.kwargs
    assert kwargs["CacheParameterGroupName"] == "p1"


def test_delete_cache_parameter_group(conn):
    assert boto3_elasticache.delete_cache_parameter_group("p1") is True
    kwargs = conn.delete_cache_parameter_group.call_args.kwargs
    assert kwargs["CacheParameterGroupName"] == "p1"
