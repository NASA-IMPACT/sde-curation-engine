"""Synth-level assertions on the dev stack (no AWS calls: the VPC lookup is stubbed via context;
account-specific values are SSM parameters resolved only at deploy time)."""

import aws_cdk as cdk
import pytest
from aws_cdk.assertions import Match, Template

from config import PARAMS, get_config
from stacks.engine_stack import CurationEngineStack

ACCOUNT, REGION = "123456789012", "us-east-1"
VPC_CONTEXT = {
    "vpcId": "vpc-test", "vpcCidrBlock": "172.31.0.0/16", "ownerAccountId": ACCOUNT,
    "availabilityZones": [], "subnetGroups": [{"name": "Public", "type": "Public", "subnets": [
        {"subnetId": "subnet-a", "cidr": "172.31.0.0/20", "availabilityZone": "us-east-1a", "routeTableId": "rtb-a"},
        {"subnetId": "subnet-b", "cidr": "172.31.16.0/20", "availabilityZone": "us-east-1b", "routeTableId": "rtb-b"},
    ]}],
}


def synth(env: str) -> Template:
    cfg = get_config(env)
    key = f"vpc-provider:account={ACCOUNT}:filter.isDefault=true:region={REGION}:returnAsymmetricSubnets=true"
    app = cdk.App(context={key: VPC_CONTEXT, "@aws-cdk/core:bootstrapQualifier": "sde"})
    stack = CurationEngineStack(app, f"CurationEngine-{env}", cfg=cfg,
                                env=cdk.Environment(account=ACCOUNT, region=REGION))
    return Template.from_stack(stack)


@pytest.fixture(scope="module")
def template() -> Template:
    return synth("dev")


def test_every_account_value_is_a_deploy_time_ssm_parameter(template):
    params = template.to_json()["Parameters"]
    ssm_defaults = {v["Default"] for v in params.values()
                    if v["Type"] == "AWS::SSM::Parameter::Value<String>" and v["Default"].startswith("/sde-curation-engine/")}
    assert ssm_defaults == {f"/sde-curation-engine/dev/{k}" for k in PARAMS}


def test_single_task_service(template):
    template.resource_count_is("AWS::ECS::Service", 1)
    template.has_resource_properties("AWS::ECS::Service", {
        "DesiredCount": 1, "LaunchType": "FARGATE",
        "DeploymentConfiguration": Match.object_like({"MinimumHealthyPercent": 0, "MaximumPercent": 100}),
        "NetworkConfiguration": {"AwsvpcConfiguration": Match.object_like({"AssignPublicIp": "ENABLED"})},
    })


def test_efs_mounted_at_data(template):
    template.resource_count_is("AWS::EFS::FileSystem", 1)
    template.has_resource_properties("AWS::ECS::TaskDefinition", {
        "ContainerDefinitions": [Match.object_like({
            "MountPoints": [{"ContainerPath": "/data", "SourceVolume": "data", "ReadOnly": False}],
            "Environment": Match.array_with([  # insertion order
                {"Name": "DATA_DIR", "Value": "/data"},
                {"Name": "DB_HOST", "Value": {"Fn::GetAtt": [Match.any_value(), "Endpoint.Address"]}},
                {"Name": "DB_NAME", "Value": "engine"},
                {"Name": "DB_SSLMODE", "Value": "require"},
                {"Name": "SCRAPE_BACKEND", "Value": "ssm"},
                {"Name": "INDEX_BACKEND", "Value": "ecs"},
                {"Name": "CRAWLER_INSTANCE_ID", "Value": {"Ref": Match.any_value()}},
                {"Name": "INDEXING_SUBNETS", "Value": '["subnet-a", "subnet-b"]'},
            ]),
            "Secrets": Match.array_with([Match.object_like({"Name": "DB_USER"}),
                                         Match.object_like({"Name": "DB_PASSWORD"}),
                                         Match.object_like({"Name": "OPENAI_API_KEY"}),
                                         Match.object_like({"Name": "APP_PASSWORD"})]),
        })],
        "Volumes": [Match.object_like({"EFSVolumeConfiguration": Match.object_like({"TransitEncryption": "ENABLED"})})],
    })


def test_rds_postgres_is_private_encrypted_and_backed_up(template):
    template.resource_count_is("AWS::RDS::DBInstance", 1)
    template.has_resource_properties("AWS::RDS::DBInstance", {
        "Engine": "postgres", "EngineVersion": Match.string_like_regexp("^17"), "DBName": "engine",
        "DBInstanceClass": "db.t4g.medium", "PubliclyAccessible": False, "StorageEncrypted": True,
        "StorageType": "gp3", "AllocatedStorage": "20", "MaxAllocatedStorage": 100,
        "MultiAZ": False, "BackupRetentionPeriod": 7, "DeletionProtection": False,
        "EnablePerformanceInsights": True, "EnableCloudwatchLogsExports": ["postgresql"],
    })
    template.has_resource("AWS::RDS::DBInstance", {"DeletionPolicy": "Snapshot", "UpdateReplacePolicy": "Snapshot"})
    # only the service may reach port 5432
    template.has_resource_properties("AWS::EC2::SecurityGroupIngress", {
        "FromPort": 5432, "ToPort": 5432, "IpProtocol": "tcp",
        "SourceSecurityGroupId": {"Fn::GetAtt": [Match.string_like_regexp("^ServiceSg"), "GroupId"]},
    })
    # the password is the generated secret's JSON field, never a plain env var
    props = template.to_json()["Resources"]
    (task_def,) = [r for r in props.values() if r["Type"] == "AWS::ECS::TaskDefinition"]
    env = {e["Name"] for e in task_def["Properties"]["ContainerDefinitions"][0]["Environment"]}
    assert "DB_PASSWORD" not in env and "DB_LOCKING_MODE" not in env
    (secret,) = [r for r in props.values() if r["Type"] == "AWS::SecretsManager::Secret"
                 and r["Properties"].get("Name") == "/sde-curation-engine/dev/db"]
    assert secret["Properties"]["GenerateSecretString"]["SecretStringTemplate"] == '{"username":"engine"}'


def test_prod_database_is_multi_az_and_protected():
    synth("prod").has_resource_properties("AWS::RDS::DBInstance", {
        "DBInstanceClass": "db.t4g.large", "MultiAZ": True, "BackupRetentionPeriod": 35, "DeletionProtection": True,
    })
    synth("test").has_resource_properties("AWS::RDS::DBInstance", {
        "DBInstanceClass": "db.t4g.large", "MultiAZ": False, "BackupRetentionPeriod": 7, "DeletionProtection": False,
    })


def test_alb_health_and_sse_timeout(template):
    template.has_resource_properties("AWS::ElasticLoadBalancingV2::TargetGroup", {"HealthCheckPath": "/health"})
    template.has_resource_properties("AWS::ElasticLoadBalancingV2::LoadBalancer", {
        "LoadBalancerAttributes": Match.array_with([{"Key": "idle_timeout.timeout_seconds", "Value": "3600"}]),
    })
    template.has_resource_properties("AWS::EC2::SecurityGroupIngress", {
        "SourcePrefixListId": "pl-3b927c52", "FromPort": 80, "ToPort": 80, "IpProtocol": "tcp",
    })


def test_cloudfront_fronts_alb_with_waf(template):
    template.resource_count_is("AWS::CloudFront::Distribution", 1)
    template.resource_count_is("AWS::WAFv2::WebACL", 1)
    template.has_resource_properties("AWS::CloudFront::Distribution", {
        "DistributionConfig": Match.object_like({
            "DefaultCacheBehavior": Match.object_like({
                "ViewerProtocolPolicy": "redirect-to-https",
                "CachePolicyId": "4135ea2d-6df8-44a3-9df3-4b5a84be39ad",  # CachingDisabled
            }),
            "Origins": Match.array_with([Match.object_like({
                "CustomOriginConfig": Match.object_like({"OriginProtocolPolicy": "http-only", "OriginReadTimeout": 60}),
            })]),
        }),
    })


def test_static_cache_keys_on_query_string(template):
    """/static/x.css?v=<hash> must miss the cache after a deploy (CachingOptimized ignores ?v=)."""
    template.resource_count_is("AWS::CloudFront::CachePolicy", 1)
    template.has_resource_properties("AWS::CloudFront::CachePolicy", {
        "CachePolicyConfig": Match.object_like({
            "ParametersInCacheKeyAndForwardedToOrigin": Match.object_like({
                "QueryStringsConfig": {"QueryStringBehavior": "all"},
                "EnableAcceptEncodingGzip": True,
            }),
        }),
    })
    template.has_resource_properties("AWS::CloudFront::Distribution", {
        "DistributionConfig": Match.object_like({
            "CacheBehaviors": [Match.object_like({"PathPattern": "/static/*", "CachePolicyId": {"Ref": Match.any_value()}})],
        }),
    })


def test_task_role_can_drive_indexer_and_crawler(template):
    template.has_resource_properties("AWS::IAM::Policy", {
        "PolicyDocument": {"Statement": Match.array_with([  # statement order
            Match.object_like({"Action": "ssm:SendCommand"}),
            Match.object_like({
                "Action": "ecs:RunTask",
                "Resource": {"Fn::Join": ["", [Match.string_like_regexp(":task-definition/$"), {"Ref": Match.any_value()}, ":*"]]},
                "Condition": {"ArnEquals": {"ecs:cluster": {"Fn::Join": Match.any_value()}}},
            }),
            Match.object_like({"Action": "iam:PassRole", "Resource": [{"Ref": Match.any_value()}, {"Ref": Match.any_value()}]}),
            Match.object_like({"Action": "aoss:APIAccessAll"}),
        ])},
    })
    template.has_resource_properties("AWS::OpenSearchServerless::AccessPolicy", {"Type": "data", "Name": "sde-curation-engine-dev"})


def test_all_environments_have_config():
    for env in ("dev", "test", "prod"):
        assert get_config(env).name == f"sde-curation-engine-{env}"
