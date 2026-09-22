"""Synth-level assertions on the dev stack (no AWS calls: the VPC lookup is stubbed via context;
account-specific values are SSM parameters resolved only at deploy time)."""

import json

import aws_cdk as cdk
import pytest
from aws_cdk.assertions import Match, Template

from config import PARAMS, ROLE_PARAMS, get_config
from stacks.engine_stack import CurationEngineStack

ACCOUNT, REGION = "123456789012", "us-east-1"
VPC_CONTEXT = {
    "vpcId": "vpc-test", "vpcCidrBlock": "172.31.0.0/16", "ownerAccountId": ACCOUNT,
    "availabilityZones": [], "subnetGroups": [{"name": "Public", "type": "Public", "subnets": [
        {"subnetId": "subnet-a", "cidr": "172.31.0.0/20", "availabilityZone": "us-east-1a", "routeTableId": "rtb-a"},
        {"subnetId": "subnet-b", "cidr": "172.31.16.0/20", "availabilityZone": "us-east-1b", "routeTableId": "rtb-b"},
        {"subnetId": "subnet-c", "cidr": "172.31.32.0/20", "availabilityZone": "us-east-1c", "routeTableId": "rtb-c"},
        {"subnetId": "subnet-e", "cidr": "172.31.64.0/20", "availabilityZone": "us-east-1e", "routeTableId": "rtb-e"},
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
        "DBInstanceClass": "db.m6i.large", "PubliclyAccessible": False, "StorageEncrypted": True,
        "StorageType": "gp3", "AllocatedStorage": "20", "MaxAllocatedStorage": 200,
        "MultiAZ": False, "BackupRetentionPeriod": 7, "DeletionProtection": False,
        "EnablePerformanceInsights": True, "EnableCloudwatchLogsExports": ["postgresql"],
    })
    template.has_resource("AWS::RDS::DBInstance", {"DeletionPolicy": "Snapshot", "UpdateReplacePolicy": "Snapshot"})
    # the subnet group is wider than the task's AZs (a, b) but never includes us-east-1e
    template.has_resource_properties("AWS::RDS::DBSubnetGroup", {"SubnetIds": ["subnet-a", "subnet-b", "subnet-c"]})
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
        "DBInstanceClass": "db.m6i.large", "MultiAZ": True, "BackupRetentionPeriod": 35, "DeletionProtection": True,
    })
    # test carries every collection's page text and is where the big crawls land, so it autoscales
    # further than the others; the volume is billed on what is allocated, not on this ceiling
    synth("test").has_resource_properties("AWS::RDS::DBInstance", {
        "DBInstanceClass": "db.m6i.large", "MultiAZ": False, "BackupRetentionPeriod": 7, "DeletionProtection": False,
        "MaxAllocatedStorage": 500,
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


def _policy_statements(template: Template) -> list[dict]:
    return [s for p in template.find_resources("AWS::IAM::Policy").values()
            for s in p["Properties"]["PolicyDocument"]["Statement"]]


def _container_env(template: Template) -> dict:
    [td] = template.find_resources("AWS::ECS::TaskDefinition").values()
    return {e["Name"]: e["Value"] for e in td["Properties"]["ContainerDefinitions"][0]["Environment"]}


def test_dev_publishes_into_its_own_collection(template):
    sids = {s.get("Sid") for s in _policy_statements(template)}
    assert "CosmosVectorizedRead" in sids and "AssumeProdIndexRole" not in sids
    assert "PROD_INDEX_ROLE_ARN" not in _container_env(template)
    [policy] = template.find_resources("AWS::OpenSearchServerless::AccessPolicy").values()
    body = str(policy["Properties"]["Policy"])
    assert "index/${Collection}/sde-web-subset" in body and "aoss:WriteDocument" in body


@pytest.mark.parametrize("env", ["dev", "test"])
def test_aoss_data_policy_has_one_rule_per_resource_type_per_statement(env):
    # AOSS rejects the policy otherwise ("There should only be 1 rule for index ResourceType ...")
    [policy] = synth(env).find_resources("AWS::OpenSearchServerless::AccessPolicy").values()
    for stmt in json.loads(policy["Properties"]["Policy"]["Fn::Sub"][0]):
        types = [r["ResourceType"] for r in stmt["Rules"]]
        assert len(types) == len(set(types)), stmt


@pytest.fixture(scope="module")
def test_template() -> Template:
    return synth("test")


def test_test_env_publishes_to_prod_through_an_assumed_role(test_template):
    params = test_template.to_json()["Parameters"]
    ssm_defaults = {v["Default"] for v in params.values()
                    if v["Type"] == "AWS::SSM::Parameter::Value<String>" and v["Default"].startswith("/sde-curation-engine/")}
    assert ssm_defaults == {f"/sde-curation-engine/test/{k}" for k in {**PARAMS, **ROLE_PARAMS}}
    stmts = {s.get("Sid"): s for s in _policy_statements(test_template)}
    assert stmts["AssumeProdIndexRole"]["Action"] == "sts:AssumeRole"
    assert stmts["CosmosVectorizedRead"]["Action"] == "s3:GetObject"
    # the crawler writes at the bucket root in test
    assert stmts["CrawlerObjectsRead"]["Resource"][0]["Fn::Join"][1][-1] == "/scraped_collections/*"
    env = _container_env(test_template)
    assert "PROD_INDEX_ROLE_ARN" in env and env["CRAWLER_S3_PREFIX"] == "" and env["WEB_INDEX_NAME"] == "sde-web"
    [policy] = test_template.find_resources("AWS::OpenSearchServerless::AccessPolicy").values()
    assert "aoss:WriteDocument" not in str(policy["Properties"]["Policy"])


def test_stress_context_is_dev_only_and_sizes_the_task():
    import pytest

    from config import Environment, get_config, stress_config

    cfg = stress_config(get_config("dev"))
    assert (cfg.cpu, cfg.memory_mib, cfg.llm_provider) == (2048, 8192, "fake") and cfg.env is Environment.DEV
    assert get_config("dev").llm_provider == "openai" and get_config("dev").cpu == 1024  # a plain deploy undoes it
    for env in ("test", "prod"):
        with pytest.raises(ValueError):
            stress_config(get_config(env))
