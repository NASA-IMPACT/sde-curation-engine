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
            # the indexer's tasks only: described while a run is watched, stopped when a curator cancels it
            Match.object_like({"Action": ["ecs:DescribeTasks", "ecs:StopTask"],
                               "Resource": {"Fn::Join": ["", Match.array_with([Match.string_like_regexp(":task/$")])]}}),
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
    assert "index/${Collection}/sde-web" in body and "aoss:WriteDocument" in body


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
    assert (cfg.cpu, cfg.memory_mib, cfg.llm_provider) == (4096, 16384, "fake") and cfg.env is Environment.DEV
    assert get_config("dev").llm_provider == "openai"  # a plain deploy undoes it
    for env in ("test", "prod"):
        with pytest.raises(ValueError):
            stress_config(get_config(env))


def test_dev_task_is_sized_like_test():
    from config import get_config

    dev, test = get_config("dev"), get_config("test")
    assert (dev.cpu, dev.memory_mib) == (test.cpu, test.memory_mib)


def _waf_rate_limit(template: Template) -> int:
    [acl] = template.find_resources("AWS::WAFv2::WebACL").values()
    [rule] = [r for r in acl["Properties"]["Rules"] if r["Name"] == "RateLimit"]
    return rule["Statement"]["RateBasedStatement"]["Limit"]


def test_test_waf_rate_limit_fits_curators_behind_one_ip(template, test_template):
    # curators share one egress IP; at 1,000 / 5 min a running job's refreshes got them CloudFront 403s
    assert _waf_rate_limit(test_template) == 10_000
    assert _waf_rate_limit(template) == 1_000  # dev keeps the default


def test_waf_lets_crawled_urls_with_dot_segments_through(template):
    # an exclude on https://simbad.cds.unistra.fr/simbad/../guide/otypes.htx got a CloudFront 403
    # (body), and so did its rule's match-count link on the Rules tab (?match=<url>, query string)
    [acl] = template.find_resources("AWS::WAFv2::WebACL").values()
    [rule] = [r for r in acl["Properties"]["Rules"] if r["Name"] == "AWSManagedRulesCommonRuleSet"]
    overrides = rule["Statement"]["ManagedRuleGroupStatement"]["RuleActionOverrides"]
    assert {o["Name"]: o["ActionToUse"] for o in overrides} == {
        "SizeRestrictions_BODY": {"Count": {}}, "GenericLFI_BODY": {"Count": {}},
        "GenericLFI_QUERYARGUMENTS": {"Count": {}},
    }


def test_database_records_query_statistics_and_slow_statements(template):
    """pg_stat_statements is preloaded (schema V13 creates the extension) and every statement over
    2 s is logged, so the next load incident can be traced to its statement while it happens."""
    template.resource_count_is("AWS::RDS::DBParameterGroup", 1)
    template.has_resource_properties("AWS::RDS::DBParameterGroup", {
        "Family": "postgres17",
        "Parameters": {
            "shared_preload_libraries": "pg_stat_statements",
            "pg_stat_statements.track": "top",
            "log_min_duration_statement": "2000",
        },
    })
    params = template.find_resources("AWS::RDS::DBParameterGroup")
    (pg_id,) = params
    template.has_resource_properties("AWS::RDS::DBInstance", {"DBParameterGroupName": {"Ref": pg_id}})


def test_alarms_notify_one_topic(template):
    """Database CPU, 5xx answers, slow answers and an unhealthy engine each raise an alarm on one SNS
    topic (subscribed by hand)."""
    template.resource_count_is("AWS::SNS::Topic", 1)
    (topic_id,) = template.find_resources("AWS::SNS::Topic")
    alarms = template.find_resources("AWS::CloudWatch::Alarm")
    by_metric = {a["Properties"]["MetricName"]: a["Properties"] for a in alarms.values()}
    assert set(by_metric) >= {"CPUUtilization", "HTTPCode_Target_5XX_Count", "TargetResponseTime", "UnHealthyHostCount"}
    cpu = by_metric["CPUUtilization"]
    assert (cpu["Namespace"], cpu["Threshold"], cpu["EvaluationPeriods"], cpu["Period"]) == ("AWS/RDS", 70, 5, 60)
    errors = by_metric["HTTPCode_Target_5XX_Count"]
    assert (errors["Threshold"], errors["EvaluationPeriods"], errors["Period"], errors["Statistic"]) == (10, 1, 300, "Sum")
    slow = by_metric["TargetResponseTime"]
    assert (slow["Threshold"], slow["EvaluationPeriods"], slow["ExtendedStatistic"]) == (5, 5, "p95")
    unhealthy = by_metric["UnHealthyHostCount"]
    assert (unhealthy["Threshold"], unhealthy["EvaluationPeriods"],
            unhealthy["ComparisonOperator"]) == (1, 2, "GreaterThanOrEqualToThreshold")
    memory = by_metric["FreeableMemory"]
    assert (memory["Threshold"], memory["EvaluationPeriods"], memory["ComparisonOperator"]) == (
        1_000_000_000, 5, "LessThanThreshold")
    conns = by_metric["DatabaseConnections"]
    assert (conns["Threshold"], conns["EvaluationPeriods"], conns["Namespace"]) == (80, 5, "AWS/RDS")
    engine = by_metric["MemoryUtilization"]
    assert (engine["Threshold"], engine["EvaluationPeriods"], engine["Namespace"]) == (85, 5, "AWS/ECS")
    for props in by_metric.values():
        assert props["AlarmActions"] == [{"Ref": topic_id}]
        assert props["TreatMissingData"] == "notBreaching"


def test_the_task_carries_the_per_job_and_the_shared_llm_limits(template):
    """#24: LLM_WORKERS limits one job; LLM_WORKERS_TOTAL limits every LLM job on the task together."""
    env = _container_env(template)
    assert (env["LLM_WORKERS"], env["LLM_WORKERS_TOTAL"]) == ("16", "32")


# ── Known bugs from REVIEW-SINCE-DEV-MERGE-2026-10-08.md (expected failures until fixed) ──────────
# Each test states the correct behaviour. While the bug exists it fails and is reported as XFAIL;
# once fixed it passes, and strict=True fails the run until the marker is removed.

# The engine's own maximum: DB_POOL_SIZE (16) + DB_READ_POOL_SIZE (12), sde_curation/config.py defaults.
ENGINE_MAX_DB_CONNECTIONS = 28


@pytest.mark.xfail(strict=True, reason="M6: the alarm topic has no subscription, so no one is notified")
def test_the_alarm_topic_notifies_someone(template):
    subs = template.find_resources("AWS::SNS::Subscription")
    (topic,) = template.find_resources("AWS::SNS::Topic").values()
    assert subs or topic["Properties"].get("Subscription")


@pytest.mark.xfail(strict=True, reason="M7: no alarm fires when the engine has no healthy target (fast crash loop)")
def test_an_alarm_fires_when_no_engine_is_healthy(template):
    alarms = [a["Properties"] for a in template.find_resources("AWS::CloudWatch::Alarm").values()]
    healthy = [a for a in alarms if a.get("MetricName") == "HealthyHostCount"]
    assert healthy and healthy[0]["ComparisonOperator"] == "LessThanThreshold" and healthy[0]["Threshold"] == 1
    assert healthy[0]["TreatMissingData"] == "breaching"
    assert any(a.get("MetricName") == "HTTPCode_ELB_5XX_Count" for a in alarms)


@pytest.mark.xfail(strict=True, reason="L12: the database-connections alarm (80) cannot fire from the engine (max 28)")
def test_the_connections_alarm_can_fire_from_the_engine(template):
    alarms = [a["Properties"] for a in template.find_resources("AWS::CloudWatch::Alarm").values()]
    (conns,) = [a for a in alarms if a.get("MetricName") == "DatabaseConnections"]
    assert conns["Threshold"] < ENGINE_MAX_DB_CONNECTIONS


@pytest.mark.xfail(strict=True, reason="L14: ecs:StopTask covers every task in the indexer cluster, not only the engine's")
def test_the_engine_can_stop_only_its_own_indexer_tasks(template):
    (stop,) = [s for s in _policy_statements(template)
               if "ecs:StopTask" in (s["Action"] if isinstance(s["Action"], list) else [s["Action"]])]
    assert "Condition" in stop


@pytest.mark.xfail(strict=True, reason="L11: the slow-statement log writes full bind parameters, kept forever")
def test_the_slow_statement_log_is_bounded(template):
    """log_min_duration_statement logs each slow statement with its parameters (5,000-URL arrays).
    The parameters are cut to a short prefix, and the exported database log expires."""
    (params,) = template.find_resources("AWS::RDS::DBParameterGroup").values()
    assert int(params["Properties"]["Parameters"].get("log_parameter_max_length", -1)) in range(1025)
    assert any(r["Properties"].get("LogGroupName", {}) and "postgresql" in json.dumps(r["Properties"]["LogGroupName"])
               for r in template.find_resources("AWS::Logs::LogGroup").values()) or \
        template.find_resources("Custom::LogRetention")
