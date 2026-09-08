"""CurationEngine-<env>: one Fargate task (SQLite on EFS) behind ALB + CloudFront/WAF, with a
task role that can drive the dev crawler (SSM) and the WEB_COSMOS indexer (ecs:RunTask) and read
the OpenSearch Serverless web index for validation."""

from __future__ import annotations

import json
from pathlib import Path

import aws_cdk as cdk
from aws_cdk import (
    Duration,
    RemovalPolicy,
    Stack,
)
from aws_cdk import (
    aws_cloudfront as cloudfront,
)
from aws_cdk import (
    aws_cloudfront_origins as origins,
)
from aws_cdk import (
    aws_ec2 as ec2,
)
from aws_cdk import (
    aws_ecr_assets as ecr_assets,
)
from aws_cdk import (
    aws_ecs as ecs,
)
from aws_cdk import (
    aws_efs as efs,
)
from aws_cdk import (
    aws_elasticloadbalancingv2 as elbv2,
)
from aws_cdk import (
    aws_iam as iam,
)
from aws_cdk import (
    aws_logs as logs,
)
from aws_cdk import (
    aws_opensearchserverless as aoss,
)
from aws_cdk import (
    aws_secretsmanager as sm,
)
from aws_cdk import (
    aws_ssm as ssm,
)
from aws_cdk import (
    aws_wafv2 as wafv2,
)
from constructs import Construct

from config import PARAMS, EnvConfig

REPO_ROOT = Path(__file__).resolve().parents[2]
CONTAINER_PORT = 8080
DATA_DIR = "/data"
POSIX_UID = "1000"  # matches the `app` user in the Dockerfile
# Managed prefix list of CloudFront origin-facing IPs (same id in every account of a region).
CLOUDFRONT_ORIGIN_PREFIX_LIST = {"us-east-1": "pl-3b927c52"}


class CurationEngineStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, *, cfg: EnvConfig, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        self.cfg = cfg

        vpc = self._vpc()
        public = ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC, availability_zones=list(cfg.azs))
        public_subnet_ids = vpc.select_subnets(subnet_type=ec2.SubnetType.PUBLIC,
                                               availability_zones=list(cfg.azs)).subnet_ids

        # Account-specific values, resolved by CloudFormation from SSM at deploy time (see config.py).
        p = {k: ssm.StringParameter.value_for_string_parameter(self, cfg.param_name(k)) for k in PARAMS}
        secrets = self._secrets()
        log_group = logs.LogGroup(
            self, "LogGroup", log_group_name=f"/ecs/{cfg.name}",
            retention=logs.RetentionDays.ONE_MONTH,
            removal_policy=RemovalPolicy.DESTROY,
        )

        # ── security groups ────────────────────────────────────────────
        alb_sg = ec2.SecurityGroup(self, "AlbSg", vpc=vpc, description=f"{cfg.name} ALB", allow_all_outbound=True)
        pl = CLOUDFRONT_ORIGIN_PREFIX_LIST.get(cfg.region)
        alb_sg.add_ingress_rule(
            ec2.Peer.prefix_list(pl) if pl else ec2.Peer.any_ipv4(), ec2.Port.tcp(80),
            "HTTP from CloudFront only" if pl else "HTTP",
        )
        svc_sg = ec2.SecurityGroup(self, "ServiceSg", vpc=vpc, description=f"{cfg.name} service", allow_all_outbound=True)
        svc_sg.add_ingress_rule(alb_sg, ec2.Port.tcp(CONTAINER_PORT), "from ALB")
        efs_sg = ec2.SecurityGroup(self, "EfsSg", vpc=vpc, description=f"{cfg.name} EFS", allow_all_outbound=False)
        efs_sg.add_ingress_rule(svc_sg, ec2.Port.tcp(2049), "NFS from service")

        # ── EFS: DATA_DIR (SQLite + per-collection yaml) ───────────────
        fs = efs.FileSystem(
            self, "Data", vpc=vpc, vpc_subnets=public, security_group=efs_sg, encrypted=True,
            lifecycle_policy=efs.LifecyclePolicy.AFTER_30_DAYS,
            performance_mode=efs.PerformanceMode.GENERAL_PURPOSE,
            throughput_mode=efs.ThroughputMode.ELASTIC,
            removal_policy=RemovalPolicy.RETAIN,
            file_system_name=f"{cfg.name}-data",
        )
        access_point = fs.add_access_point(
            "DataAp", path="/engine",
            posix_user=efs.PosixUser(uid=POSIX_UID, gid=POSIX_UID),
            create_acl=efs.Acl(owner_uid=POSIX_UID, owner_gid=POSIX_UID, permissions="750"),
        )

        # ── ECS ─────────────────────────────────────────────────────────
        cluster = ecs.Cluster(self, "Cluster", vpc=vpc, cluster_name=cfg.name, container_insights_v2=ecs.ContainerInsights.ENABLED)

        task_role = iam.Role(
            self, "TaskRole", role_name=f"{cfg.name}-task-role",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            description="sde-curation-engine: S3 hand-off, SSM to crawler, ecs:RunTask indexer, AOSS read",
        )
        self._grant_backend_access(task_role, access_point, p)

        task_def = ecs.FargateTaskDefinition(
            self, "TaskDef", family=cfg.name, cpu=cfg.cpu, memory_limit_mib=cfg.memory_mib,
            task_role=task_role,
            runtime_platform=ecs.RuntimePlatform(
                cpu_architecture=ecs.CpuArchitecture.X86_64, operating_system_family=ecs.OperatingSystemFamily.LINUX,
            ),
        )
        task_def.add_volume(
            name="data",
            efs_volume_configuration=ecs.EfsVolumeConfiguration(
                file_system_id=fs.file_system_id, transit_encryption="ENABLED",
                authorization_config=ecs.AuthorizationConfig(access_point_id=access_point.access_point_id, iam="ENABLED"),
            ),
        )

        # .dockerignore at the repo root decides what goes into the build context / asset hash.
        image = ecr_assets.DockerImageAsset(
            self, "Image", directory=str(REPO_ROOT), platform=ecr_assets.Platform.LINUX_AMD64,
        )
        environment = {
            "DATA_DIR": DATA_DIR,
            "DB_LOCKING_MODE": "exclusive",  # engine.db is on EFS
            "AUTH_COOKIE_SECURE": "true",  # viewers only ever reach us over CloudFront HTTPS
            "AWS_REGION": cfg.region,
            "SCRAPE_BACKEND": "ssm",
            "INDEX_BACKEND": "ecs",
            "CRAWLER_INSTANCE_ID": p["crawler_instance_id"],
            "CRAWLER_S3_BUCKET": p["crawler_bucket"],
            "CRAWLER_REMOTE_INBOX": cfg.crawler_inbox,
            "COSMOS_INDEX_BUCKET": p["cosmos_index_bucket"],
            "INDEXING_ECS_CLUSTER": p["indexing_cluster_name"],
            "INDEXING_TASK_FAMILY": p["indexing_task_family"],
            "INDEXING_CONTAINER_NAME": cfg.indexing_container_name,
            "INDEXING_SUBNETS": json.dumps(public_subnet_ids),
            "INDEXING_SECURITY_GROUPS": "[]",
            "INDEXING_ASSIGN_PUBLIC_IP": "true",
            "WEB_INDEX_NAME": cfg.web_index_name,
            "OPENSEARCH_ENDPOINT_TEST": p["opensearch_endpoint_test"],
            "OPENSEARCH_ENDPOINT_PROD": p["opensearch_endpoint_prod"],
            "LLM_PROVIDER": "openai",
            "OPENAI_MODEL": cfg.openai_model,
            "VALIDATION_DELAY_S": "30",
        }
        container = task_def.add_container(
            "engine",
            image=ecs.ContainerImage.from_docker_image_asset(image),
            logging=ecs.LogDrivers.aws_logs(stream_prefix="engine", log_group=log_group),
            environment=environment,
            secrets={
                "OPENAI_API_KEY": ecs.Secret.from_secrets_manager(secrets["openai_api_key"]),
                "APP_PASSWORD": ecs.Secret.from_secrets_manager(secrets["app_password"]),
                "SESSION_SECRET": ecs.Secret.from_secrets_manager(secrets["session_secret"]),
                "NOTIFY_WEBHOOK_URL": ecs.Secret.from_secrets_manager(secrets["notify_webhook_url"]),
            },
            port_mappings=[ecs.PortMapping(container_port=CONTAINER_PORT)],
            user=POSIX_UID,
            stop_timeout=Duration.seconds(60),
        )
        container.add_mount_points(ecs.MountPoint(container_path=DATA_DIR, source_volume="data", read_only=False))

        # Single writer to SQLite + in-process job registry → never two tasks at once.
        service = ecs.FargateService(
            self, "Service", cluster=cluster, task_definition=task_def, service_name=cfg.name,
            desired_count=1, min_healthy_percent=0, max_healthy_percent=100,
            assign_public_ip=True, vpc_subnets=public, security_groups=[svc_sg],
            platform_version=ecs.FargatePlatformVersion.LATEST,
            enable_execute_command=True,
            health_check_grace_period=Duration.seconds(120),
            circuit_breaker=ecs.DeploymentCircuitBreaker(enable=True, rollback=True),
        )

        # ── ALB (HTTP; TLS terminates at CloudFront) ──────────────────
        alb = elbv2.ApplicationLoadBalancer(
            self, "Alb", vpc=vpc, internet_facing=True, vpc_subnets=public, security_group=alb_sg,
            load_balancer_name=cfg.name, idle_timeout=Duration.seconds(3600),  # SSE streams
        )
        listener = alb.add_listener("Http", port=80, open=False)
        listener.add_targets(
            "Engine", port=CONTAINER_PORT, protocol=elbv2.ApplicationProtocol.HTTP, targets=[service],
            deregistration_delay=Duration.seconds(30),
            health_check=elbv2.HealthCheck(path="/health", healthy_http_codes="200", interval=Duration.seconds(30)),
        )
        # ── WAF + CloudFront ───────────────────────────────────────────
        web_acl = self._web_acl()
        distribution = cloudfront.Distribution(
            self, "Cdn", comment=f"{cfg.name}",
            default_behavior=cloudfront.BehaviorOptions(
                origin=origins.LoadBalancerV2Origin(
                    alb, protocol_policy=cloudfront.OriginProtocolPolicy.HTTP_ONLY,
                    read_timeout=Duration.seconds(60), keepalive_timeout=Duration.seconds(60),
                ),
                allowed_methods=cloudfront.AllowedMethods.ALLOW_ALL,
                cache_policy=cloudfront.CachePolicy.CACHING_DISABLED,
                origin_request_policy=cloudfront.OriginRequestPolicy.ALL_VIEWER,
                viewer_protocol_policy=cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
                compress=False,  # never buffer the SSE stream
            ),
            price_class=cloudfront.PriceClass.PRICE_CLASS_100,
            web_acl_id=web_acl.attr_arn,
            minimum_protocol_version=cloudfront.SecurityPolicyProtocol.TLS_V1_2_2021,
            http_version=cloudfront.HttpVersion.HTTP2_AND_3,
        )
        distribution.add_behavior(
            "/static/*", origins.LoadBalancerV2Origin(alb, protocol_policy=cloudfront.OriginProtocolPolicy.HTTP_ONLY),
            cache_policy=cloudfront.CachePolicy.CACHING_OPTIMIZED,
            viewer_protocol_policy=cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
        )

        # Service → TaskDef → Distribution → ALB; nothing points back at the service, so no cycle.
        container.add_environment(
            "PUBLIC_BASE_URL", cfg.public_base_url or f"https://{distribution.distribution_domain_name}"
        )

        # ── outputs ────────────────────────────────────────────────────
        cdk.CfnOutput(self, "CloudFrontUrl", value=f"https://{distribution.distribution_domain_name}")
        cdk.CfnOutput(self, "AlbDnsName", value=alb.load_balancer_dns_name)
        cdk.CfnOutput(self, "ClusterName", value=cluster.cluster_name)
        cdk.CfnOutput(self, "ServiceName", value=service.service_name)
        cdk.CfnOutput(self, "TaskRoleArn", value=task_role.role_arn)
        cdk.CfnOutput(self, "EfsId", value=fs.file_system_id)
        cdk.CfnOutput(self, "LogGroupName", value=log_group.log_group_name)
        for key, secret in secrets.items():
            cdk.CfnOutput(self, f"Secret{key.title().replace('_', '')}", value=secret.secret_name)

    # ── pieces ─────────────────────────────────────────────────────────

    def _vpc(self) -> ec2.IVpc:
        if self.cfg.vpc_id:
            return ec2.Vpc.from_lookup(self, "Vpc", vpc_id=self.cfg.vpc_id)
        return ec2.Vpc.from_lookup(self, "Vpc", is_default=True)

    def _secrets(self) -> dict[str, sm.Secret]:
        cfg = self.cfg
        out: dict[str, sm.Secret] = {}
        out["app_password"] = sm.Secret(
            self, "AppPassword", secret_name=cfg.secret_name("app_password"),
            description="Shared login password for the curation engine UI/API",
            generate_secret_string=sm.SecretStringGenerator(password_length=24, exclude_punctuation=True),
            removal_policy=RemovalPolicy.RETAIN,
        )
        out["session_secret"] = sm.Secret(
            self, "SessionSecret", secret_name=cfg.secret_name("session_secret"),
            description="HMAC key for the login cookie",
            generate_secret_string=sm.SecretStringGenerator(password_length=64, exclude_punctuation=True),
            removal_policy=RemovalPolicy.RETAIN,
        )
        # Placeholders: replace with `aws secretsmanager put-secret-value` after the first deploy.
        # (ECS refuses to start a task whose secret has no value, so they are seeded.)
        out["openai_api_key"] = sm.Secret(
            self, "OpenAiApiKey", secret_name=cfg.secret_name("openai_api_key"),
            description="OpenAI API key (LLM assist). Placeholder until set.",
            secret_string_value=cdk.SecretValue.unsafe_plain_text("REPLACE_ME"),
            removal_policy=RemovalPolicy.RETAIN,
        )
        out["notify_webhook_url"] = sm.Secret(
            self, "NotifyWebhookUrl", secret_name=cfg.secret_name("notify_webhook_url"),
            description="Slack-compatible webhook for status notifications (non-URL = disabled)",
            secret_string_value=cdk.SecretValue.unsafe_plain_text("disabled"),
            removal_policy=RemovalPolicy.RETAIN,
        )
        return out

    def _grant_backend_access(self, role: iam.Role, access_point: efs.IAccessPoint, p: dict[str, str]) -> None:
        cfg, a, r = self.cfg, self.account, self.region
        cosmos = f"arn:aws:s3:::{p['cosmos_index_bucket']}"
        crawler = f"arn:aws:s3:::{p['crawler_bucket']}"
        cluster_arn = f"arn:aws:ecs:{r}:{a}:cluster/{p['indexing_cluster_name']}"
        role.add_to_policy(iam.PolicyStatement(
            sid="CosmosHandoffBucket", actions=["s3:ListBucket"], resources=[cosmos],
        ))
        role.add_to_policy(iam.PolicyStatement(
            sid="CosmosHandoffObjects", actions=["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
            resources=[f"{cosmos}/curated_collections/*", f"{cosmos}/index_runs/*"],
        ))
        role.add_to_policy(iam.PolicyStatement(
            sid="CrawlerBucketRead", actions=["s3:ListBucket"], resources=[crawler],
        ))
        role.add_to_policy(iam.PolicyStatement(
            sid="CrawlerObjectsRead", actions=["s3:GetObject"],
            resources=[f"{crawler}/scraped_collections/*", f"{crawler}/failure_logs/*"],
        ))
        role.add_to_policy(iam.PolicyStatement(
            sid="CrawlerSsmSend", actions=["ssm:SendCommand"],
            resources=[f"arn:aws:ec2:{r}:{a}:instance/{p['crawler_instance_id']}",
                       f"arn:aws:ssm:{r}::document/AWS-RunShellScript"],
        ))
        role.add_to_policy(iam.PolicyStatement(
            sid="CrawlerSsmRead", actions=["ssm:GetCommandInvocation"], resources=["*"],
        ))
        # Same statements as CosmosIndexingDispatchRole-<env> (sde-api-scrapers), granted directly
        # because that role's trust policy only admits indexing-helper-role.
        role.add_to_policy(iam.PolicyStatement(
            sid="RunWebCosmosTask", actions=["ecs:RunTask"],
            resources=[f"arn:aws:ecs:{r}:{a}:task-definition/{p['indexing_task_family']}:*"],
            conditions={"ArnEquals": {"ecs:cluster": cluster_arn}},
        ))
        role.add_to_policy(iam.PolicyStatement(
            sid="DescribeWebCosmosTasks", actions=["ecs:DescribeTasks"],
            resources=[f"arn:aws:ecs:{r}:{a}:task/{p['indexing_cluster_name']}/*"],
        ))
        role.add_to_policy(iam.PolicyStatement(
            sid="PassEcsRoles", actions=["iam:PassRole"],
            resources=[p["indexing_task_role_arn"], p["indexing_execution_role_arn"]],
            conditions={"StringEquals": {"iam:PassedToService": "ecs-tasks.amazonaws.com"}},
        ))
        role.add_to_policy(iam.PolicyStatement(
            sid="AossApi", actions=["aoss:APIAccessAll"],
            resources=[f"arn:aws:aoss:{r}:{a}:collection/{p['aoss_collection_id']}"],
        ))
        role.add_to_policy(iam.PolicyStatement(
            sid="EfsMount",
            actions=["elasticfilesystem:ClientMount", "elasticfilesystem:ClientWrite"],
            resources=[access_point.file_system.file_system_arn],
            conditions={"StringEquals": {"elasticfilesystem:AccessPointArn": access_point.access_point_arn}},
        ))
        # AOSS data-access is separate from IAM: our own policy so nothing owned by other stacks
        # (sde-services-access, …) has to change. Read-only — the engine only validates.
        aoss.CfnAccessPolicy(
            self, "AossDataAccess", name=f"{cfg.name}"[:32], type="data",
            description="sde-curation-engine validation reads on the web index",
            policy=cdk.Fn.sub(json.dumps([{
                "Description": "curation engine read access",
                "Principal": ["${TaskRoleArn}"],
                "Rules": [
                    {"ResourceType": "collection", "Resource": ["collection/${Collection}"],
                     "Permission": ["aoss:DescribeCollectionItems"]},
                    {"ResourceType": "index", "Resource": ["index/${Collection}/sde-web*"],
                     "Permission": ["aoss:DescribeIndex", "aoss:ReadDocument"]},
                ],
            }]), {"TaskRoleArn": role.role_arn, "Collection": p["aoss_collection_name"]}),
        )

    def _web_acl(self) -> wafv2.CfnWebACL:
        cfg = self.cfg
        return wafv2.CfnWebACL(
            self, "WebAcl", name=f"{cfg.name}-waf", scope="CLOUDFRONT",
            default_action=wafv2.CfnWebACL.DefaultActionProperty(allow={}),
            visibility_config=wafv2.CfnWebACL.VisibilityConfigProperty(
                cloud_watch_metrics_enabled=True, metric_name=f"{cfg.name}-waf", sampled_requests_enabled=True,
            ),
            rules=[
                wafv2.CfnWebACL.RuleProperty(
                    name="AWSManagedRulesCommonRuleSet", priority=1,
                    override_action=wafv2.CfnWebACL.OverrideActionProperty(none={}),
                    statement=wafv2.CfnWebACL.StatementProperty(
                        managed_rule_group_statement=wafv2.CfnWebACL.ManagedRuleGroupStatementProperty(
                            vendor_name="AWS", name="AWSManagedRulesCommonRuleSet",
                            # SizeRestrictions_BODY (8 KB) would block large pattern/URL edits
                            rule_action_overrides=[wafv2.CfnWebACL.RuleActionOverrideProperty(
                                name="SizeRestrictions_BODY",
                                action_to_use=wafv2.CfnWebACL.RuleActionProperty(count={}),
                            )],
                        ),
                    ),
                    visibility_config=wafv2.CfnWebACL.VisibilityConfigProperty(
                        cloud_watch_metrics_enabled=True, metric_name="common", sampled_requests_enabled=True,
                    ),
                ),
                wafv2.CfnWebACL.RuleProperty(
                    name="RateLimit", priority=2,
                    action=wafv2.CfnWebACL.RuleActionProperty(block={}),
                    statement=wafv2.CfnWebACL.StatementProperty(
                        rate_based_statement=wafv2.CfnWebACL.RateBasedStatementProperty(
                            limit=cfg.waf_rate_limit_per_5min, aggregate_key_type="IP",
                        ),
                    ),
                    visibility_config=wafv2.CfnWebACL.VisibilityConfigProperty(
                        cloud_watch_metrics_enabled=True, metric_name="rate", sampled_requests_enabled=True,
                    ),
                ),
            ],
        )
