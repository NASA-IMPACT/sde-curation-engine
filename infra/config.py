"""Per-environment settings that are safe to commit: sizes, knobs, and the *names* of the SSM
parameters holding everything account-specific (instance ids, buckets, endpoints, role ARNs).

The values themselves live in SSM Parameter Store of the target account under
`/sde-curation-engine/<env>/<key>` and are resolved by CloudFormation at deploy time, so they never
appear in git, in the synthesized template, or in cdk.out. Seed them with `make infra-seed ENV=…`
(reads infra/envs/<env>.json, which is gitignored; infra/envs/example.json shows the shape).
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

APP_NAME = "sde-curation-engine"


class Environment(str, Enum):
    DEV = "dev"
    TEST = "test"
    PROD = "prod"


# key → what it is. Every one is a plain String parameter; every one is required.
PARAMS: dict[str, str] = {
    "crawler_instance_id": "EC2 instance of the crawler (sde-crawl4ai-scraper-v1, SdeCrawlerStack)",
    "crawler_bucket": "S3 bucket the crawler writes <crawler_s3_prefix>/scraped_collections/ and failure_logs/ to",
    "indexing_cluster_name": "ECS cluster of the WEB_COSMOS indexer (sde-api-scrapers)",
    "indexing_task_family": "task definition family of the WEB_COSMOS indexer",
    "indexing_task_role_arn": "task role ARN of the indexer task definition (iam:PassRole)",
    "indexing_execution_role_arn": "execution role ARN of the indexer task definition (iam:PassRole)",
    "cosmos_index_bucket": "S3 hand-off bucket (curated_collections/, index_runs/)",
    "aoss_collection_name": "OpenSearch Serverless collection holding the web index",
    "aoss_collection_id": "id of that collection (aoss:APIAccessAll resource)",
    "opensearch_endpoint_test": "AOSS endpoint the engine validates the 'test' target against",
    "opensearch_endpoint_prod": "AOSS endpoint the engine validates the 'prod' target against",
}


@dataclass(frozen=True)
class EnvConfig:
    env: Environment
    region: str = "us-east-1"
    # Public subnets are picked from these AZs only: Fargate is not offered in us-east-1e, and the
    # same subnets are handed to the indexer as INDEXING_SUBNETS.
    azs: tuple[str, ...] = ("us-east-1a", "us-east-1b")
    # None → the account's default VPC (Vpc.from_lookup(is_default=True))
    vpc_id: str | None = None

    crawler_inbox: str = "/opt/sde-crawler/jobs/incoming"
    # Folder inside crawler_bucket the crawler writes to ("" = bucket root).
    crawler_s3_prefix: str = "sde-curation-engine-prototype"
    indexing_container_name: str = "WEB_COSMOSContainer"
    web_index_name: str = "sde-web-subset"
    openai_model: str = "gpt-5.6-luna"  # 1.05M-token window: the full page text always fits
    llm_workers: int = 16
    llm_pattern_batch_urls: int = 1000
    # Override for notification links; default = the stack's CloudFront URL.
    public_base_url: str | None = None
    cpu: int = 1024
    memory_mib: int = 2048
    waf_rate_limit_per_5min: int = 1000
    # RDS PostgreSQL (the state store). Storage starts at 20 GB gp3 and autoscales to 100 GB.
    # x86 dedicated compute, 2 vCPU / 8 GiB, no CPU-credit model. Graviton burstable (t4g) was
    # the first choice but hit "insufficient-capacity" twice in us-east-1 on 2026-09-11; the m6i
    # pools are far larger. Same class in every environment.
    db_instance_class: str = "m6i.large"
    # The DB subnet group spans every AZ where the class is orderable (not just `azs`, which is
    # limited by Fargate): RDS picks one with free capacity at create time, so more AZs = fewer
    # "insufficient-capacity" failures. us-east-1e offers no db.t3/t4g classes.
    db_azs: tuple[str, ...] = ("us-east-1a", "us-east-1b", "us-east-1c", "us-east-1d", "us-east-1f")
    db_multi_az: bool = False
    db_backup_days: int = 7
    db_deletion_protection: bool = False

    @property
    def name(self) -> str:
        return f"{APP_NAME}-{self.env.value}"

    def param_name(self, key: str) -> str:
        assert key in PARAMS, key
        return f"/{APP_NAME}/{self.env.value}/{key}"

    def secret_name(self, key: str) -> str:
        return f"/{APP_NAME}/{self.env.value}/{key}"


CONFIGS: dict[Environment, EnvConfig] = {
    Environment.DEV: EnvConfig(env=Environment.DEV),
    Environment.TEST: EnvConfig(env=Environment.TEST),
    Environment.PROD: EnvConfig(
        env=Environment.PROD, cpu=2048, memory_mib=4096,
        db_multi_az=True, db_backup_days=35, db_deletion_protection=True,
    ),
}


def get_config(env_name: str) -> EnvConfig:
    return CONFIGS[Environment(env_name)]
