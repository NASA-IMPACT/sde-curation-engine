"""CDK app for sde-curation-engine.

    cd infra && AWS_PROFILE=sde-dev uv run cdk deploy -c environment=dev

One stack per environment (`CurationEngine-<env>`). The target account is whatever the active AWS
profile resolves to (CDK_DEFAULT_ACCOUNT); everything account-specific is read from SSM Parameter
Store at deploy time, so deploying with the wrong profile fails on missing parameters.
"""

import os

import aws_cdk as cdk

from config import APP_NAME, get_config
from stacks.engine_stack import CurationEngineStack

app = cdk.App()
cfg = get_config(app.node.try_get_context("environment") or "dev")
account = os.environ.get("CDK_DEFAULT_ACCOUNT")
if not account:
    raise SystemExit("no AWS credentials: set AWS_PROFILE (e.g. `aws sso login --profile sde-dev`)")

CurationEngineStack(
    app,
    f"CurationEngine-{cfg.env.value}",
    cfg=cfg,
    env=cdk.Environment(account=account, region=cfg.region),
    description=f"SDE Curation Engine ({cfg.env.value}): Fargate + EFS + ALB + CloudFront",
)

for k, v in {"Project": "SDE", "ManagedBy": "AWS CDK", "Environment": cfg.env.value,
             "Application": APP_NAME}.items():
    cdk.Tags.of(app).add(k, v)

app.synth()
