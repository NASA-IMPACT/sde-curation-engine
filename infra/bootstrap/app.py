"""One-time bootstrap CDK app — run by an admin, never by GitHub Actions.

Creates the IAM role GitHub Actions assumes (via OIDC) to deploy CurationEngine-<env> into this
account. Separate from infra/app.py on purpose: the deploy role must exist before CI can deploy
anything, and CI must never be able to modify its own role.

    make bootstrap-github ENV=dev PROFILE=sde-dev
    # then copy the GitHubActionsRoleArn output into the GitHub repository secret AWS_ROLE_DEV

Pass `-c create_oidc_provider=true` the first time in an account that has no
token.actions.githubusercontent.com identity provider yet (the dev account already has one).
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import aws_cdk as cdk
from bootstrap_stack import BootstrapStack

from config import APP_NAME, get_config

app = cdk.App()
cfg = get_config(app.node.try_get_context("environment") or "dev")
account = os.environ.get("CDK_DEFAULT_ACCOUNT")
if not account:
    raise SystemExit("no AWS credentials: set AWS_PROFILE (e.g. `aws sso login --profile sde-dev`)")

BootstrapStack(
    app,
    f"CurationEngine-Bootstrap-{cfg.env.value}",
    cfg=cfg,
    create_oidc_provider=str(app.node.try_get_context("create_oidc_provider")).lower() == "true",
    env=cdk.Environment(account=account, region=cfg.region),
    description=f"GitHub Actions deploy role for {APP_NAME} ({cfg.env.value})",
)
for k, v in {"Project": "SDE", "ManagedBy": "AWS CDK", "Environment": cfg.env.value,
             "Application": APP_NAME}.items():
    cdk.Tags.of(app).add(k, v)
app.synth()
