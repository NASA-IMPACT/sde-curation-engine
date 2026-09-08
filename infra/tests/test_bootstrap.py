"""The GitHub Actions deploy role trusts exactly one repo+branch and can only act through the CDK toolkit roles."""

import sys
from pathlib import Path

import aws_cdk as cdk
from aws_cdk.assertions import Match, Template

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bootstrap"))
from bootstrap_stack import BootstrapStack

from config import get_config


def synth(env: str, **kw) -> Template:
    app = cdk.App()
    stack = BootstrapStack(app, f"CurationEngine-Bootstrap-{env}", cfg=get_config(env),
                           env=cdk.Environment(account="123456789012", region="us-east-1"), **kw)
    return Template.from_stack(stack)


def test_role_is_scoped_to_this_repo_and_branch():
    t = synth("dev")
    t.resource_count_is("AWS::IAM::Role", 1)
    t.has_resource_properties("AWS::IAM::Role", {
        "RoleName": "GitHubActions-CurationEngine-DEV",
        "AssumeRolePolicyDocument": {"Statement": [Match.object_like({
            "Action": "sts:AssumeRoleWithWebIdentity",
            "Condition": {
                "StringEquals": {"token.actions.githubusercontent.com:aud": "sts.amazonaws.com"},
                "StringLike": {"token.actions.githubusercontent.com:sub": [
                    "repo:NASA-IMPACT/sde-curation-engine:ref:refs/heads/dev",
                    "repo:NASA-IMPACT@22798984/sde-curation-engine@1349822651:ref:refs/heads/dev",
                ]},
            },
        })]},
    })
    t.has_resource_properties("AWS::IAM::Policy", {"PolicyDocument": {"Statement": Match.array_with([
        Match.object_like({"Action": "sts:AssumeRole",
                           "Resource": "arn:aws:iam::123456789012:role/cdk-sde-*-role-123456789012-us-east-1"}),
    ])}})
    t.resource_count_is("AWS::IAM::OIDCProvider", 0)


def test_prod_role_trusts_prod_branch_and_can_create_provider():
    t = synth("prod", create_oidc_provider=True)
    t.has_resource_properties("AWS::IAM::Role", {"RoleName": "GitHubActions-CurationEngine-PROD"})
    t.resource_count_is("AWS::IAM::OIDCProvider", 1)
