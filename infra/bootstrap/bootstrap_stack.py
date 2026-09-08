"""CurationEngine-Bootstrap-<env>: the GitHub Actions deployment role for one environment.

Mirrors sde-api-scrapers' bootstrap stack. One role per account, trusted only by this repository's
branch for that environment (dev → `dev`, test → `test`, prod → `prod`). The role itself can only
assume the CDK toolkit roles (`cdk-sde-*`) — CDK does every CloudFormation, S3 and ECR operation
through those — plus the few read calls the workflow's verify step makes.
"""

from __future__ import annotations

from aws_cdk import CfnOutput, Duration, Stack
from aws_cdk import aws_iam as iam
from constructs import Construct

from config import APP_NAME, EnvConfig

GITHUB_ORG = "NASA-IMPACT"
GITHUB_REPO = "sde-curation-engine"
OIDC_PROVIDER_URL = "token.actions.githubusercontent.com"
BOOTSTRAP_QUALIFIER = "sde"
BRANCH_FOR_ENV = {"dev": "dev", "test": "test", "prod": "prod"}


class BootstrapStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, *, cfg: EnvConfig,
                 create_oidc_provider: bool = False, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        env, branch = cfg.env.value, BRANCH_FOR_ENV[cfg.env.value]

        if create_oidc_provider:  # native resource (no custom-resource Lambda); thumbprint is unused by AWS for GitHub
            provider_arn = iam.CfnOIDCProvider(
                self, "GitHubOidc", url=f"https://{OIDC_PROVIDER_URL}", client_id_list=["sts.amazonaws.com"],
                thumbprint_list=["ffffffffffffffffffffffffffffffffffffffff"],
            ).attr_arn
        else:  # account-wide singleton, normally already present
            provider_arn = f"arn:aws:iam::{self.account}:oidc-provider/{OIDC_PROVIDER_URL}"

        role = iam.Role(
            self, "GitHubActionsRole", role_name=f"GitHubActions-CurationEngine-{env.upper()}",
            description=f"GitHub Actions deploys {APP_NAME} ({env}) from {GITHUB_ORG}/{GITHUB_REPO}@{branch}",
            max_session_duration=Duration.hours(1),
            assumed_by=iam.FederatedPrincipal(
                federated=provider_arn,
                conditions={
                    "StringEquals": {f"{OIDC_PROVIDER_URL}:aud": "sts.amazonaws.com"},
                    "StringLike": {f"{OIDC_PROVIDER_URL}:sub": f"repo:{GITHUB_ORG}/{GITHUB_REPO}:ref:refs/heads/{branch}"},
                },
                assume_role_action="sts:AssumeRoleWithWebIdentity",
            ),
        )
        # cdk deploy: lookups, asset publishing (S3 + ECR) and CloudFormation all run as these roles
        role.add_to_policy(iam.PolicyStatement(
            sid="AssumeCdkToolkitRoles", actions=["sts:AssumeRole"],
            resources=[f"arn:aws:iam::{self.account}:role/cdk-{BOOTSTRAP_QUALIFIER}-*-role-{self.account}-{self.region}"],
        ))
        role.add_to_policy(iam.PolicyStatement(
            sid="CdkBootstrapVersion", actions=["ssm:GetParameter"],
            resources=[f"arn:aws:ssm:{self.region}:{self.account}:parameter/cdk-bootstrap/{BOOTSTRAP_QUALIFIER}/version"],
        ))
        # verify step: stack outputs + service state
        role.add_to_policy(iam.PolicyStatement(
            sid="VerifyDeployment", actions=["cloudformation:DescribeStacks", "ecs:DescribeServices"],
            resources=[f"arn:aws:cloudformation:{self.region}:{self.account}:stack/CurationEngine-{env}/*",
                       f"arn:aws:ecs:{self.region}:{self.account}:service/{cfg.name}/{cfg.name}"],
        ))

        CfnOutput(self, "GitHubActionsRoleArn", value=role.role_arn,
                  description=f"Add as GitHub repository secret AWS_ROLE_{env.upper()}")
