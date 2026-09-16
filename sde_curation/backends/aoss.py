"""SigV4 OpenSearch Serverless clients, optionally through an assumed role.

The role path matters for prod: the engine runs in the SMCE test account and writes the production
collection by assuming a role in the prod account (PROD_INDEX_ROLE_ARN). A publish can outlive one
set of STS credentials (1 h), so assumed-role credentials refresh themselves; AWSV4SignerAuth reads
frozen credentials per request, so a refresh is picked up mid-run.
"""

from __future__ import annotations

from ..config import Settings


def aoss_credentials(settings: Settings, role_arn: str | None = None, session_name: str = "sde-curation-engine"):
    import boto3

    session = boto3.Session(region_name=settings.aws_region)
    if not role_arn:
        return session.get_credentials()
    from botocore.credentials import DeferredRefreshableCredentials

    sts = session.client("sts")

    def refresh() -> dict[str, str]:
        cr = sts.assume_role(RoleArn=role_arn, RoleSessionName=session_name)["Credentials"]
        return {"access_key": cr["AccessKeyId"], "secret_key": cr["SecretAccessKey"],
                "token": cr["SessionToken"], "expiry_time": cr["Expiration"].isoformat()}

    return DeferredRefreshableCredentials(refresh_using=refresh, method="sts-assume-role")


def aoss_client(settings: Settings, endpoint: str, role_arn: str | None = None, *,
                session_name: str = "sde-curation-engine", timeout: int = 60):
    from opensearchpy import AWSV4SignerAuth, OpenSearch, RequestsHttpConnection

    host = endpoint.replace("https://", "").rstrip("/")
    return OpenSearch(
        hosts=[{"host": host, "port": 443}],
        http_auth=AWSV4SignerAuth(aoss_credentials(settings, role_arn, session_name), settings.aws_region, "aoss"),
        use_ssl=True, verify_certs=True, connection_class=RequestsHttpConnection, timeout=timeout,
    )
