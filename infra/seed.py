"""Write the account-specific values for one environment into SSM Parameter Store.

    python3 seed.py dev [--profile sde-dev]

Reads infra/envs/<env>.json (gitignored; see envs/example.json), checks it has exactly the keys in
config.PARAMS, and `aws ssm put-parameter --overwrite`s each one. Re-run after changing a value,
then `make deploy` so CloudFormation re-resolves the parameters.
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path

from config import PARAMS, get_config

HERE = Path(__file__).parent


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("environment", choices=["dev", "test", "prod"])
    ap.add_argument("--profile")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    cfg = get_config(args.environment)

    src = HERE / "envs" / f"{args.environment}.json"
    if not src.exists():
        sys.exit(f"{src} not found — copy envs/example.json and fill it in")
    values = json.loads(src.read_text())
    if set(values) != set(PARAMS) or not all(isinstance(v, str) and v for v in values.values()):
        sys.exit(f"{src} must contain exactly these non-empty string keys: {sorted(PARAMS)}")

    for key, value in values.items():
        cmd = ["aws", "ssm", "put-parameter", "--name", cfg.param_name(key), "--type", "String",
               "--value", value, "--description", PARAMS[key], "--overwrite", "--region", cfg.region]
        if args.profile:
            cmd += ["--profile", args.profile]
        print(cfg.param_name(key))
        if not args.dry_run:
            subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL)
    return 0


if __name__ == "__main__":
    sys.exit(main())
