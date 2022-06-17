import json
import os
import re
from subprocess import PIPE, Popen
from typing import List

import boto3

AWS_REGIONS = [
    "us-east-2",
    "us-east-1",
    "us-west-1",
    "us-west-2",
    "af-south-1",
    "ap-east-1",
    "ap-southeast-3",
    "ap-south-1",
    "ap-northeast-3",
    "ap-northeast-2",
    "ap-southeast-1",
    "ap-southeast-2",
    "ap-northeast-1",
    "ca-central-1",
    "eu-central-1",
    "eu-west-1",
    "eu-west-2",
    "eu-south-1",
    "eu-west-3",
    "eu-north-1",
    "me-south-1",
    "sa-east-1",
    "us-gov-east-1",
    "us-gov-west-1",
]


def execute_command(command: List[str]):
    p = Popen(command, stdout=PIPE, stderr=PIPE, env=os.environ)
    stack = []
    while p.returncode is None or len(stack) != 0:
        if p.returncode is None:
            stack.append(p.stdout.read())
            stack.append(p.stderr.read())
            p.poll()

        yield stack.pop(0)

    if p.returncode != 0:
        raise RuntimeError(f"failed to execute command: `{' '.join(command)}`")


def update_config_version(version: str):
    with open("gdk-config.json", "r") as fid:
        config = json.load(fid)

    for component_config in config.get("component", {}).values():
        print(f"updating configured version {component_config['version']} -> {version}")
        component_config["version"] = version

    with open("gdk-config.json", "w") as fid:
        json.dump(config, fid, indent=2)


def create_recipe_template(input_recipe_file: str, bucket_name: str):
    with open(input_recipe_file, "r") as fid:
        recipe_text = fid.read()

    replacement = re.sub("|".join(AWS_REGIONS), "${AWS::Region}", bucket_name)
    recipe_text = re.sub(f"\\b{bucket_name}\\b", replacement, recipe_text)

    output_file = os.path.join("greengrass-build", "recipes", "recipe_template.yaml")
    with open(output_file, "w") as fid:
        fid.write(recipe_text)

    return output_file


def upload_recipe_template(name: str, version: str, template_file: str, bucket_name: str):
    with open(template_file, "rb") as fid:
        session = boto3.Session()
        s3 = session.client("s3")
        s3.put_object(
            Bucket=bucket_name,
            Key=f"{name}/{version}/recipe_template.yaml",
            Body=fid,
        )
