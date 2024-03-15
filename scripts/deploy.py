import json
import click

from utils import (
    create_recipe_template,
    execute_command,
    update_config_version,
    upload_recipe_template,
)


def _load_config():
    with open("gdk-config.json", "r") as fid:
        return json.load(fid)


def deploy(version: str):
    command = ["gdk", "component", "publish", "--bucket"]
    config = _load_config()

    # bucket is at "component.<component-name>.publish.bucket"
    # there's only one entry in `component`, so `component.values()` should only
    # return one value
    for key, component_config in config.get("component", {}).items():
        component_name = key
        bucket = component_config.get("publish").get("bucket")

    update_config_version(version)

    print(f"publishing component to bucket: {bucket}")
    command.append(bucket)
    for line in execute_command(command):
        print(line.decode())

    print(f"uploading recipe-template to bucket: {bucket}")
    template_file = create_recipe_template(
        f"greengrass-build/recipes/com.bolt-data.iot.MqttBridge-{version}.json",
        bucket,
    )
    upload_recipe_template(component_name, version, template_file, bucket)

    print("... done")


@click.command()
@click.option("--version", type=str, help="The new version to deploy")
def _deploy(version: str):
    deploy(version)


if __name__ == "__main__":
    _deploy()
