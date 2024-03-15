- We use this component only for "simulator" (not for virtual gateway).

- This component name is revised from `aws.greengrass.clientdevices.mqtt.Bridge` -> `com.bolt-data.iot.MqttBridge`

- To install the latest version, we have upgraded other component version as well.

```bash
    aws.greengrass.Nucleus	2.9.3
    aws.greengrass.StreamManager	2.1.6
    aws.greengrass.clientdevices.Auth	2.4.1
    aws.greengrass.clientdevices.mqtt.Moquette	2.3.2
```

```bash
    aws.greengrass.ShadowManager	2.3.2
    aws.greengrass.clientdevices.IPDetector	2.1.6
```

- Configuration to merge, when you deploy/revise

```json
{
    "mqttTopicMapping": {
        "forwardAllMqttToIpc": {
            "source": "LocalMqtt",
            "sourceTopic": "#",
            "targetTopic": "connector.mqtts.message.input",
            "target": "Pubsub",
            "targetPayloadTemplate": "{\"topic\": \"${sourceTopic}\", \"payload\": \"${sourcePayload:base64}\"}"
        },
        "ShadowsLocalMqttToPubsub": {
            "topic": "$aws/things/+/shadow/#",
            "source": "LocalMqtt",
            "target": "Pubsub"
        },
        "ShadowsPubsubToLocalMqtt": {
            "topic": "$aws/things/+/shadow/#",
            "source": "Pubsub",
            "target": "LocalMqtt"
        }
    }
}
```


# how to build component locally and deploy


```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

- Build component 

```bash
  gdk component build
```

- Deploy Component

```bash
python scripts/deploy.py --version <version-number>
```






🧙🧙🧙 magic
This is a patch which allows us to publish to our accounts rather than use the global AWS version of this component. See [BDC_README.md](BDC_README.md) for more info.
🧙🧙🧙

## AWS Greengrass MQTT Bridge

The MQTT bridge component (aws.greengrass.clientdevices.mqtt.Bridge) relays MQTT messages between client devices, local Greengrass publish/subscribe, and AWS IoT Core. You can use this component to act on MQTT messages from client devices in custom components and sync client devices with the AWS Cloud.

You can use this component to relay messages between the following message brokers:
* Local MQTT – The local MQTT broker handles messages between client devices and a core device.
* Local publish/subscribe – The local Greengrass message broker handles messages between components on a core device. For more information about how to interact with these messages in Greengrass components, see [Publish/subscribe local messages](https://docs.aws.amazon.com/greengrass/v2/developerguide/ipc-publish-subscribe.html).
* AWS IoT Core – The AWS IoT Core MQTT broker handles messages between IoT devices and AWS Cloud destinations. For more information about how to interact with these messages in Greengrass components, see [Publish/subscribe AWS IoT Core MQTT messages](https://docs.aws.amazon.com/greengrass/v2/developerguide/ipc-iot-core-mqtt.html).

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.
