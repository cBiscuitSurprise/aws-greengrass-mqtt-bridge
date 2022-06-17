import json
import re


def convertToDict(value: str):
    output = dict()
    for match in re.finditer("(?P<key>mapping\d+)=(?P<value>.*?)(?=,\smapping\d+|\})", value):
        print(match.group("key"))
        output[match.group("key")] = match.group("value")
    return output;

def compareBullshit(expected, actual):
    expected = convertToDict(expected)
    actual = convertToDict(actual)

    print(json.dumps(expected, sort_keys=True))
    print(json.dumps(actual, sort_keys=True))

if __name__ == "__main__":
    expected="{mapping5={source-topic: topic/to/map/from/local, source: LocalMqtt, target-topic: topic/to/map/to/cloud, target: IotCore, target-topic-prefix: a-prefix, target-payload-template: null}, mapping4={source-topic: topic/to/map/from/local/to/pubsub/2, source: LocalMqtt, target-topic: null, target: Pubsub, target-topic-prefix: a-prefix, target-payload-template: null}, mapping3={source-topic: topic/to/map/from/local/to/cloud/2, source: LocalMqtt, target-topic: null, target: IotCore, target-topic-prefix: null, target-payload-template: null}, mapping2={source-topic: topic/to/map/from/local/to/pubsub, source: LocalMqtt, target-topic: null, target: Pubsub, target-topic-prefix: null, target-payload-template: null}, mapping1={source-topic: topic/to/map/from/local/to/cloud, source: LocalMqtt, target-topic: null, target: IotCore, target-topic-prefix: null, target-payload-template: null}}"
    actual="{mapping1={source-topic: topic/to/map/from/local/to/cloud, source: LocalMqtt, target-topic: null, target: IotCore, target-topic-prefix: null, target-payload-template: null}, mapping2={source-topic: topic/to/map/from/local/to/pubsub, source: LocalMqtt, target-topic: null, target: Pubsub, target-topic-prefix: null, target-payload-template: null}, mapping3={source-topic: topic/to/map/from/local/to/cloud/2, source: LocalMqtt, target-topic: null, target: IotCore, target-topic-prefix: null, target-payload-template: null}, mapping4={source-topic: topic/to/map/from/local/to/pubsub/2, source: LocalMqtt, target-topic: null, target: Pubsub, target-topic-prefix: a-prefix, target-payload-template: null}, mapping5={source-topic: topic/to/map/from/local, source: LocalMqtt, target-topic: topic/to/map/to/cloud, target: IotCore, target-topic-prefix: null, target-payload-template: ${sourceTopic}::${sourcePayload:base64}}}"
    compareBullshit(expected, actual)
