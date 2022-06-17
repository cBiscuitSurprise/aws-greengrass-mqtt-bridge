* [] explicit `targetTopic` working
  ```json
  {
    "sourceTopic": "acme/widget/+/temperature",
    "source": "LocalMqtt",
    "targetTopic": "our-component/input",
    "target": "Pubsub"
  }
  ```
* [] `targetTopic` with transform
  ```json
  {
    "targetPayload": {
      "mqttTopic": "{sourceTopic}",
      "mqttPayload": "{sourcePayload}",
      "source": "connector.mqtts",
    }
  }
  ```
* [] `targetTopic` with replacements
  ```json
  {
    "targetTopic": "iot/gateways/{thingName}/{sourceTopic}",
  }
  ```
* [] `target` of `stream`
  ```json
  {
    "target": "Stream",
    "targetName": "our-input-stream",
  }
  ```