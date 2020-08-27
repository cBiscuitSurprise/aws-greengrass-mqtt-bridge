package com.aws.iot.evergreen.mqtt.bridge;

import com.aws.iot.evergreen.config.Topics;
import com.aws.iot.evergreen.dependency.ImplementsService;
import com.aws.iot.evergreen.dependency.State;
import com.aws.iot.evergreen.kernel.EvergreenService;
import com.aws.iot.evergreen.packagemanager.KernelConfigResolver;
import com.aws.iot.evergreen.util.Coerce;
import com.aws.iot.evergreen.util.Utils;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.AccessLevel;
import lombok.Getter;

import javax.inject.Inject;

@ImplementsService(name = MQTTBridge.SERVICE_NAME)
public class MQTTBridge extends EvergreenService {
    public static final String SERVICE_NAME = "aws.greengrass.mqtt.bridge";

    @Getter(AccessLevel.PACKAGE) // Getter for unit tests
    private final TopicMapping topicMapping;
    static final String MQTT_TOPIC_MAPPING = "mqttTopicMapping";

    /**
     * Ctr for MQTTBridge.
     * @param topics topic
     * @param topicMapping mapping of mqtt topics to iotCore/pubsub topics
     */
    @Inject
    public MQTTBridge(Topics topics, TopicMapping topicMapping) {
        super(topics);
        this.topicMapping = topicMapping;

        topics.lookup(KernelConfigResolver.PARAMETERS_CONFIG_KEY, MQTT_TOPIC_MAPPING).dflt("{}")
                .subscribe((why, newv) -> {
                    try {
                        String mapping = Coerce.toString(newv);
                        if (Utils.isEmpty(mapping)) {
                            logger.debug("Mapping null or empty");
                            return;
                        }
                        topicMapping.updateMapping(mapping);
                    } catch (JsonProcessingException e) {
                        logger.atError("Invalid topic mapping").kv("TopicMapping", Coerce.toString(newv)).log();
                        // Currently, kernel spills all exceptions in std err which junit consider failures
                        serviceErrored(String.format("Invalid topic mapping. %s", e.getMessage()));
                    }
                });
    }

    @Override
    public void startup() {
        reportState(State.RUNNING);
    }

    @Override
    public void shutdown() {
    }
}
