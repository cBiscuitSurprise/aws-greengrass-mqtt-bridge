/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqttbridge;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.mqttbridge.clients.MessageClient;
import com.aws.greengrass.mqttbridge.clients.MessageClientException;
import com.aws.greengrass.util.Utils;
import org.eclipse.paho.client.mqttv3.MqttTopic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Bridges/Routes the messages flowing between clients to various brokers. This class process the topics mappings. It
 * tells the clients to subscribe to the relevant topics and routes the messages to other clients when received.
 */
public class MessageBridge {
    private static final Logger LOGGER = LogManager.getLogger(MessageBridge.class);
    private static final String LOG_KEY_SOURCE_TYPE = "source";
    private static final String LOG_KEY_SOURCE_TOPIC = "sourceTopic";
    private static final String LOG_KEY_TARGET_TYPE = "target";
    private static final String LOG_KEY_TARGET_TOPIC = "targetTopic";
    private static final String LOG_KEY_RESOLVED_TARGET_TOPIC = "resolvedTargetTopic";

    private final TopicMapping topicMapping;
    // A map from type of message client to the clients. For example, LocalMqtt -> MQTTClient
    private final Map<TopicMapping.TopicType, MessageClient> messageClientMap = new ConcurrentHashMap<>();
    // A map from type of source to its mapping. The mapping is actually mapping from topic name to its destinations
    // (destination topic + type). This data structure may change once we introduce complex routing mechanism.
    // Example:
    // LocalMqtt -> {"/sourceTopic", [<TopicMapping>, <TopicMapping>]}
    private Map<TopicMapping.TopicType, Map<String, List<TopicMapping.MappingEntry>>>
            perClientSourceDestinationMap = new HashMap<>();

    /**
     * Ctr for Message Bridge.
     *
     * @param topicMapping topics mapping
     */
    public MessageBridge(TopicMapping topicMapping) {
        this.topicMapping = topicMapping;
        this.topicMapping.listenToUpdates(this::processMappingAndSubscribe);
        processMappingAndSubscribe();
    }

    /**
     * Add or replace the client of given type.
     *
     * @param clientType    type of the client (type is the `source` type). Example, it will be LocalMqtt for
     *                      MQTTClient
     * @param messageClient client
     */
    public void addOrReplaceMessageClient(TopicMapping.TopicType clientType, MessageClient messageClient) {
        messageClientMap.put(clientType, messageClient);
        updateSubscriptionsForClient(clientType, messageClient);
    }

    /**
     * Remove the client of given type.
     *
     * @param clientType client type
     */
    public void removeMessageClient(TopicMapping.TopicType clientType) {
        messageClientMap.remove(clientType);
    }

    private void handleMessage(TopicMapping.TopicType sourceType, Message message) {
        String fullSourceTopic = message.getTopic();
        LOGGER.atDebug().kv(LOG_KEY_SOURCE_TYPE, sourceType).kv(LOG_KEY_SOURCE_TOPIC, fullSourceTopic)
                .log("Message received");

        MessageClient sourceClient = messageClientMap.get(sourceType);
        if (sourceClient == null) {
            LOGGER.atError().kv(LOG_KEY_SOURCE_TYPE, sourceType).kv(LOG_KEY_SOURCE_TOPIC, fullSourceTopic)
                    .log("Source client not found");
            return;
        }

        Map<String, List<TopicMapping.MappingEntry>> srcDestMapping = perClientSourceDestinationMap.get(sourceType);

        if (srcDestMapping != null) {
            final Consumer<TopicMapping.MappingEntry> processDestination = mapping -> {
                MessageClient client = messageClientMap.get(mapping.getTarget());
                // If the mapped topic string is empty string, we forward the message to the same topic as the
                // source topic.
                final String baseTargetTopic = Utils.isEmpty(mapping.getTargetTopic())
                    ? fullSourceTopic
                    : mapping.getTargetTopic();
                final String targetTopic = Utils.isEmpty(mapping.getTargetTopicPrefix())
                    ? baseTargetTopic
                    : mapping.getTargetTopicPrefix() + baseTargetTopic;
                if (client == null) {
                    LOGGER.atError().kv(LOG_KEY_SOURCE_TYPE, sourceType).kv(LOG_KEY_SOURCE_TOPIC, fullSourceTopic)
                            .kv(LOG_KEY_TARGET_TYPE, mapping.getTarget())
                            .kv(LOG_KEY_TARGET_TOPIC, mapping.getTargetTopic())
                            .kv(LOG_KEY_RESOLVED_TARGET_TOPIC, targetTopic)
                            .log("Message client not found for target type");
                } else {
                    Message msg = new Message(targetTopic, message.getPayload());
                    try {
                        client.publish(msg);
                        LOGGER.atInfo().kv(LOG_KEY_SOURCE_TYPE, sourceType).kv(LOG_KEY_SOURCE_TOPIC, fullSourceTopic)
                                .kv(LOG_KEY_TARGET_TYPE, mapping.getTarget())
                                .kv(LOG_KEY_TARGET_TOPIC, mapping.getTargetTopic())
                                .kv(LOG_KEY_RESOLVED_TARGET_TOPIC, targetTopic).log("Published message");
                    } catch (MessageClientException e) {
                        LOGGER.atError().kv(LOG_KEY_SOURCE_TYPE, sourceType).kv(LOG_KEY_SOURCE_TOPIC, fullSourceTopic)
                                .kv(LOG_KEY_TARGET_TYPE, mapping.getTarget())
                                .kv(LOG_KEY_TARGET_TOPIC, mapping.getTargetTopic())
                                .kv(LOG_KEY_RESOLVED_TARGET_TOPIC, targetTopic).log("Failed to publish");
                    }
                }
            };

            if (sourceClient.supportsTopicFilters()) {
                // Do topic filter matching
                srcDestMapping.entrySet().stream()
                        .filter(mapping -> MqttTopic.isMatched(mapping.getKey(), fullSourceTopic))
                        .map(Map.Entry::getValue)
                        .forEach(perTopicDestinationList -> perTopicDestinationList.forEach(processDestination));
            } else {
                // Do direct matching
                List<TopicMapping.MappingEntry> destinations = srcDestMapping.get(fullSourceTopic);
                if (destinations == null) {
                    return;
                }
                destinations.forEach(processDestination);
            }
        }
    }

    private void processMappingAndSubscribe() {
        Map<String, TopicMapping.MappingEntry> mapping = topicMapping.getMapping();
        LOGGER.atDebug().kv("topicMapping", mapping).log("Processing mapping");

        Map<TopicMapping.TopicType, Map<String, List<TopicMapping.MappingEntry>>>
                perClientSourceDestinationMapTemp = new HashMap<>();

        mapping.forEach((key, mappingEntry) -> {
            // Ensure mapping for client type
            Map<String, List<TopicMapping.MappingEntry>> sourceDestinationMap =
                    perClientSourceDestinationMapTemp.computeIfAbsent(mappingEntry.getSource(), k -> new HashMap<>());

            // Add destinations for each source topic
            // TODO: Support more types of topic mapping.
            sourceDestinationMap.computeIfAbsent(mappingEntry.getTopic(), k -> new ArrayList<>())
                    .add(mappingEntry);
        });

        perClientSourceDestinationMap = perClientSourceDestinationMapTemp;

        messageClientMap.forEach(this::updateSubscriptionsForClient);
        LOGGER.atDebug().kv("topicMapping", perClientSourceDestinationMap).log("Processed mapping");
    }

    private synchronized void updateSubscriptionsForClient(TopicMapping.TopicType clientType,
                                                           MessageClient messageClient) {
        Map<String, List<TopicMapping.MappingEntry>> srcDestMapping =
                perClientSourceDestinationMap.get(clientType);

        Set<String> topicsToSubscribe;
        if (srcDestMapping == null) {
            topicsToSubscribe = new HashSet<>();
        } else {
            topicsToSubscribe = srcDestMapping.keySet();
        }

        LOGGER.atDebug().kv("clientType", clientType).kv("topics", topicsToSubscribe).log("Updating subscriptions");

        messageClient.updateSubscriptions(topicsToSubscribe, message -> handleMessage(clientType, message));
    }
}
