/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Topic mappings from mqtt topic to other topics (iot core or pub sub).
 */
@NoArgsConstructor
public class TopicMapping {
    @Getter
    private Map<String, MappingEntry> mapping = new HashMap<>();

    private List<UpdateListener> updateListeners = new CopyOnWriteArrayList<>();

    /**
     * Type of the topic.
     */
    public enum TopicType {
        IotCore, Pubsub, LocalMqtt
    }

    /**
     * A single entry in the mapping.
     */
    @AllArgsConstructor
    @NoArgsConstructor
    @EqualsAndHashCode
    public static class MappingEntry {
        @Getter
        private String sourceTopic;
        @Getter
        private TopicType source;
        @Getter
        private String targetTopic;
        @Getter
        private TopicType target;
        private String targetTopicPrefix;
        @Getter
        private String targetPayloadTemplate;

        /**
         * Get the source topic (provided for backwards compatibility).
         * 
         * @return the source-topic
         */
        public String getTopic() {
            return sourceTopic;
        }

        /**
         * Set the source topic (provided for backwards compatibility).
         *
         * @param value the source topic string
         */
        public void setTopic(String value) {
            sourceTopic = value;
        }

        /**
         * Get the configured target-topic-prefix.
         *
         * @return configured target-topic-prefix
         */
        public String getTargetTopicPrefix() {
            if (targetTopicPrefix == null) {
                return "";
            } else {
                return targetTopicPrefix;
            }
        }

        /**
         * Create MappingEntry with no `targetTopicPrefix`.
         * 
         * @param topic the source-topic to map from
         * @param source the source integration to listen on
         * @param target the target integration to bridge to
         */
        MappingEntry(String sourceTopic, TopicType source, TopicType target) {
            this.sourceTopic = sourceTopic;
            this.source = source;
            this.target = target;
        }

        @Override
        public String toString() {
            return String.format(
                "{source-topic: %s, source: %s, target-topic: %s, target: %s, target-topic-prefix: %s, " 
                    + "target-payload-template: %s}",
                sourceTopic, source, targetTopic, target, targetTopicPrefix, targetPayloadTemplate);
        }
    }

    @FunctionalInterface
    public interface UpdateListener {
        void onUpdate();
    }

    /**
     * Update the topic mapping.
     *
     * @param mapping mapping to update
     */
    public void updateMapping(@NonNull Map<String, MappingEntry> mapping) {
        // TODO: Check for duplicates, General validation + unit tests. Topic strings need to be validated (allowed
        //  filter?, etc)
        this.mapping = mapping;
        updateListeners.forEach(UpdateListener::onUpdate);
    }

    public void listenToUpdates(UpdateListener listener) {
        updateListeners.add(listener);
    }
}