/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqttbridge;

import com.aws.greengrass.builtin.services.pubsub.PubSubIPCEventStreamAgent;
import com.aws.greengrass.certificatemanager.certificate.CsrProcessingException;
import com.aws.greengrass.componentmanager.KernelConfigResolver;
import com.aws.greengrass.config.Node;
import com.aws.greengrass.config.Topic;
import com.aws.greengrass.config.Topics;
import com.aws.greengrass.config.WhatHappened;
import com.aws.greengrass.dependency.ImplementsService;
import com.aws.greengrass.dependency.State;
import com.aws.greengrass.device.ClientDevicesAuthService;
import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.lifecyclemanager.PluginService;
import com.aws.greengrass.lifecyclemanager.exceptions.ServiceLoadException;
import com.aws.greengrass.mqttbridge.auth.CsrGeneratingException;
import com.aws.greengrass.mqttbridge.auth.MQTTClientKeyStore;
import com.aws.greengrass.mqttbridge.clients.IoTCoreClient;
import com.aws.greengrass.mqttbridge.clients.MQTTClient;
import com.aws.greengrass.mqttbridge.clients.MQTTClientException;
import com.aws.greengrass.mqttbridge.clients.PubSubClient;
import com.aws.greengrass.mqttbridge.util.BatchedSubscriber;
import com.aws.greengrass.mqttclient.MqttClient;
import com.aws.greengrass.util.Utils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import lombok.AccessLevel;
import lombok.Getter;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyStoreException;
import java.security.cert.CertificateException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import javax.inject.Inject;


@ImplementsService(name = MQTTBridge.SERVICE_NAME)
public class MQTTBridge extends PluginService {
    public static final String SERVICE_NAME = "aws.greengrass.clientdevices.mqtt.Bridge";

    @Getter(AccessLevel.PACKAGE) // Getter for unit tests
    private final TopicMapping topicMapping;
    private final MessageBridge messageBridge;
    private final Kernel kernel;
    private final MQTTClientKeyStore mqttClientKeyStore;
    private final ExecutorService executorService;
    private final ConfigurationChangeHandler configurationChangeHandler;
    private final CertificateAuthorityChangeHandler certificateAuthorityChangeHandler;
    private MQTTClient mqttClient;
    private PubSubClient pubSubClient;
    private IoTCoreClient ioTCoreClient;
    private static final JsonMapper OBJECT_MAPPER =
            JsonMapper.builder().enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES).build();

    private URI brokerUri;
    private String clientId;


    /**
     * Ctr for MQTTBridge.
     *
     * @param topics             topics passed by the Nucleus
     * @param topicMapping       mapping of mqtt topics to iotCore/pubsub topics
     * @param pubSubIPCAgent     IPC agent for pubsub
     * @param iotMqttClient      mqtt client for iot core
     * @param kernel             Greengrass kernel
     * @param mqttClientKeyStore KeyStore for MQTT Client
     * @param executorService    Executor service
     */
    @Inject
    public MQTTBridge(Topics topics, TopicMapping topicMapping, PubSubIPCEventStreamAgent pubSubIPCAgent,
                      MqttClient iotMqttClient, Kernel kernel, MQTTClientKeyStore mqttClientKeyStore,
                      ExecutorService executorService) {
        this(topics, topicMapping, new MessageBridge(topicMapping), pubSubIPCAgent, iotMqttClient, kernel,
                mqttClientKeyStore, executorService);
    }

    protected MQTTBridge(Topics topics, TopicMapping topicMapping, MessageBridge messageBridge,
                         PubSubIPCEventStreamAgent pubSubIPCAgent, MqttClient iotMqttClient, Kernel kernel,
                         MQTTClientKeyStore mqttClientKeyStore, ExecutorService executorService) {
        super(topics);
        this.topicMapping = topicMapping;
        this.kernel = kernel;
        this.mqttClientKeyStore = mqttClientKeyStore;
        this.messageBridge = messageBridge;
        this.pubSubClient = new PubSubClient(pubSubIPCAgent);
        this.ioTCoreClient = new IoTCoreClient(iotMqttClient);
        this.executorService = executorService;
        this.configurationChangeHandler = new ConfigurationChangeHandler();
        this.certificateAuthorityChangeHandler = new CertificateAuthorityChangeHandler();
    }

    @Override
    public void install() {
        configurationChangeHandler.start();

        try {
            this.brokerUri = BridgeConfig.getBrokerUri(config);
        } catch (URISyntaxException e) {
            serviceErrored(e);
            return;
        }
        this.clientId = BridgeConfig.getClientId(config);
    }

    @Override
    public void startup() {
        try {
            mqttClientKeyStore.init();
        } catch (CsrProcessingException | KeyStoreException | CsrGeneratingException e) {
            serviceErrored(e);
            return;
        }

        try {
            certificateAuthorityChangeHandler.start();
        } catch (ServiceLoadException e) {
            serviceErrored(e);
            return;
        }

        try {
            mqttClient = new MQTTClient(brokerUri, clientId, mqttClientKeyStore, executorService);
            mqttClient.start();
            messageBridge.addOrReplaceMessageClient(TopicMapping.TopicType.LocalMqtt, mqttClient);
        } catch (MQTTClientException e) {
            serviceErrored(e);
            return;
        }
        pubSubClient.start();
        messageBridge.addOrReplaceMessageClient(TopicMapping.TopicType.Pubsub, pubSubClient);

        ioTCoreClient.start();
        messageBridge.addOrReplaceMessageClient(TopicMapping.TopicType.IotCore, ioTCoreClient);

        reportState(State.RUNNING);
    }

    @Override
    public void shutdown() {
        certificateAuthorityChangeHandler.stop();

        messageBridge.removeMessageClient(TopicMapping.TopicType.LocalMqtt);
        if (mqttClient != null) {
            mqttClient.stop();
        }

        messageBridge.removeMessageClient(TopicMapping.TopicType.Pubsub);
        if (pubSubClient != null) {
            pubSubClient.stop();
        }

        messageBridge.removeMessageClient(TopicMapping.TopicType.IotCore);
        if (ioTCoreClient != null) {
            ioTCoreClient.stop();
        }
    }

    public class CertificateAuthorityChangeHandler {

        private BatchedSubscriber subscriber;

        @SuppressWarnings({"unchecked", "PMD.UnusedFormalParameter"})
        private void onCAChange(WhatHappened what, Set<Node> whatChanged) {
            Topic caTopic;
            try {
                caTopic = findCATopic();
            } catch (ServiceLoadException e) {
                serviceErrored(e);
                return;
            }

            List<String> caCerts = (List<String>) caTopic.toPOJO();
            if (Utils.isEmpty(caCerts)) {
                return;
            }

            logger.atDebug().kv("numCaCerts", caCerts.size()).log("CA update received");
            try {
                mqttClientKeyStore.updateCA(caCerts);
            } catch (IOException | CertificateException | KeyStoreException e) {
                serviceErrored(e);
            }
        }

        /**
         * Begin listening and responding to CDA CA changes.
         *
         * <p>This operation is idempotent.
         *
         * @throws ServiceLoadException if CDA service could not be loaded
         */
        public void start() throws ServiceLoadException {
            if (subscriber == null) {
                subscriber = new BatchedSubscriber(findCATopic(), this::onCAChange);
            }
            subscriber.subscribe();
        }

        private Topic findCATopic() throws ServiceLoadException {
            return kernel
                    .locate(ClientDevicesAuthService.CLIENT_DEVICES_AUTH_SERVICE_NAME).getConfig()
                    .lookup(RUNTIME_STORE_NAMESPACE_TOPIC,
                            ClientDevicesAuthService.CERTIFICATES_KEY,
                            ClientDevicesAuthService.AUTHORITIES_TOPIC);
        }

        /**
         * Stop listening to CDA CA changes.
         */
        public void stop() {
            if (subscriber != null) {
                subscriber.unsubscribe();
            }
        }
    }

    /**
     * Responsible for handling all bridge config changes.
     */
    public class ConfigurationChangeHandler {

        private final Topics configurationTopics = config.lookupTopics(KernelConfigResolver.CONFIGURATION_CONFIG_KEY);

        private final BatchedSubscriber subscriber = new BatchedSubscriber(configurationTopics, (what, whatChanged) -> {

            if (what == WhatHappened.initialized
                    || whatChanged.stream().allMatch(n -> n.childOf(BridgeConfig.KEY_MQTT_TOPIC_MAPPING))) {
                updateTopicMapping();
                return;
            }

            logger.atInfo("service-config-change")
                    .kv("changes", whatChanged.stream().map(Node::getName).collect(Collectors.toSet()))
                    .log("Requesting reinstallation of bridge");
            requestReinstall();
        });

        private void updateTopicMapping() {
            Map<String, TopicMapping.MappingEntry> mapping;
            try {
                mapping = getMapping();
            } catch (IllegalArgumentException e) {
                serviceErrored(e);
                return;
            }

            logger.atInfo("service-config-change").kv("mapping", mapping).log("Updating mapping");
            topicMapping.updateMapping(mapping);
        }

        private Map<String, TopicMapping.MappingEntry> getMapping() {
            return OBJECT_MAPPER.convertValue(
                    config.lookupTopics(BridgeConfig.PATH_MQTT_TOPIC_MAPPING).toPOJO(),
                    new TypeReference<Map<String, TopicMapping.MappingEntry>>() {
                    }
            );
        }

        /**
         * Begin listening and responding to bridge configuration changes.
         *
         * <p>This operation is idempotent.
         */
        public void start() {
            subscriber.subscribe();
        }
    }
}
