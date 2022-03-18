/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqttbridge.clients;

import com.aws.greengrass.componentmanager.KernelConfigResolver;
import com.aws.greengrass.config.Topics;
import com.aws.greengrass.dependency.Context;
import com.aws.greengrass.mqttbridge.BridgeConfig;
import com.aws.greengrass.mqttbridge.Message;
import com.aws.greengrass.mqttbridge.auth.MQTTClientKeyStore;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import javax.net.ssl.SSLSocketFactory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@ExtendWith({MockitoExtension.class, GGExtension.class})
public class MQTTClientTest {
    private Topics configTopics;

    private FakeMqttClient fakeMqttClient;

    @Mock
    private MQTTClientKeyStore mockMqttClientKeyStore;

    private ScheduledExecutorService ses;

    @BeforeEach
    void setup() {
        configTopics = Topics.of(new Context(), KernelConfigResolver.CONFIGURATION_CONFIG_KEY, null);
        fakeMqttClient = new FakeMqttClient("clientId");
        ses = new ScheduledThreadPoolExecutor(1);
    }

    @AfterEach
    void tearDown() {
        configTopics.getContext().shutdown();
        ses.shutdownNow();
    }

    @Test
    void GIVEN_mqttClient_WHEN_start_THEN_clientConnects() throws MQTTClientException {
        MQTTClient mqttClient = new MQTTClient(configTopics, mockMqttClientKeyStore, ses, fakeMqttClient);
        mqttClient.start();
        fakeMqttClient.waitForConnect(1000);

        assertThat(fakeMqttClient.isConnected(), is(true));
    }

    @Test
    void GIVEN_subscribedMqttClient_WHEN_stop_THEN_clientUnsubscribes() throws MQTTClientException {
        MQTTClient mqttClient = new MQTTClient(configTopics, mockMqttClientKeyStore, ses, fakeMqttClient);
        mqttClient.start();
        fakeMqttClient.waitForConnect(1000);

        Set<String> topics = new HashSet<>();
        topics.add("mqtt/topic");
        topics.add("mqtt/topic2");
        mqttClient.updateSubscriptions(topics, message -> {
        });

        List<String> subscriptions = fakeMqttClient.getSubscriptionTopics();
        assertThat(subscriptions, containsInAnyOrder("mqtt/topic", "mqtt/topic2"));

        mqttClient.stop();

        subscriptions = fakeMqttClient.getSubscriptionTopics();
        assertThat(fakeMqttClient.isConnected(), is(false));
        assertThat(subscriptions, hasSize(0));
    }

    @Test
    void GIVEN_subscribedMqttClient_WHEN_updateSubscriptions_THEN_subscriptionsUpdated() throws MQTTClientException {
        MQTTClient mqttClient = new MQTTClient(configTopics, mockMqttClientKeyStore, ses, fakeMqttClient);
        mqttClient.start();
        fakeMqttClient.waitForConnect(1000);

        // Initial subscriptions
        Set<String> topics = new HashSet<>();
        topics.add("mqtt/topic");
        topics.add("mqtt/topic2");
        mqttClient.updateSubscriptions(topics, message -> {
        });
        List<String> subscriptions = fakeMqttClient.getSubscriptionTopics();
        assertThat(subscriptions, containsInAnyOrder("mqtt/topic", "mqtt/topic2"));

        // Add new topics
        topics.add("mqtt/topic3");
        mqttClient.updateSubscriptions(topics, message -> {
        });
        mqttClient.updateSubscriptions(topics, message -> {
        });
        subscriptions = fakeMqttClient.getSubscriptionTopics();
        assertThat(subscriptions, containsInAnyOrder("mqtt/topic", "mqtt/topic2", "mqtt/topic3"));

        // Replace topics
        topics.clear();
        topics.add("mqtt/topic");
        topics.add("mqtt/topic2/changed");
        topics.add("mqtt/topic3/changed");
        mqttClient.updateSubscriptions(topics, message -> {
        });
        subscriptions = fakeMqttClient.getSubscriptionTopics();
        assertThat(subscriptions, containsInAnyOrder("mqtt/topic", "mqtt/topic2/changed", "mqtt/topic3/changed"));

        // Remove topics
        topics.remove("mqtt/topic");
        topics.remove("mqtt/topic3/changed");
        mqttClient.updateSubscriptions(topics, message -> {
        });
        subscriptions = fakeMqttClient.getSubscriptionTopics();
        assertThat(subscriptions, containsInAnyOrder("mqtt/topic2/changed"));

        topics.clear();
        mqttClient.updateSubscriptions(topics, message -> {
        });
        subscriptions = fakeMqttClient.getSubscriptionTopics();
        assertThat(subscriptions, hasSize(0));
    }

    @Test
    void GIVEN_subscribedMqttClient_WHEN_mqttMessageReceived_THEN_messageRoutedToHandler() throws Exception {
        MQTTClient mqttClient = new MQTTClient(configTopics, mockMqttClientKeyStore, ses, fakeMqttClient);
        mqttClient.start();
        fakeMqttClient.waitForConnect(1000);

        String t1 = "mqtt/topic";
        String t2 = "mqtt/topic2";
        byte[] m1 = "message from topic mqtt/topic".getBytes();
        byte[] m2 = "message from topic mqtt/topic2".getBytes();

        List<Message> receivedMessages = new ArrayList<>();

        // Initial subscriptions
        Set<String> topics = new HashSet<>();
        topics.add(t1);
        topics.add(t2);
        mqttClient.updateSubscriptions(topics, message -> {
            receivedMessages.add(message);
        });

        fakeMqttClient.injectMessage(t1, new MqttMessage(m1));
        fakeMqttClient.injectMessage(t2, new MqttMessage(m2));

        assertThat(receivedMessages, contains(new Message(t1, m1), new Message(t2, m2)));
    }

    @Test
    void GIVEN_mqttClient_WHEN_publish_THEN_routedToBroker() throws Exception {
        MQTTClient mqttClient = new MQTTClient(configTopics, mockMqttClientKeyStore, ses, fakeMqttClient);
        mqttClient.start();
        fakeMqttClient.waitForConnect(1000);

        byte[] messageFromPubsub = "message from pusub".getBytes();
        byte[] messageFromIotCore = "message from iotcore".getBytes();

        mqttClient.publish(new Message("from/pubsub", messageFromPubsub));
        mqttClient.publish(new Message("from/iotcore", messageFromIotCore));

        List<FakeMqttClient.TopicMessagePair> publishedMessages = fakeMqttClient.getPublishedMessages();
        assertThat(publishedMessages.size(), is(2));
        assertThat(publishedMessages.get(0).getTopic(), equalTo("from/pubsub"));
        assertThat(publishedMessages.get(0).getMessage().getPayload(), equalTo(messageFromPubsub));
        assertThat(publishedMessages.get(1).getTopic(), equalTo("from/iotcore"));
        assertThat(publishedMessages.get(1).getMessage().getPayload(), equalTo(messageFromIotCore));
    }

    @Test
    void GIVEN_mqttClient_WHEN_connectionLost_THEN_clientReconnectsAndResubscribes() throws Exception {
        MQTTClient mqttClient = new MQTTClient(configTopics, mockMqttClientKeyStore, ses, fakeMqttClient);
        mqttClient.start();
        fakeMqttClient.waitForConnect(1000);

        Set<String> topics = new HashSet<>();
        topics.add("mqtt/topic");
        topics.add("mqtt/topic2");
        mqttClient.updateSubscriptions(topics, message -> {
        });

        fakeMqttClient.injectConnectionLoss();

        assertThat(fakeMqttClient.isConnected(), is(true));
        assertThat(fakeMqttClient.getConnectCount(), is(2));
        assertThat(fakeMqttClient.getSubscriptionTopics(), containsInAnyOrder("mqtt/topic", "mqtt/topic2"));
    }

    @Test
    void GIVEN_mqttClient_WHEN_reset_THEN_connectsWithUpdatedSslContext() throws Exception {
        MQTTClientKeyStore mockKeyStore = mock(MQTTClientKeyStore.class);
        MQTTClient mqttClient = new MQTTClient(configTopics, mockKeyStore, ses, fakeMqttClient);
        mqttClient.start();
        fakeMqttClient.waitForConnect(1000);

        SSLSocketFactory mockSocketFactory = mock(SSLSocketFactory.class);
        when(mockKeyStore.getSSLSocketFactory()).thenReturn(mockSocketFactory);

        // This code assumes reset synchronously disconnects. This will need to be revisited if
        // this assumption changes and this test starts failing
        mqttClient.reset();
        fakeMqttClient.waitForConnect(1000);

        assertThat(fakeMqttClient.getConnectOptions().getSocketFactory(), is(mockSocketFactory));
        assertThat(fakeMqttClient.getConnectCount(), is(2));
    }

    @Test
    void GIVEN_mqttClient_WHEN_credentials_provided_THEN_connectsWithCredentials() throws Exception {
        configTopics.lookup(KernelConfigResolver.CONFIGURATION_CONFIG_KEY, BridgeConfig.KEY_USERNAME)
                .dflt("user");
        configTopics.lookup(KernelConfigResolver.CONFIGURATION_CONFIG_KEY, BridgeConfig.KEY_PASSWORD)
                .dflt("password");

        MQTTClientKeyStore mockKeyStore = mock(MQTTClientKeyStore.class);
        SSLSocketFactory mockSocketFactory = mock(SSLSocketFactory.class);
        when(mockKeyStore.getSSLSocketFactory()).thenReturn(mockSocketFactory);

        MQTTClient mqttClient = new MQTTClient(configTopics, mockKeyStore, ses, fakeMqttClient);
        mqttClient.start();
        fakeMqttClient.waitForConnect(1000);

        assertThat(fakeMqttClient.getConnectOptions().getUserName(), is("user"));
        assertThat(fakeMqttClient.getConnectOptions().getPassword(), is("password".toCharArray()));
    }

    @Test
    void GIVEN_mqttClient_WHEN_only_password_provided_THEN_connectsAnonymously() throws Exception {
        configTopics.lookup(KernelConfigResolver.CONFIGURATION_CONFIG_KEY, BridgeConfig.KEY_PASSWORD)
                .dflt("password");

        MQTTClientKeyStore mockKeyStore = mock(MQTTClientKeyStore.class);
        SSLSocketFactory mockSocketFactory = mock(SSLSocketFactory.class);
        when(mockKeyStore.getSSLSocketFactory()).thenReturn(mockSocketFactory);

        MQTTClient mqttClient = new MQTTClient(configTopics, mockKeyStore, ses, fakeMqttClient);
        mqttClient.start();
        fakeMqttClient.waitForConnect(1000);

        assertNull(fakeMqttClient.getConnectOptions().getUserName());
        assertNull(fakeMqttClient.getConnectOptions().getPassword());
    }
}
