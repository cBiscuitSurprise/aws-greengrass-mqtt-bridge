/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqttbridge.clients;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.mqttbridge.BridgeConfig;
import com.aws.greengrass.mqttbridge.Message;
import com.aws.greengrass.mqttbridge.auth.MQTTClientKeyStore;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttPersistenceException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.net.URI;
import java.security.KeyStoreException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import javax.net.ssl.SSLSocketFactory;

public class MQTTClient implements MessageClient {
    private static final Logger LOGGER = LogManager.getLogger(MQTTClient.class);

    public static final String ERROR_MISSING_USERNAME = "Password provided without username";

    public static final String TOPIC = "topic";

    private final SubscribeTask subscribeTask;
    private final ConnectTask connectTask;

    private final MQTTClientKeyStore.UpdateListener updateListener = this::reset;
    private Consumer<Message> messageHandler;

    private final Config config;
    private final MqttClientPersistence dataStore;
    private final IMqttClient mqttClientInternal;

    private final MqttCallback mqttCallback = new MqttCallback() {
        @Override
        public void connectionLost(Throwable cause) {
            LOGGER.atDebug().setCause(cause).log("MQTT client disconnected, reconnecting...");
            try {
                connectAndResubscribe().get();
            } catch (Exception e) {
                System.out.println(e);
            }
        }

        @Override
        public void messageArrived(String topic, org.eclipse.paho.client.mqttv3.MqttMessage message) {
            LOGGER.atTrace().kv(TOPIC, topic).log("Received MQTT message");

            if (messageHandler == null) {
                LOGGER.atWarn().kv(TOPIC, topic).log("MQTT message received but message handler not set");
            } else {
                Message msg = new Message(topic, message.getPayload());
                messageHandler.accept(msg);
            }
        }

        @Override
        public void deliveryComplete(IMqttDeliveryToken token) {
        }
    };

    @Value
    @Builder
    public static class Config {
        @NonNull
        URI brokerUri;
        @NonNull
        String clientId;
        @NonNull
        String username;
        @NonNull
        String password;
        @NonNull
        MQTTClientKeyStore mqttClientKeyStore;
        @NonNull
        ExecutorService executorService;
    }

    /**
     * Construct an MQTTClient.
     *
     * @param config MQTTClient configuration
     * @throws MQTTClientException if unable to create client for the mqtt broker
     */
    public MQTTClient(Config config) throws MQTTClientException {
        this(config, null);
    }

    protected MQTTClient(Config config, IMqttClient mqttClient) throws MQTTClientException {
        if (config.getUsername().isEmpty() && !config.getPassword().isEmpty()) {
            throw new MQTTClientException(ERROR_MISSING_USERNAME);
        }
        this.config = config;
        this.connectTask = new ConnectTask(this.config.getExecutorService());
        this.subscribeTask = new SubscribeTask(this.config.getExecutorService());
        this.dataStore = new MemoryPersistence();

        try {
            this.mqttClientInternal = mqttClient == null ? new MqttClient(
                    config.getBrokerUri().toString(),
                    config.getClientId(),
                    this.dataStore
            ) : mqttClient;
        } catch (MqttException e) {
            throw new MQTTClientException("Unable to create an MQTT client", e);
        }
    }

    void reset() {
        if (mqttClientInternal.isConnected()) {
            try {
                mqttClientInternal.disconnect();
            } catch (MqttException e) {
                LOGGER.atError().setCause(e).log("Failed to disconnect MQTT client");
                return;
            }
        }

        connectAndResubscribe();
    }

    /**
     * Start the {@link MQTTClient}.
     */
    public CompletableFuture<Void> start() {
        mqttClientInternal.setCallback(mqttCallback);
        config.getMqttClientKeyStore().listenToUpdates(updateListener);
        return connectAndResubscribe();
    }

    /**
     * Stop the {@link MQTTClient}.
     */
    public void stop() {
        config.getMqttClientKeyStore().unsubscribeToUpdates(updateListener);

        try {
            subscribeTask.unsubscribe().get(10, TimeUnit.SECONDS);
            subscribeTask.shutdown(10, TimeUnit.SECONDS);
        } catch (TimeoutException | ExecutionException | InterruptedException e) {
            LOGGER.atDebug().setCause(e).log("subscribe task shutdown not clean");
        }

        try {
            connectTask.disconnect().get(10, TimeUnit.SECONDS);
            connectTask.shutdown(10, TimeUnit.SECONDS);
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            LOGGER.atDebug().setCause(e).log("connect task shutdown not clean");
        }

        try {
            dataStore.close();
        } catch (MqttPersistenceException e) {
            LOGGER.atError().setCause(e).log("Failed to close data store");
        }
    }

    @Override
    public void publish(Message message) throws MessageClientException {
        try {
            mqttClientInternal
                    .publish(message.getTopic(), new org.eclipse.paho.client.mqttv3.MqttMessage(message.getPayload()));
        } catch (MqttException e) {
            LOGGER.atError().setCause(e).kv(TOPIC, message.getTopic()).log("MQTT publish failed");
            throw new MQTTClientException("Failed to publish message", e);
        }
    }

    @Override
    public boolean supportsTopicFilters() {
        return true;
    }

    @Override
    public void updateSubscriptions(Set<String> topics, Consumer<Message> messageHandler) {
        this.messageHandler = messageHandler;
        try {
            subscribeTask.subscribe(topics).get();
            LOGGER.atDebug().kv("topics", topics).log("Updated local MQTT topics to subscribe");
        } catch (Exception e) {
            // TODO throw?
            LOGGER.atError().log("whoops");
        }
    }

    private CompletableFuture<Void> connectAndResubscribe() {
        return connectTask.connect().thenCompose(connect -> subscribeTask.resubscribe());
    }

    private class ConnectTask extends Task {

        private static final int MIN_WAIT_RETRY_IN_SECONDS = 1;
        private static final int MAX_WAIT_RETRY_IN_SECONDS = 120;

        private final AtomicReference<MqttConnectOptions> connectOptions = new AtomicReference<>();

        private CountDownLatch connectComplete;
        private final AtomicReference<Future<?>> connectAttempt = new AtomicReference<>();
        private final AtomicInteger waitBeforeRetry = new AtomicInteger(MIN_WAIT_RETRY_IN_SECONDS);
        private final ScheduledExecutorService ses = Executors.newScheduledThreadPool(1);


        ConnectTask(ExecutorService executorService) {
            super(executorService);
        }

        public CompletableFuture<Void> connect() {
            return start(this::doConnect);
        }

        private void doConnect(AtomicReference<Runnable> cancelCallback) throws
                MQTTClientException, InterruptedException {
            // gracefully stop connecting when asked
            cancelCallback.set(() -> {
                // cancel current connection attempt
                Future<?> conn = connectAttempt.get();
                if (conn != null) {
                    conn.cancel(true);
                }
                // stop waiting for connection attempt to complete
                if (connectComplete != null) {
                    connectComplete.countDown();
                }
            });

            try {
                connectOptions.set(buildConnectOptions());
            } catch (KeyStoreException e) {
                throw new MQTTClientException("Unable to load key store", e);
            }

            LOGGER.atInfo().kv("uri", config.getBrokerUri()).kv(BridgeConfig.KEY_CLIENT_ID, config.getClientId())
                    .log("Connecting to broker");

            connectComplete = new CountDownLatch(1);
            waitBeforeRetry.set(MIN_WAIT_RETRY_IN_SECONDS);
            attemptConnection();
            connectComplete.await();
        }

        private void attemptConnection() {
            if (!mqttClientInternal.isConnected()) {
                try {
                    mqttClientInternal.connect(connectOptions.get());
                } catch (MqttException e) {
                    waitBeforeRetry.set(Math.min(2 * waitBeforeRetry.get(), MAX_WAIT_RETRY_IN_SECONDS));
                    LOGGER.atDebug().setCause(e)
                            .log("Unable to connect. Will be retried after {} seconds", waitBeforeRetry.get());
                    connectAttempt.set(ses.schedule(this::attemptConnection, waitBeforeRetry.get(), TimeUnit.SECONDS));
                    return;
                }
            }
            LOGGER.atInfo().kv("uri", config.getBrokerUri()).kv(BridgeConfig.KEY_CLIENT_ID, config.getClientId())
                    .log("Connected to broker");
            connectComplete.countDown();
        }

        private MqttConnectOptions buildConnectOptions() throws KeyStoreException {
            MqttConnectOptions connectOptions = new MqttConnectOptions();

            //TODO: persistent session could be used
            connectOptions.setCleanSession(true);

            if ("ssl".equalsIgnoreCase(config.getBrokerUri().getScheme())) {
                SSLSocketFactory ssf = config.getMqttClientKeyStore().getSSLSocketFactory();
                connectOptions.setSocketFactory(ssf);
            }

            if (!config.getUsername().isEmpty()) {
                connectOptions.setUserName(config.getUsername());
                if (!config.getPassword().isEmpty()) {
                    connectOptions.setPassword(config.getPassword().toCharArray());
                }
            }

            return connectOptions;
        }

        public CompletableFuture<Void> disconnect() {
            return replace(this::doDisconnect);
        }

        private void doDisconnect(AtomicReference<Runnable> cancelCallback) {
            if (mqttClientInternal.isConnected()) {
                try {
                    mqttClientInternal.disconnect();
                } catch (MqttException e) {
                    LOGGER.atError().setCause(e).log("Failed to disconnect MQTT client");
                }
            }
        }

        @Override
        protected void onShutdown() {
            ses.shutdownNow();
        }
    }

    class SubscribeTask extends Task {

        private final Set<String> subscribedLocalMqttTopics = new HashSet<>();
        private final Set<String> toSubscribeLocalMqttTopics = new HashSet<>();

        SubscribeTask(ExecutorService executorService) {
            super(executorService);
        }

        CompletableFuture<Void> subscribe() {
            return subscribe(Collections.emptySet(), false);
        }

        CompletableFuture<Void> subscribe(Set<String> toSubscribeLocalMqttTopics) {
            return subscribe(toSubscribeLocalMqttTopics, false);
        }

        CompletableFuture<Void> resubscribe() {
            return subscribe(Collections.emptySet(), true);
        }

        private CompletableFuture<Void> subscribe(Set<String> toSubscribeLocalMqttTopics, boolean resubscribe) {
            // TODO clean this up
            AtomicBoolean cancelled = new AtomicBoolean();
            return start(cb -> {
                if (resubscribe) {
                    this.subscribedLocalMqttTopics.clear();
                }
                this.toSubscribeLocalMqttTopics.addAll(toSubscribeLocalMqttTopics);
                cb.set(() -> cancelled.set(true));
                doSubscribe(cancelled);
            });
        }

        private void doSubscribe(AtomicBoolean cancelled) {
            if (!mqttClientInternal.isConnected()) {
                return;
            }

            Set<String> topicsToRemove = new HashSet<>(subscribedLocalMqttTopics);
            topicsToRemove.removeAll(toSubscribeLocalMqttTopics);

            for (String s : topicsToRemove) {
                if (cancelled.get() || !mqttClientInternal.isConnected()) {
                    return;
                }
                try {
                    mqttClientInternal.unsubscribe(s);
                    LOGGER.atDebug().kv(TOPIC, s).log("Unsubscribed from topic");
                    subscribedLocalMqttTopics.remove(s);
                } catch (MqttException e) {
                    LOGGER.atError().kv(TOPIC, s).setCause(e).log("Unable to unsubscribe");
                    // If we are unable to unsubscribe, leave the topic in the set so that we can try to remove next time.
                }
            }

            Set<String> topicsToSubscribe = new HashSet<>(toSubscribeLocalMqttTopics);
            topicsToSubscribe.removeAll(subscribedLocalMqttTopics);

            // TODO: Support configurable qos, add retry
            for (String s : topicsToSubscribe) {
                if (cancelled.get() || !mqttClientInternal.isConnected()) {
                    return;
                }
                try {
                    mqttClientInternal.subscribe(s);
                    LOGGER.atDebug().kv(TOPIC, s).log("Subscribed to topic");
                    subscribedLocalMqttTopics.add(s);
                } catch (MqttException e) {
                    LOGGER.atError().kv(TOPIC, s).log("Failed to subscribe");
                }
            }
        }

        CompletableFuture<Void> unsubscribe() {
            return replace(cancel -> {
                LOGGER.atDebug().kv("mapping", subscribedLocalMqttTopics).log("Unsubscribe from local MQTT topics");
                this.subscribedLocalMqttTopics.forEach(s -> {
                    try {
                        mqttClientInternal.unsubscribe(s);
                        LOGGER.atDebug().kv(TOPIC, s).log("Unsubscribed from topic");
                    } catch (MqttException e) {
                        LOGGER.atWarn().kv(TOPIC, s).setCause(e).log("Unable to unsubscribe");
                    }
                });
                subscribedLocalMqttTopics.clear();
            });
        }
    }

    interface Action {
        void perform(AtomicReference<Runnable> cancelledCallback) throws Exception;
    }

     abstract static class Task {

        private final AtomicReference<CompletableFuture<Void>> taskFuture =
                new AtomicReference<>(CompletableFuture.completedFuture(null));
        private AtomicReference<Runnable> cancelledCallback;
        private final AtomicBoolean shutdown = new AtomicBoolean();
        private final ExecutorService executorService;

        protected Task(ExecutorService executorService) {
            this.executorService = executorService;
        }

        protected CompletableFuture<Void> start(@NonNull Action action) {
            return start(action, false);
        }

        protected CompletableFuture<Void> replace(@NonNull Action action) {
            return start(action, true);
        }

        private CompletableFuture<Void> start(@NonNull Action action, boolean replace) {
            if (shutdown.get()) {
                // TODO throw exception here?
                return CompletableFuture.completedFuture(null);
            }

            return taskFuture.getAndUpdate(f -> {
                if (replace && !f.isDone()) {
                    try {
                        cancel(10, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        // ignore TODO
                    }
                }

                if (f.isDone()) {
                    cancelledCallback = new AtomicReference<>(() -> {
                    });
                    return CompletableFuture.runAsync(() -> {
                        try {
                            action.perform(cancelledCallback);
                        } catch (Exception e) {
                            throw new CompletionException(e);
                        }
                    }, executorService);
                }
                return f;
            });
        }

        /**
         * Cancel the currently running task.
         *
         * @param time time to wait
         * @param unit unit of time
         * @throws ExecutionException   if the task failed
         * @throws InterruptedException if the task was interrupted
         * @throws TimeoutException     if we timed out waiting
         */
        public void cancel(long time, TimeUnit unit) throws ExecutionException, InterruptedException, TimeoutException {
            Runnable cb = cancelledCallback.get();
            if (cb != null) {
                cb.run();
            }
            taskFuture.get().get(time, unit);
        }

        /**
         * Cancel the currently running task and
         * prevent future tasks from starting.
         *
         * @param time time to wait
         * @param unit unit of time
         * @throws ExecutionException   if the task failed
         * @throws InterruptedException if the task was interrupted
         * @throws TimeoutException     if we timed out waiting
         */
        public void shutdown(long time, TimeUnit unit)
                throws ExecutionException, InterruptedException, TimeoutException {
            shutdown.set(true);
            cancel(time, unit);
            onShutdown();
        }

        protected void onShutdown() {
        }
    }
}
