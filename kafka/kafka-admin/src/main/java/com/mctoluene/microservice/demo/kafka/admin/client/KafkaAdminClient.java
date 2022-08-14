package com.mctoluene.microservice.demo.kafka.admin.client;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import com.mctoluene.microservice.demo.config.KafkaConfigData;
import com.mctoluene.microservice.demo.config.RetryConfigData;
import com.mctoluene.microservice.demo.kafka.admin.exception.KafkaClientException;

import reactor.core.publisher.Mono;

@Component
public class KafkaAdminClient {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaAdminClient.class);
    private final KafkaConfigData kafkaConfgData;
    private final RetryConfigData retryConfigData;
    private final AdminClient adminClient;
    private final RetryTemplate retryTemplate;
    private final WebClient webClient;

    public KafkaAdminClient(KafkaConfigData kafkaConfgData, RetryConfigData retryConfigData,
            AdminClient adminClient, RetryTemplate retryTemplate, WebClient webClient) {
        this.kafkaConfgData = kafkaConfgData;
        this.retryConfigData = retryConfigData;
        this.adminClient = adminClient;
        this.retryTemplate = retryTemplate;
        this.webClient = webClient;
    }

    public void createTopic() {
        CreateTopicsResult createTopicsResult;
        try {
            createTopicsResult = retryTemplate.execute(this::doCreateTopics);
            LOG.info("Create topic result {}", createTopicsResult.values().values());
        } catch (Throwable t) {
            throw new KafkaClientException("Reached max number of retry for creating kafka topic(s)!", t);
        }
        checkTopicCreated();
    }

    public void checkTopicCreated() {
        Collection<TopicListing> topicListings = getTopics();
        int retryCount = 1;
        Integer maxRetry = retryConfigData.getMaxAttempts();
        Integer multiplier = retryConfigData.getMultiplier().intValue();
        Long sleepTimeMs = retryConfigData.getSleepTimeMs();
        for (String topic : kafkaConfgData.getTopicNamesToCreate()) {
            while (!isTopicCreated(topicListings, topic)) {
                checkMaxRetry(retryCount++, maxRetry);
                sleep(sleepTimeMs);
                sleepTimeMs *= multiplier;
                topicListings = getTopics();
            }
        }
    }

    public void checkSchemaRegistry() {
        int retryCount = 1;
        Integer maxRetry = retryConfigData.getMaxAttempts();
        Integer multiplier = retryConfigData.getMultiplier().intValue();
        Long sleepTimeMs = retryConfigData.getSleepTimeMs();
        while (!getSchemaRegistryStatus().is2xxSuccessful()) {
            checkMaxRetry(retryCount++, maxRetry);
            sleep(sleepTimeMs);
            sleepTimeMs *= multiplier;
        }
    }

    private HttpStatus getSchemaRegistryStatus() {
        try {
            return webClient
                    .method(HttpMethod.GET)
                    .uri(kafkaConfgData.getSchemaRegistryUrl())
                    .exchangeToMono(response -> {
                        if (response.statusCode().is2xxSuccessful()) {
                            return Mono.just(response.statusCode());
                        } else
                            return Mono.just(HttpStatus.SERVICE_UNAVAILABLE);
                    }).block();
        } catch (Exception e) {
            return HttpStatus.SERVICE_UNAVAILABLE;

        }

    }

    private void sleep(Long sleepTimeMs) {
        try {
            Thread.sleep(sleepTimeMs);
        } catch (InterruptedException e) {
            throw new KafkaClientException("Error while sleeping for waiting for created topic(s)!");
        }
    }

    private void checkMaxRetry(int retry, Integer maxRetry) {
        if (retry > maxRetry)
            throw new KafkaClientException("Reached max number of retry for reading kafka topic(s)!");
    }

    private boolean isTopicCreated(Collection<TopicListing> topicListings, String topicName) {
        if (topicListings == null)
            return false;
        return topicListings.stream().anyMatch(topic -> topic.name().equals(topicName));
    }

    private CreateTopicsResult doCreateTopics(RetryContext retrycontext) {
        List<String> topicNames = kafkaConfgData.getTopicNamesToCreate();
        LOG.info("Creating {} topic(s), attempt {}", topicNames.size(), retrycontext.getRetryCount());
        List<NewTopic> topics = topicNames.stream().map(n -> new NewTopic(n.trim(),
                kafkaConfgData.getNumberOfPartitions(), kafkaConfgData.getReplicationFactor()))
                .collect(Collectors.toList());
        var create = adminClient.createTopics(topics);
        return create;
    }

    private Collection<TopicListing> getTopics() {
        Collection<TopicListing> topicListings;
        try {
            topicListings = retryTemplate.execute(this::doGetTopics);
        } catch (Throwable t) {
            throw new KafkaClientException("Reached max number of retry for reading kafka topic(s)!", t);
        }
        return topicListings;
    }

    private Collection<TopicListing> doGetTopics(RetryContext retrycontext)
            throws InterruptedException, ExecutionException {
        LOG.info("Reading {} topic(s), attempt {}", kafkaConfgData.getTopicNamesToCreate().toArray(),
                retrycontext.getRetryCount());
        Collection<TopicListing> topicListings = adminClient.listTopics().listings().get();
        if (topicListings != null)
            topicListings.forEach(topic -> LOG.info("Topic with name {}", topic.name()));
        return topicListings;
    }
}
