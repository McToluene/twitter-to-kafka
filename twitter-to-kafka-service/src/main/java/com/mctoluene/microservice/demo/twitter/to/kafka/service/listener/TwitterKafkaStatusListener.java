package com.mctoluene.microservice.demo.twitter.to.kafka.service.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.mctoluene.microservice.demo.config.KafkaConfigData;
import com.mctoluene.microservice.demo.kafka.avro.model.TwitterAvroModel;
import com.mctoluene.microservice.demo.kafka.producer.config.service.KafkaProducer;
import com.mctoluene.microservice.demo.twitter.to.kafka.service.transform.TwitterStatusToAvroTransformer;

import twitter4j.Status;
import twitter4j.StatusAdapter;

@Component
public class TwitterKafkaStatusListener extends StatusAdapter {
    public static final Logger LOG = LoggerFactory.getLogger(TwitterKafkaStatusListener.class);
    private final KafkaConfigData kafkaConfigData;
    private final KafkaProducer<Long, TwitterAvroModel> kafkaProducer;
    private final TwitterStatusToAvroTransformer twitterStatusToAvroTransformer;

    public TwitterKafkaStatusListener(KafkaConfigData kafkaConfigData,
            KafkaProducer<Long, TwitterAvroModel> kafkaProducer,
            TwitterStatusToAvroTransformer twitterStatusToAvroTransformer) {
        this.kafkaConfigData = kafkaConfigData;
        this.kafkaProducer = kafkaProducer;
        this.twitterStatusToAvroTransformer = twitterStatusToAvroTransformer;
    }

    @Override
    public void onStatus(Status status) {
        super.onStatus(status);
        LOG.info("Recieved status with text {} sending to kafka topic {}", status.getText(),
                kafkaConfigData.getTopicName());
        TwitterAvroModel model = twitterStatusToAvroTransformer.geTwitterAvroModelFromStatus(status);
        kafkaProducer.send(kafkaConfigData.getTopicName(), model.getUserId(), model);
    }
}
