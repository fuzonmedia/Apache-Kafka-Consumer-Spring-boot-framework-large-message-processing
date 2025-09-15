package com.consumer.store.service;

//import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

//@Slf4j
@Service
public final class KafkaProducerService {
    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerService.class);
    @Autowired
    private Environment env;
    private final KafkaTemplate<String, String> kafkaTemplate;
    //private final String TOPIC = env.getProperty("spring.kafka.topic");
    //private final String TOPIC = "gcp.store.inventory.all.sync.0";

    public KafkaProducerService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }
    public void sendMessage(String message,String topic) {
        //logger.info(String.format("$$$$ => Producing message: %s", message));
        //log.info(message);

        ListenableFuture<SendResult<String, String>> future = this.kafkaTemplate.send(topic, message);
        future.addCallback(new ListenableFutureCallback<>() {
            @Override
            public void onFailure(Throwable ex) {
                //logger.info("Unable to send message=[ {} ] due to : {}", message, ex.getMessage());
                //log.info("unable to process message : "+ex.getMessage());
            }

            @Override
            public void onSuccess(SendResult<String, String> result) {
                //log.info("Sent message=[ {} ] with offset=[ {} ]", message, result.getRecordMetadata().offset());
            }
        });
    }
}