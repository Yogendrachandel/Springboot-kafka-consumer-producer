package com.learnkafka.consumer;


import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/*

This class is used only when manual commit is required (default commit is BATCH)
otherwise we can proceed with class - LibraryEventsConsumer and comment this class.
 */
//@Component
@Slf4j
public class LibraryEventsConsumerManualOffset implements AcknowledgingMessageListener<Integer,String> {
    private static final Logger log = LoggerFactory.getLogger(LibraryEventsConsumerManualOffset.class);

    @Override
    @KafkaListener(topics = {"library-topic"}
            , groupId = "library-events-listener-group")
     public void onMessage(ConsumerRecord<Integer, String> consumerRecord, Acknowledgment acknowledgment) {
        log.info("Manual commit consumed msg from consumerRecord \n"+
                "topicName : {} \n"+
                "partition : {} \n"+
                "Key : {} \n"+
                "Header value :{} \n"+
                "Msg : {}",consumerRecord.topic(),consumerRecord.partition(),consumerRecord.key(),consumerRecord.headers(),consumerRecord.value());
        acknowledgment.acknowledge();// manual commit
    }
}
