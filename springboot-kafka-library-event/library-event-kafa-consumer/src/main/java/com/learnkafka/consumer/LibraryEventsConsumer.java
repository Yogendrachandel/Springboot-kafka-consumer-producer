package com.learnkafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class LibraryEventsConsumer {

    private static final Logger log = LoggerFactory.getLogger(LibraryEventsConsumer.class);

    /*This listener read the msgs from multiple topic also.
     we can also add multiple topic by , seprated.
     This method Taking String as argumnet ,but if u want to levaerage another info
     like topicname , partion and header values we can use ConsumerRecord class as below one.
     */
 /*   @KafkaListener(
            topics = {"test-topic"}
            , autoStartup = "${libraryListener.startup:true}"
            , groupId = "library-events-listener-group")
    public void consumer(String libraryEvent){
        log.info(" The Consumed msg :: {}"+libraryEvent);

    } */


    @KafkaListener(
            topics = {"library-topic"}
            , autoStartup = "${libraryListener.startup:true}"
            , groupId = "library-events-listener-group")
    public void consumerWithConsumerRecord(ConsumerRecord<Integer,String> consumerRecord){
        log.info("consumed msg from consumerRecord \n"+
                "topicName : {} \n"+
                "partition : {} \n"+
                "Key : {} \n"+
                "Header value :{} \n"+
                 "Msg : {}",consumerRecord.topic(),consumerRecord.partition(),consumerRecord.key(),consumerRecord.headers(),consumerRecord.value());

    }

    /*
      If we have one consumer within the Consumer-group-id then all N number of partiotions
      read by single  consumer.
      But if we have N number of Consumer with same Consumer-group-id then each consumer will read
      the msgs only from the partitions that is assigned to that consumer not from all partitions

      we can simulate this behaviour by running the same kafka consumer application with different
      port .
     */
}
