package com.kafkalearn.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkalearn.domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Component
@Slf4j
public class LibraryEventProducer {


    private static final Logger log = LoggerFactory.getLogger(LibraryEventProducer.class);
    @Autowired
    private KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Value("${topic.name}")
    String topic;


    //Asynchronous call to send method, controller method finished before this method
    public ListenableFuture<SendResult<Integer, String>> sendKafkaProducerLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException {

        String value = objectMapper.writeValueAsString(libraryEvent);
        log.info("Kafka Producer going to send msgs ASYNCHRONOUSLY.....");

        // here result will return in future
        ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.send(topic, value);
        /*CompletableFuture when method has two argument one is SendResult is the one that hold complete
        info like on  which topic on which partition msg send successfully.
         */

        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
            @Override
            public void onFailure(Throwable ex) {
                handleFailure(value, ex);
            }

            @Override
            public void onSuccess(SendResult<Integer, String> result) {

                handelSuccess(result);
            }
        });

        return listenableFuture;

    }

    private void handelSuccess(SendResult<Integer, String> sendResult) {
        log.info("Msg with value : {} and partition :{}", sendResult.getProducerRecord().value(), sendResult.getRecordMetadata().partition());
    }

    private void handleFailure(String value, Throwable throwable) {
        log.error("error occurred while sending msg to kafka : {}", throwable.getMessage());
    }


    //Synchronous call
    public SendResult<Integer,String> sendKafkaProducerLibraryEventSynchronouly(LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException {

        String value = objectMapper.writeValueAsString(libraryEvent);
        log.info("Kafka Producer going to send as SYNCHRONOUS call .....");
        // here get() method make its blocking call
        SendResult<Integer,String> sendResult = kafkaTemplate.send(topic, value).get();
        handelSuccess(sendResult);
        return sendResult;
    }


    //Send msg with ProducerRecord  with -topic, partition, Headers ,Key,value Timestamp
    public ListenableFuture<SendResult<Integer, String>> sendKafkaMsgWithProducerRecord(LibraryEvent libraryEvent) throws JsonProcessingException {

        String value = objectMapper.writeValueAsString(libraryEvent);
        log.info("Kafka Producer going to send as ASYNCHRONOUSLY with Producer class .....");

        //create header info
        List<Header> recordHeaders = List.of(new RecordHeader("event-source", "scanner".getBytes()));
        //producerRecord with null partition and null key
        ProducerRecord<Integer,String> producerRecord = new ProducerRecord<Integer,String>(topic,null,null,value,recordHeaders);

        //Asynchronous
        ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.send(producerRecord);
        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
            @Override
            public void onFailure(Throwable ex) {
                handleFailure(value, ex);
            }

            @Override
            public void onSuccess(SendResult<Integer, String> result) {

                handelSuccess(result);
            }
        });

        return listenableFuture;

    }

    /*
    Send data with key
  */
    public SendResult<Integer, String> sendKafkaProducerLibraryEventSynchronouslyWithKey(LibraryEvent libraryEvent,Integer key) throws JsonProcessingException, ExecutionException, InterruptedException {

        String value = objectMapper.writeValueAsString(libraryEvent);
        log.info("Kafka Producer going to send as SYNCHRONOUS call with key :{}",key);
        // here get() method make its blocking call
        SendResult<Integer,String> sendResult = kafkaTemplate.send(topic,key,value).get();

        handelSuccessWithKey(sendResult);
        return sendResult;
    }


    private void handelSuccessWithKey(SendResult<Integer, String> sendResult) {
        log.info("Msg with value :{} and partition :{} and key :{}", sendResult.getProducerRecord().value(), sendResult.getRecordMetadata().partition() ,sendResult.getProducerRecord().key());
    }
}
