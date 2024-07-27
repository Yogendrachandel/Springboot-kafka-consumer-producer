package com.kafkalearn.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kafkalearn.domain.LibraryEvent;
import com.kafkalearn.domain.LibraryEventType;
import com.kafkalearn.producer.LibraryEventProducer;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.bind.annotation.*;

import java.util.concurrent.ExecutionException;

@RestController
@Slf4j // we have use lombok
public class LibraryEventsController {


    @Autowired
    private LibraryEventProducer libraryEventProducer;


    @Value("${topic.name}")
    String topic;


    private static final Logger log = LoggerFactory.getLogger(LibraryEventsController.class);

    @PostMapping("/v1/libraryevent")
    public ResponseEntity<?> postLibraryEvent(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException {

        if (LibraryEventType.NEW != libraryEvent.libraryEventType()) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Only NEW event type is supported");
        }
        log.info("Going to send Library event on KAFKA -" + topic);

        /* WAy-1
          //Asysnchronous call method
            ListenableFuture<SendResult<Integer, String>> result = libraryEventProducer.sendKafkaProducerLibraryEvent(libraryEvent);
 */

        /*
          //synchronous call method - WAY2
         SendResult<Integer, String> result = libraryEventProducer.sendKafkaProducerLibraryEventSynchronouly(libraryEvent);
 */

         //3rd way-  to send mesg to kafka with ProducerRecord class that contains another values like -
        // topic, partition, Headers ,Key,value Timestamp
        libraryEventProducer.sendKafkaMsgWithProducerRecord(libraryEvent);

        log.info(" Library event Finished...");

        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }



    /*
     Here we are sending the library event with key. same key will always send to same partition.
     Key should be as per this key-serializer mentioned in yml
     */

    @PutMapping("/v1/libraryevent")
    public ResponseEntity<?> postLibraryEventWithHeader(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException {

        ResponseEntity<String> BAD_REQUEST = validateLibraryEvent(libraryEvent);
        if (BAD_REQUEST != null) return BAD_REQUEST;

        log.info("Going to send Library event on KAFKA topic : {}  with  key : {}" ,topic, libraryEvent.libraryEventId());
          //synchronous call method - WAY2
         SendResult<Integer, String> result = libraryEventProducer.sendKafkaProducerLibraryEventSynchronouslyWithKey(libraryEvent,libraryEvent.libraryEventId());
        log.info("Library event Finished...");

        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }



    private static ResponseEntity<String> validateLibraryEvent(LibraryEvent libraryEvent) {
        if (libraryEvent.libraryEventId() == null) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Please pass the LibraryEventId");
        }

        if (!LibraryEventType.UPDATE.equals(libraryEvent.libraryEventType()))  {
            log.info("Inside the if block");
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Only UPDATE event type is supported");
        }
        return null;
    }

}
