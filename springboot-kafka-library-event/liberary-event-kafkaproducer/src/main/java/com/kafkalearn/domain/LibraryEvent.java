package com.kafkalearn.domain;




//It can be also class but introduce in java 14 for data container
public record LibraryEvent(
        Integer libraryEventId,
        LibraryEventType libraryEventType,


        Book book
) {
}
