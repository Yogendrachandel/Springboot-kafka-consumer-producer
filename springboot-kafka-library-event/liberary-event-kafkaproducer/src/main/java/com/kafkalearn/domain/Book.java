package com.kafkalearn.domain;



//It can be class also
public record Book(

        Integer bookId,

        String bookName,

        String bookAuthor
) {
}
