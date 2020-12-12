package com.cihangir.kafka.model;

import java.time.LocalDate;

public class MessageDto {

    private String message;

    private LocalDate createdDate = LocalDate.now();

    public MessageDto(String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public LocalDate getCreatedDate() {
        return createdDate;
    }

    public void setCreatedDate(LocalDate createdDate) {
        this.createdDate = createdDate;
    }

}
