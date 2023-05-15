package com.jimmy.hulk.buffer.core;

import lombok.Data;

import java.io.Serializable;

@Data
public class Message implements Serializable {

    private String body;

    private Long offset;

    public Message(String body, Long offset) {
        this.body = body;
        this.offset = offset;
    }
}
