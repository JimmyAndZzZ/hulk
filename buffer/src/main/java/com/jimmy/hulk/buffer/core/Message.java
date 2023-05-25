package com.jimmy.hulk.buffer.core;

import lombok.Getter;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicBoolean;

public class Message implements Serializable {

    @Getter
    private String body;

    @Getter
    private Long offset;

    private AtomicBoolean durable;

    public Message(String body, Long offset) {
        this.body = body;
        this.offset = offset;
        this.durable = new AtomicBoolean(false);
    }

    public boolean getDurable() {
        return durable.get();
    }
}
