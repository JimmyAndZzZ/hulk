package com.jimmy.hulk.buffer.core;

import lombok.Getter;

import java.io.Serializable;

public class Message implements Serializable {

    @Getter
    private String body;

    @Getter
    private Long offset;

    @Getter
    private Boolean poisonPill = false;

    public Message(String body, Long offset) {
        this.body = body;
        this.offset = offset;
    }

    public Message(Boolean poisonPill) {
        this.poisonPill = poisonPill;
    }

}
