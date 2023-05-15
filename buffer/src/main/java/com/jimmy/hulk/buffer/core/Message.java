package com.jimmy.hulk.buffer.core;

import lombok.Data;

import java.io.Serializable;

@Data
public class Message implements Serializable {

    private String body;

    private Long offset;
}
