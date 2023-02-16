package com.jimmy.hulk.actuator.core;

import lombok.Data;

import java.io.Serializable;

@Data
public class Null implements Serializable {

    private Null() {

    }

    public static Null build() {
        return null;
    }
}
