package com.jimmy.hulk.actuator.base;

public interface Segment {

    boolean write(byte[] bytes);

    byte[] read();

    void free();

    boolean isFree();
}
