package com.jimmy.hulk.actuator.memory;

import com.jimmy.hulk.actuator.base.Segment;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class BaseSegment implements Segment {

    protected AtomicBoolean free = new AtomicBoolean(true);

    @Override
    public boolean isFree() {
        return free.get();
    }
}
