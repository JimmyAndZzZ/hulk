package com.jimmy.hulk.canal.instance;

import com.jimmy.hulk.canal.base.Instance;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;

public abstract class AbstractInstance implements Instance {

    protected final String destination;

    protected volatile boolean running = false; // 是否处于运行中

    protected AbstractInstance(String destination) {
        this.destination = destination;
    }

    public boolean isStart() {
        return running;
    }

    public void start() {
        if (running) {
            throw new HulkException(this.getClass().getName() + " has startup , don't repeat start", ModuleEnum.CANAL);
        }

        running = true;
    }

    public void stop() {
        if (!running) {
            throw new HulkException(this.getClass().getName() + " isn't start , please check", ModuleEnum.CANAL);
        }

        running = false;
    }
}
