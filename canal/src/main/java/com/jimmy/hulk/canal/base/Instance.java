package com.jimmy.hulk.canal.base;

import com.jimmy.hulk.canal.core.CanalMessage;
import com.jimmy.hulk.canal.core.CanalPosition;

import java.util.List;
import java.util.concurrent.TimeUnit;

public interface Instance {

    void start();

    void stop();

    void subscribe();

    void unsubscribe();

    void point(CanalPosition canalPosition);

    void ack(long batchId);

    void rollback();

    void destroy();

    CanalMessage get(int batchSize, Long timeout, TimeUnit unit);
}
