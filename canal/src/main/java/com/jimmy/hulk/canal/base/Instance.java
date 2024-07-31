package com.jimmy.hulk.canal.base;

import com.jimmy.hulk.canal.core.CanalPosition;

public interface Instance {

    void start();

    void stop();

    void subscribe();

    void unsubscribe();

    void point(CanalPosition canalPosition);
}
