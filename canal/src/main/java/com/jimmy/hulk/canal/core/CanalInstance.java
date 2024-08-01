package com.jimmy.hulk.canal.core;

import com.jimmy.hulk.canal.base.Instance;
import lombok.Data;

import java.io.Serializable;
import java.util.concurrent.ExecutorService;

@Data
public class CanalInstance implements Serializable {

    private Instance instance;

    private ExecutorService executorService;
}
