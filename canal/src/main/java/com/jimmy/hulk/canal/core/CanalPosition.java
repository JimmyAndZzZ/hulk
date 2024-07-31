package com.jimmy.hulk.canal.core;

import lombok.Data;

import java.io.Serializable;

@Data
public class CanalPosition implements Serializable {

    private Long timestamp;

    private Long position;

    private String gtid;

    private String journalName;
}
