package com.jimmy.hulk.data.core;

import com.jimmy.hulk.data.notify.ImportNotify;
import lombok.Data;

import java.io.File;
import java.io.Serializable;

@Data
public class Dump implements Serializable {

    private File file;

    private String alias;

    private String table;

    private ImportNotify notify;
}
