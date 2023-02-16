package com.jimmy.hulk.data.other;

import com.google.common.collect.Lists;
import lombok.Data;

import java.util.List;

@Data
public class ExcelProperties {

    private String fileName;

    private List<List<String>> head = Lists.newArrayList();

    public ExcelProperties(String fileName, List<List<String>> head) {
        this.fileName = fileName;
        this.head = head;
    }
}
