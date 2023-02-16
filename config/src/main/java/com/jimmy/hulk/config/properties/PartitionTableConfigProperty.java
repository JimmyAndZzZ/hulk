package com.jimmy.hulk.config.properties;

import com.google.common.collect.Sets;
import lombok.Data;

import java.io.Serializable;
import java.util.Set;

@Data
public class PartitionTableConfigProperty implements Serializable {

    private String range;

    private String prefix;

    private String dsName;

    private Set<String> tables = Sets.newHashSet();
}
