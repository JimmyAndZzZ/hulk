package com.jimmy.hulk.parse.core.result;

import com.google.common.collect.Lists;
import com.jimmy.hulk.parse.core.element.*;
import com.jimmy.hulk.parse.enums.ResultTypeEnum;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class ParseResultNode implements Serializable {

    private String sql;

    private Integer fetch;

    private Integer offset;

    private ResultTypeEnum resultType;

    private ExtraNode extraNode;

    private List<ColumnNode> columns = Lists.newArrayList();

    private List<TableNode> tableNodes = Lists.newArrayList();

    private List<OrderNode> orderNodes = Lists.newArrayList();

    private List<RelationNode> relationNodes = Lists.newArrayList();

    private List<ConditionGroupNode> whereConditionNodes = Lists.newArrayList();

    private List<PrepareParamNode> prepareParamNodes = Lists.newArrayList();

    public ParseResultNode() {
        this.setResultType(ResultTypeEnum.SELECT);
    }
}
