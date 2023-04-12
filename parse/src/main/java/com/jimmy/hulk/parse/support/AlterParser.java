package com.jimmy.hulk.parse.support;

import cn.hutool.core.collection.CollUtil;
import com.google.common.collect.Lists;
import com.jimmy.hulk.common.enums.AlterTypeEnum;
import com.jimmy.hulk.common.enums.FieldTypeEnum;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.parse.core.element.AlterNode;
import com.jimmy.hulk.parse.core.element.ColumnNode;
import com.jimmy.hulk.parse.core.element.IndexNode;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.alter.AlterOperation;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.Index;

import java.util.List;

@Slf4j
public class AlterParser {

    private List<AlterNode> parse(String sql) {
        List<AlterNode> alters = Lists.newArrayList();

        try {
            Statement statement = CCJSqlParserUtil.parse(sql);

            if (statement instanceof Alter) {
                Alter alter = (Alter) statement;
                Table table = alter.getTable();
                String name = table.getName();
                List<AlterExpression> alterExpressions = alter.getAlterExpressions();

                for (AlterExpression alterExpression : alterExpressions) {
                    AlterOperation operation = alterExpression.getOperation();
                    switch (operation) {
                        case ADD:
                            if (CollUtil.isNotEmpty(alterExpression.getColDataTypeList())) {
                                AlterNode alterNode = new AlterNode();
                                alterNode.setAlterTypeEnum(AlterTypeEnum.ADD_COLUMN);
                                alterNode.setColumnNode(this.handlerColumn(alterExpression.getColDataTypeList().stream().findFirst().get()));
                                alters.add(alterNode);
                            }

                            if (alterExpression.getIndex() != null) {
                                Index index = alterExpression.getIndex();

                                List<Index.ColumnParams> columns = index.getColumns();

                            }

                    }
                }
            }

            return alters;
        } catch (HulkException hulkException) {
            throw hulkException;
        } catch (Exception e) {
            log.error("SQL解析失败,{}\n", sql, e);
            throw new HulkException("解析失败", ModuleEnum.PARSE);
        }
    }

    /**
     * 处理字段类型
     *
     * @param columnDataType
     * @return
     */
    private ColumnNode handlerColumn(AlterExpression.ColumnDataType columnDataType) {
        String columnName = columnDataType.getColumnName();
        ColDataType colDataType = columnDataType.getColDataType();
        List<String> argumentsStringList = colDataType.getArgumentsStringList();

        String dataType = colDataType.getDataType();

        FieldTypeEnum byType = FieldTypeEnum.getByCode(dataType);
        if (byType == null) {
            throw new HulkException("类型映射失败" + dataType, ModuleEnum.PARSE);
        }

        ColumnNode column = new ColumnNode();
        column.setName(columnName);
        column.setFieldType(byType);
        if (CollUtil.isNotEmpty(argumentsStringList)) {
            column.setLength(argumentsStringList.size() == 1 ? argumentsStringList.stream().findFirst().get() : CollUtil.join(argumentsStringList, ","));
        }

        return column;
    }

    /**
     * 索引处理
     *
     * @param alterExpression
     * @return
     */
    private IndexNode handlerIndex(AlterExpression alterExpression) {

    }

    public static void main(String[] args) {
        String addC = "ALTER TABLE vendors\n" +
                "ADD COLUMN phone VARCHAR(15) AFTER name;";

        String addI = "ALTER TABLE `datafeed_dss`.`dss_b_code` \n" +
                "ADD UNIQUE INDEX `test_index`(`ID`, `delflag`) USING BTREE;";

        List<AlterNode> parse = new AlterParser().parse(addI);
    }
}
