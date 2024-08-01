package com.jimmy.hulk.parse.support;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import com.google.common.collect.Lists;
import com.jimmy.hulk.common.enums.AlterTypeEnum;
import com.jimmy.hulk.common.enums.FieldTypeEnum;
import com.jimmy.hulk.common.enums.IndexTypeEnum;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.parse.core.element.AlterNode;
import com.jimmy.hulk.parse.core.element.ColumnNode;
import com.jimmy.hulk.parse.core.element.IndexNode;
import com.jimmy.hulk.parse.core.element.TableNode;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.alter.AlterOperation;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;
import net.sf.jsqlparser.statement.drop.Drop;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class AlterParser {

    public static TableNode tableParse(String sql) {
        sql = StrUtil.trim(sql);

        try {
            Statement statement = CCJSqlParserUtil.parse(sql);
            if (StrUtil.startWithIgnoreCase(sql, "DROP TABLE")) {
                Drop drop = (Drop) statement;
                TableNode tableNode = new TableNode();
                tableNode.setTableName(StrUtil.removeAll(drop.getName().toString(), "`"));
                return tableNode;
            }

            if (StrUtil.startWithIgnoreCase(sql, "CREATE TABLE")) {
                CreateTable createTable = (CreateTable) statement;

                TableNode tableNode = new TableNode();
                tableNode.setTableName(StrUtil.removeAll(createTable.getTable().toString(), "`"));

                List<ColumnDefinition> columnDefinitions = createTable.getColumnDefinitions();
                if (CollUtil.isEmpty(columnDefinitions)) {
                    throw new HulkException("创建表字段为空", ModuleEnum.PARSE);
                }

                List<Index> indexes = createTable.getIndexes();
                if (CollUtil.isEmpty(indexes)) {
                    throw new HulkException("创建表主键为空", ModuleEnum.PARSE);
                }

                List<Index> primaryKey = indexes.stream().filter(index -> index.getType().equalsIgnoreCase("PRIMARY KEY")).collect(Collectors.toList());
                if (CollUtil.isEmpty(primaryKey)) {
                    throw new HulkException("创建表主键为空", ModuleEnum.PARSE);
                }

                List<String> primaryKeys = primaryKey.stream().findFirst().get().getColumns().stream().map(bean -> StrUtil.removeAll(bean.getColumnName(), "`")).collect(Collectors.toList());
                for (ColumnDefinition columnDefinition : columnDefinitions) {
                    String columnName = StrUtil.removeAll(columnDefinition.getColumnName(), "`");
                    ColDataType colDataType = columnDefinition.getColDataType();

                    String dataType = colDataType.getDataType();
                    List<String> argumentsStringList = colDataType.getArgumentsStringList();

                    FieldTypeEnum byCode = FieldTypeEnum.getByCode(dataType);
                    if (byCode == null) {
                        throw new HulkException(dataType + ":类型匹配失败", ModuleEnum.PARSE);
                    }

                    ColumnNode columnNode = new ColumnNode();
                    columnNode.setName(columnName);
                    columnNode.setFieldType(byCode);

                    if (CollUtil.isNotEmpty(argumentsStringList)) {
                        columnNode.setLength(argumentsStringList.stream().findFirst().get());
                    }

                    if (primaryKeys.contains(columnName)) {
                        columnNode.setIsPrimary(true);
                        columnNode.setIsAllowNull(false);
                    }

                    tableNode.getColumnNodes().add(columnNode);
                }

                return tableNode;
            }

            return null;
        } catch (HulkException hulkException) {
            throw hulkException;
        } catch (Exception e) {
            log.error("SQL解析失败,{}\n", sql, e);
            throw new HulkException("解析失败", ModuleEnum.PARSE);
        }
    }

    public static List<AlterNode> alterParse(String sql) {
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
                                alterNode.setTable(name);
                                alterNode.setAlterTypeEnum(AlterTypeEnum.ADD_COLUMN);
                                alterNode.setColumnNode(handlerColumn(alterExpression.getColDataTypeList().stream().findFirst().get()));
                                alters.add(alterNode);
                            }

                            IndexNode indexNode = handlerIndex(alterExpression);
                            if (indexNode != null) {
                                AlterNode alterNode = new AlterNode();
                                alterNode.setTable(name);
                                alterNode.setIndexNode(indexNode);
                                alterNode.setAlterTypeEnum(AlterTypeEnum.ADD_INDEX);
                                alters.add(alterNode);
                            }

                            break;
                        case MODIFY:
                            if (CollUtil.isNotEmpty(alterExpression.getColDataTypeList())) {
                                AlterNode alterNode = new AlterNode();
                                alterNode.setTable(name);
                                alterNode.setAlterTypeEnum(AlterTypeEnum.MODIFY_COLUMN);
                                alterNode.setColumnNode(handlerColumn(alterExpression.getColDataTypeList().stream().findFirst().get()));
                                alters.add(alterNode);
                            }

                            break;
                        case CHANGE:
                            if (CollUtil.isEmpty(alterExpression.getColDataTypeList())) {
                                ColumnNode oldColumnNode = new ColumnNode();
                                oldColumnNode.setName(alterExpression.getColumnOldName());

                                AlterNode alterNode = new AlterNode();
                                alterNode.setTable(name);
                                alterNode.setOldColumnNode(oldColumnNode);
                                alterNode.setAlterTypeEnum(AlterTypeEnum.CHANGE_COLUMN);
                                alterNode.setColumnNode(handlerColumn(alterExpression.getColDataTypeList().stream().findFirst().get()));
                                alters.add(alterNode);
                            }

                            break;
                        case DROP:
                            if (CollUtil.isEmpty(alterExpression.getColDataTypeList())) {
                                ColumnNode columnNode = new ColumnNode();
                                columnNode.setName(alterExpression.getColumnOldName());

                                AlterNode alterNode = new AlterNode();
                                alterNode.setTable(name);
                                alterNode.setAlterTypeEnum(AlterTypeEnum.DROP_COLUMN);
                                alterNode.setColumnNode(columnNode);
                                alters.add(alterNode);
                            }

                            IndexNode dropIndexNode = handlerIndex(alterExpression);
                            if (dropIndexNode != null) {
                                AlterNode alterNode = new AlterNode();
                                alterNode.setTable(name);
                                alterNode.setIndexNode(dropIndexNode);
                                alterNode.setAlterTypeEnum(AlterTypeEnum.DROP_INDEX);
                                alters.add(alterNode);
                            }

                            break;
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
    private static ColumnNode handlerColumn(AlterExpression.ColumnDataType columnDataType) {
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
    private static IndexNode handlerIndex(AlterExpression alterExpression) {
        String ukName = alterExpression.getUkName();
        Index index = alterExpression.getIndex();

        switch (alterExpression.getOperation()) {
            case ADD:
                if (StrUtil.isNotBlank(ukName)) {
                    IndexNode indexNode = new IndexNode();
                    indexNode.setName(ukName);
                    indexNode.setIndexType(IndexTypeEnum.UNIQUE);
                    indexNode.setColumns(alterExpression.getUkColumns());
                    return indexNode;
                }

                if (index != null) {
                    List<Index.ColumnParams> columns = index.getColumns();

                    IndexNode indexNode = new IndexNode();
                    indexNode.setName(index.getName());
                    indexNode.setIndexType(IndexTypeEnum.NORMAL);
                    indexNode.setColumns(columns.stream().map(Index.ColumnParams::getColumnName).collect(Collectors.toList()));
                    return indexNode;
                }

                break;
            case DROP:
                if (index != null) {
                    IndexNode indexNode = new IndexNode();
                    indexNode.setName(index.getName());
                    return indexNode;
                }

                break;
        }


        return null;
    }
}
