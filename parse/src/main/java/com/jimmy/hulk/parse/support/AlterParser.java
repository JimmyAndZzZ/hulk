package com.jimmy.hulk.parse.support;

import com.google.common.collect.Lists;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.parse.core.element.AlterNode;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.alter.AlterOperation;

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
}
