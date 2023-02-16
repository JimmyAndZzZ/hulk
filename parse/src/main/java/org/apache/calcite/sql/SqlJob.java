package org.apache.calcite.sql;

import cn.hutool.core.util.StrUtil;
import lombok.Getter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;

public class SqlJob extends SqlNode {

    @Getter
    private SqlNode name;

    @Getter
    private SqlNode cron;

    @Getter
    private SqlNode sql;

    /**
     * Creates a node.
     *
     * @param pos Parser position, must not be null.
     */
    public SqlJob(SqlParserPos pos, SqlNode name, SqlNode cron, SqlNode sql) {
        super(pos);
        this.name = name;
        this.cron = cron;
        this.sql = sql;
    }

    @Override
    public SqlNode clone(SqlParserPos pos) {
        return null;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("job");
        writer.print(StrUtil.CRLF);
        writer.keyword("name:" + name);
        writer.print(StrUtil.CRLF);
        writer.keyword("cron:" + cron);
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {

    }

    @Override
    public <R> R accept(SqlVisitor<R> visitor) {
        return null;
    }

    @Override
    public boolean equalsDeep(SqlNode node, Litmus litmus) {
        return false;
    }
}
