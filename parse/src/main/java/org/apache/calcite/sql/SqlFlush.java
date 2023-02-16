package org.apache.calcite.sql;

import cn.hutool.core.util.StrUtil;
import lombok.Getter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;

public class SqlFlush extends SqlNode {

    @Getter
    private SqlNode dsName;

    @Getter
    private SqlNode index;

    @Getter
    private SqlNode mapper;

    @Getter
    private SqlNode sql;

    public SqlFlush(SqlParserPos pos, SqlNode dsName, SqlNode index, SqlNode mapper, SqlNode sql) {
        super(pos);
        this.index = index;
        this.dsName = dsName;
        this.mapper = mapper;
        this.sql = sql;
    }

    @Override
    public SqlNode clone(SqlParserPos pos) {
        return null;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("flush");
        writer.print(StrUtil.CRLF);
        writer.keyword("dsName:" + dsName);
        writer.print(StrUtil.CRLF);
        writer.keyword("index:" + index);
        writer.print(StrUtil.CRLF);
        writer.keyword("mapper:" + mapper);
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
