package org.apache.calcite.sql;

import cn.hutool.core.util.StrUtil;
import lombok.Getter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;

public class SqlNative extends SqlNode {

    @Getter
    private SqlNode dsName;

    @Getter
    private SqlNode sql;

    @Getter
    private Boolean isExecute;

    public SqlNative(SqlParserPos pos, SqlNode dsName, SqlNode sql, boolean isExecute) {
        super(pos);
        this.dsName = dsName;
        this.sql = sql;
        this.isExecute = isExecute;
    }

    @Override
    public SqlNode clone(SqlParserPos pos) {
        return null;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("native");
        writer.print(StrUtil.CRLF);
        writer.keyword("dsName:" + dsName);
        writer.print(StrUtil.CRLF);
        writer.keyword("sql:" + sql);
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
