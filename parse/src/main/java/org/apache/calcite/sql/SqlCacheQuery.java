package org.apache.calcite.sql;

import lombok.Getter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;

public class SqlCacheQuery extends SqlNode {

    @Getter
    private SqlNode sql;

    @Getter
    private SqlNode dsName;

    @Getter
    private SqlNode expireTime;

    public SqlCacheQuery(SqlParserPos pos, SqlNode expireTime, SqlNode dsName, SqlNode sql) {
        super(pos);
        this.expireTime = expireTime;
        this.sql = sql;
        this.dsName = dsName;
    }

    @Override
    public SqlNode clone(SqlParserPos pos) {
        return null;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {

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

