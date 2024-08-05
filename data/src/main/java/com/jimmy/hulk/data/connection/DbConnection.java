package com.jimmy.hulk.data.connection;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.io.IoUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.db.DbUtil;
import cn.hutool.db.sql.SqlUtil;
import com.google.common.collect.Lists;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.data.other.ConnectionContext;
import com.jimmy.hulk.data.other.ExecuteBody;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.util.List;

@Slf4j
public class DbConnection implements com.jimmy.hulk.data.base.Connection<Connection, DataSource, ExecuteBody> {

    private DataSource dataSource;

    private Connection connection;

    private Object lock = new Object();

    private ConnectionContext context = new ConnectionContext();

    @Override
    public Connection getConnection() {
        try {
            if (connection == null) {
                synchronized (lock) {
                    if (connection == null) {
                        Connection connection = dataSource.getConnection();
                        connection.setAutoCommit(false);
                        this.connection = connection;
                    }
                }
            }

            return this.connection;
        } catch (Exception e) {
            throw new HulkException(e.getMessage(), ModuleEnum.DATA);
        }
    }

    @Override
    public void commit() {
        if (connection != null) {
            try {
                connection.commit();
            } catch (SQLException e) {
                throw new HulkException(e.getMessage(), ModuleEnum.DATA);
            }
        }
    }

    @Override
    public void rollback() {
        if (connection != null) {
            try {
                connection.rollback();
            } catch (SQLException e) {
                throw new HulkException(e.getMessage(), ModuleEnum.DATA);
            }
        }
    }

    @Override
    public void setSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void close() {
        if (connection != null) {
            IoUtil.close(connection);
        }
    }

    @Override
    public void setContext(ConnectionContext context) {
        this.context = context;
    }

    @Override
    public void batchExecute(List<ExecuteBody> bodies) throws Exception {
        List<PreparedStatement> preparedStatements = Lists.newArrayList();
        try {
            for (ExecuteBody body : bodies) {
                String sql = body.getSql();
                Object[] objects = body.getObjects();
                sql = StrUtil.trim(sql);
                String priKeyName = context.getString(body.getTableName());

                PreparedStatement stmt = StrUtil.isEmpty(priKeyName) ? this.getConnection().prepareStatement(sql) : this.getConnection().prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
                preparedStatements.add(stmt);

                for (int i = 0; i < objects.length; i++) {
                    this.setParam(stmt, i + 1, objects[i]);
                }

                stmt.addBatch();
                stmt.executeBatch();

                if (StrUtil.isNotBlank(priKeyName)) {
                    ResultSet generatedKeys = stmt.getGeneratedKeys();
                    while (generatedKeys.next()) {
                        long id = generatedKeys.getLong(1);
                        body.getDoc().put(priKeyName, id);
                    }
                }
            }
        } finally {
            if (CollUtil.isNotEmpty(preparedStatements)) {
                for (PreparedStatement stmt : preparedStatements) {
                    DbUtil.close(stmt);
                }
            }
        }
    }

    @Override
    public int execute(ExecuteBody body) throws Exception {
        String priKeyName = context.getString(body.getTableName());
        return StrUtil.isNotBlank(priKeyName) ? this.executeUpdateNeedReturnPriValue(body, priKeyName) : this.executeUpdate(body);
    }

    /**
     * @param body
     * @return
     * @throws Exception
     */
    private int executeUpdate(ExecuteBody body) throws Exception {
        String sql = body.getSql();
        Object[] objects = body.getObjects();
        //去除空格
        sql = StrUtil.trim(sql);
        try (PreparedStatement ps = this.getConnection().prepareStatement(sql)) {
            for (int i = 0; i < objects.length; i++) {
                this.setParam(ps, i + 1, objects[i]);
            }

            return ps.executeUpdate();
        }
    }

    /**
     * @param body
     * @return
     * @throws Exception
     */
    private int executeUpdateNeedReturnPriValue(ExecuteBody body, String priKeyName) throws Exception {
        String sql = body.getSql();
        Object[] objects = body.getObjects();
        //去除空格
        sql = StrUtil.trim(sql);
        try (PreparedStatement ps = this.getConnection().prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            for (int i = 0; i < objects.length; i++) {
                this.setParam(ps, i + 1, objects[i]);
            }

            int i = ps.executeUpdate();
            ResultSet generatedKeys = ps.getGeneratedKeys();
            while (generatedKeys.next()) {
                long id = generatedKeys.getLong(1);
                body.getDoc().put(priKeyName, id);
            }

            return i;
        }
    }

    /**
     * 写入参数
     *
     * @param ps
     * @param index
     * @param param
     * @throws SQLException
     */
    private void setParam(PreparedStatement ps, int index, Object param) throws SQLException {
        if (null == param) {
            ps.setNull(index, Types.VARCHAR);
        }
        // 日期特殊处理，默认按照时间戳传入，避免毫秒丢失
        if (param instanceof java.util.Date) {
            if (param instanceof java.sql.Date) {
                ps.setDate(index, (java.sql.Date) param);
            } else if (param instanceof java.sql.Time) {
                ps.setTime(index, (java.sql.Time) param);
            } else {
                ps.setTimestamp(index, SqlUtil.toSqlTimestamp((java.util.Date) param));
            }
            return;
        }
        // 针对大数字类型的特殊处理
        if (param instanceof Number) {
            if (param instanceof BigDecimal) {
                // BigDecimal的转换交给JDBC驱动处理
                ps.setBigDecimal(index, (BigDecimal) param);
                return;
            }
            if (param instanceof BigInteger) {
                // BigInteger转为Long
                ps.setBigDecimal(index, new BigDecimal((BigInteger) param));
                return;
            }
            // 忽略其它数字类型，按照默认类型传入
        }
        // 其它参数类型
        ps.setObject(index, param);
    }
}
