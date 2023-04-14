package com.jimmy.hulk.data.datasource;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.util.StrUtil;
import com.google.common.collect.Maps;
import com.jimmy.hulk.common.enums.DatasourceEnum;
import com.jimmy.hulk.data.actuator.Actuator;
import com.jimmy.hulk.data.actuator.SqlServerActuator;
import com.jimmy.hulk.data.annotation.DS;
import com.jimmy.hulk.data.condition.SqlServerCondition;
import com.jimmy.hulk.data.core.Dump;
import com.jimmy.hulk.data.notify.ImportNotify;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Conditional;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.LineNumberReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Conditional(SqlServerCondition.class)
@DS(type = DatasourceEnum.SQL_SERVER, condition = SqlServerCondition.class)
public class SqlServerDatasource extends BaseDatasource<javax.sql.DataSource> {

    private static ConcurrentMap<String, HikariDataSource> dsCache = Maps.newConcurrentMap();

    @Override
    public void close() throws IOException {
        String name = dataSourceProperty.getName();
        HikariDataSource hikariDataSource = dsCache.get(name);
        if (hikariDataSource != null) {
            dsCache.remove(name);
            hikariDataSource.close();
        }
    }

    @Override
    public Actuator getActuator() {
        return new SqlServerActuator(this, dataSourceProperty);
    }

    @Override
    public javax.sql.DataSource getDataSource() {
        return getDataSource(null);
    }

    @Override
    public javax.sql.DataSource getDataSource(Long timeout) {
        String name = dataSourceProperty.getName();
        HikariDataSource dataSource = dsCache.get(name);
        if (dataSource != null) {
            return dataSource;
        }

        dataSource = (HikariDataSource) this.getDataSourceWithoutCache(timeout);
        HikariDataSource put = dsCache.putIfAbsent(name, dataSource);
        if (put != null) {
            dataSource.close();
            return put;
        }

        return dataSource;
    }

    @Override
    public javax.sql.DataSource getDataSourceWithoutCache(Long timeout) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(StrUtil.replace(dataSourceProperty.getUrl(), "+", "%2B"));
        config.setUsername(dataSourceProperty.getUsername());
        config.setPassword(dataSourceProperty.getPassword());
        config.setDriverClassName("com.microsoft.sqlserver.jdbc.SQLServerDriver");

        if (timeout != null) {
            config.setConnectionTimeout(timeout);
        }

        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        config.setConnectionTestQuery("select 1 ");
        config.setMaximumPoolSize(dataSourceProperty.getMaxPoolSize());
        return new HikariDataSource(config);
    }

    @Override
    public boolean testConnect() {
        try {
            javax.sql.DataSource connection = this.getDataSource();
            JdbcTemplate jdbcTemplate = new JdbcTemplate();
            jdbcTemplate.setDataSource(connection);
            jdbcTemplate.queryForMap("select 1 from dual");
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public void dump(Dump dump) throws Exception {
        File file = dump.getFile();
        String alias = dump.getAlias();
        String table = dump.getTable();
        ImportNotify notify = dump.getNotify();

        javax.sql.DataSource connection = this.getDataSource(0L);
        Connection conn = connection.getConnection();
        conn.setAutoCommit(false);

        String line;
        StringBuffer command = null;
        Statement statement = conn.createStatement();
        BufferedReader reader = FileUtil.getReader(file, StandardCharsets.UTF_8);
        LineNumberReader lineReader = new LineNumberReader(reader);
        //计算总行数
        List<String> strings = FileUtil.readLines(file, StandardCharsets.UTF_8);
        long sum = strings.stream().filter(str -> str.startsWith("INSERT INTO")).count();

        AtomicInteger i = new AtomicInteger(0);
        AtomicInteger count = new AtomicInteger(0);
        try {
            while ((line = lineReader.readLine()) != null) {
                //别名替换
                if (StrUtil.isNotEmpty(alias)) {
                    line = StrUtil.replace(line, new StringBuilder("`").append(table).append("`").toString(), new StringBuilder("`").append(alias).append("`").toString());
                }

                if (StrUtil.contains(line, "`")) {
                    line = StrUtil.removeAll(line, "`");
                }

                if (command == null) {
                    command = new StringBuffer();
                }
                String trimmedLine = line.trim();
                if (trimmedLine.startsWith("--")) {
                    // Do nothing
                } else if (trimmedLine.length() < 1 || trimmedLine.startsWith("//")) {
                    // Do nothing
                } else if (trimmedLine.length() < 1 || trimmedLine.startsWith("--")) {
                    // Do nothing
                } else if (StrUtil.startWith(trimmedLine, "/*!") && StrUtil.endWith(trimmedLine, "*/;")) {
                    // Do nothing
                } else if (StrUtil.startWith(trimmedLine, "/*!") && StrUtil.endWith(trimmedLine, "*/ ;")) {
                    // Do nothing
                } else if (StrUtil.startWith(trimmedLine, "DELIMITER ;")) {
                    // Do nothing
                } else if (trimmedLine.startsWith("INSERT INTO")) {
                    int size = i.get();

                    if (size >= MAX_COUNT) {
                        log.info("已导入{}条", MAX_COUNT);
                        conn.commit();
                        i.set(0);

                        notify.callback(sum, count.longValue());
                    }

                    count.incrementAndGet();
                    statement.execute(trimmedLine);
                    i.incrementAndGet();
                } else if (((!trimmedLine.startsWith("INSERT INTO"))) && trimmedLine.endsWith(DEFAULT_DELIMITER)) {
                    command.append(line, 0, line.lastIndexOf(DEFAULT_DELIMITER));
                    command.append(" ");

                    statement.execute(command.toString());
                    conn.commit();
                    command = null;
                } else {
                    command.append(line);
                    command.append(" ");
                }
            }

            conn.commit();
            if (count.get() > 0) {
                notify.callback(sum, count.longValue());
            }
        } finally {
            DataSourceUtils.releaseConnection(conn, connection);
            lineReader.close();
            reader.close();
            statement.close();
        }
    }

    @Override
    public DatasourceEnum type() {
        return DatasourceEnum.SQL_SERVER;
    }
}
