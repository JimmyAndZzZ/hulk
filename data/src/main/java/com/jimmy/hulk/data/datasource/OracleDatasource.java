package com.jimmy.hulk.data.datasource;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.util.ReUtil;
import cn.hutool.core.util.StrUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.jimmy.hulk.common.enums.DatasourceEnum;
import com.jimmy.hulk.data.actuator.Actuator;
import com.jimmy.hulk.data.actuator.OracleActuator;
import com.jimmy.hulk.data.core.Dump;
import com.jimmy.hulk.data.notify.ImportNotify;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceUtils;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.LineNumberReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import static com.jimmy.hulk.common.enums.DatasourceEnum.ORACLE;

@Slf4j
public class OracleDatasource extends BaseDatasource<javax.sql.DataSource> {

    private final static Map<String, HikariDataSource> DS_CACHE = Maps.newConcurrentMap();

    @Override
    public void close() throws IOException {
        String name = dataSourceProperty.getName();
        HikariDataSource dataSource = DS_CACHE.get(name);
        if (dataSource != null) {
            DS_CACHE.remove(name);
            dataSource.close();
        }
    }

    @Override
    public Actuator getActuator() {
        return new OracleActuator(this, dataSourceProperty);
    }

    @Override
    public javax.sql.DataSource getDataSource() {
        return this.getDataSource(null);
    }

    @Override
    public javax.sql.DataSource getDataSource(Long timeout) {
        String name = dataSourceProperty.getName();
        HikariDataSource dataSource = DS_CACHE.get(name);
        if (dataSource != null) {
            return dataSource;
        }

        dataSource = (HikariDataSource) this.getDataSourceWithoutCache(timeout);
        HikariDataSource put = DS_CACHE.putIfAbsent(name, dataSource);
        if (put != null) {
            dataSource.close();
            return put;
        }

        return dataSource;
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

        List<String> longSql = Lists.newArrayList();
        AtomicInteger i = new AtomicInteger(0);
        AtomicInteger count = new AtomicInteger(0);
        try {
            while ((line = lineReader.readLine()) != null) {
                if (StrUtil.isNotBlank(table)) {
                    line = StrUtil.replace(line, new StringBuilder("`").append(table).append("`").toString(), new StringBuilder("\"").append(table.toUpperCase()).append("\"").toString());
                    //别名替换
                    if (StrUtil.isNotEmpty(alias)) {
                        line = StrUtil.replace(line, table.toUpperCase(), alias.toUpperCase());
                    }
                }

                if (StrUtil.contains(line, "\\'")) {
                    line = StrUtil.replace(line, "\\'", "''");
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

                    if (StrUtil.endWith(trimmedLine, ";")) {
                        trimmedLine = trimmedLine.substring(0, trimmedLine.length() - 1);
                    }

                    if (trimmedLine.getBytes().length > 4000 || trimmedLine.length() > 4000) {
                        longSql.add(trimmedLine);
                    } else {
                        //时间处理
                        statement.execute(this.timeRegMatch(trimmedLine));
                    }

                    i.incrementAndGet();
                } else if (((!!trimmedLine.startsWith("INSERT INTO"))) && trimmedLine.endsWith(DEFAULT_DELIMITER)) {
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

            if (CollUtil.isNotEmpty(longSql)) {
                for (String s : longSql) {
                    //this.longSqlHandler(s, connection);
                }
            }
        } finally {
            DataSourceUtils.releaseConnection(conn, connection);
            lineReader.close();
            reader.close();
            statement.close();
        }
    }

    @Override
    public DataSource getDataSourceWithoutCache(Long timeout) {
        HikariConfig config = new HikariConfig();

        if (timeout != null) {
            config.setConnectionTimeout(timeout);
        }

        config.setJdbcUrl(dataSourceProperty.getUrl());
        config.setUsername(dataSourceProperty.getUsername());
        config.setPassword(dataSourceProperty.getPassword());
        config.setDriverClassName("oracle.jdbc.driver.OracleDriver");
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        config.setMaximumPoolSize(10);
        return new HikariDataSource(config);
    }

    @Override
    public DatasourceEnum type() {
        return ORACLE;
    }

    /**
     * 时间匹配
     *
     * @param line
     */
    private String timeRegMatch(String line) {
        String regex = "('\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}')";
        Pattern pattern = Pattern.compile(regex);
        List<String> allGroup0 = ReUtil.findAllGroup0(pattern, line);
        if (CollUtil.isNotEmpty(allGroup0)) {
            Set<String> set = Sets.newHashSet(allGroup0);
            for (String s : set) {
                line = StrUtil.replace(line, s, StrUtil.format("to_date({}, 'yyyy-MM-dd HH24:mi:ss')", s));
            }
        }

        return line;
    }
}
