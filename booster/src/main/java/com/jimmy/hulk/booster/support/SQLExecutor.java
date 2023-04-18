package com.jimmy.hulk.booster.support;

import com.jimmy.hulk.actuator.other.IntObjectHashMap;
import com.jimmy.hulk.actuator.support.ExecuteHolder;
import com.jimmy.hulk.booster.base.Action;
import com.jimmy.hulk.booster.core.Session;
import com.jimmy.hulk.common.constant.ErrorCode;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.protocol.utils.parse.QueryParse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.Collection;

@Slf4j
@Component
public class SQLExecutor {

    private final IntObjectHashMap<Action> actions = new IntObjectHashMap();

    @Autowired
    private ApplicationContext applicationContext;

    public void prepareAction() {
        Collection<Action> values = applicationContext.getBeansOfType(Action.class).values();
        for (Action value : values) {
            actions.put(value.type(), value);
        }
    }

    public void execute(String sql, Session session) {
        try {
            ExecuteHolder.setDatasourceName(session.getSchema());
            ExecuteHolder.setUsername(session.getUser());
            //去除特殊符号
            sql = this.removeFirstAnnotation(sql);
            //获取SQL类型
            int rs = QueryParse.parse(sql);
            switch (rs & 0xff) {
                case QueryParse.DROP_TABLE:
                    actions.get(QueryParse.DROP_TABLE).action(sql, session, rs >>> 8);
                    break;
                case QueryParse.ALTER:
                    actions.get(QueryParse.ALTER).action(sql, session, rs >>> 8);
                    break;
                case QueryParse.CREATE_TABLE:
                    actions.get(QueryParse.CREATE_TABLE).action(sql, session, rs >>> 8);
                    break;
                case QueryParse.START:
                case QueryParse.BEGIN:
                case QueryParse.KILL_QUERY:
                case QueryParse.EXPLAIN:
                case QueryParse.CREATE_DATABASE:
                    session.writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unsupported command");
                    break;
                case QueryParse.COMMIT:
                    actions.get(QueryParse.COMMIT).action(sql, session, rs >>> 8);
                    break;
                case QueryParse.ROLLBACK:
                    actions.get(QueryParse.ROLLBACK).action(sql, session, rs >>> 8);
                    break;
                case QueryParse.SHOW:
                    actions.get(QueryParse.SHOW).action(sql, session, rs >>> 8);
                    break;
                case QueryParse.SELECT:
                    actions.get(QueryParse.SELECT).action(sql, session, rs >>> 8);
                    break;
                case QueryParse.INSERT:
                    actions.get(QueryParse.INSERT).action(sql, session, rs >>> 8);
                    break;
                case QueryParse.UPDATE:
                    actions.get(QueryParse.UPDATE).action(sql, session, rs >>> 8);
                    break;
                case QueryParse.DELETE:
                    actions.get(QueryParse.DELETE).action(sql, session, rs >>> 8);
                    break;
                case QueryParse.JOB:
                    actions.get(QueryParse.JOB).action(sql, session, rs >> 8);
                    break;
                case QueryParse.NATIVE:
                    actions.get(QueryParse.NATIVE).action(sql, session, rs >> 8);
                    break;
                case QueryParse.FLUSH:
                    actions.get(QueryParse.FLUSH).action(sql, session, rs >> 8);
                    break;
                case QueryParse.SAVEPOINT:
                    session.writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unsupported statement");
                    break;
                case QueryParse.KILL:
                    actions.get(QueryParse.KILL).action(sql, session, rs >> 8);
                    break;
                case QueryParse.USE:
                    actions.get(QueryParse.USE).action(sql, session, rs >> 8);
                    break;
                case QueryParse.CACHE:
                    actions.get(QueryParse.CACHE).action(sql, session, rs >> 8);
                    break;
                default:
                    log.error("当前sql无法处理:{}", sql);
                    throw new HulkException("无法处理sql", ModuleEnum.BOOSTER);
            }
        } catch (HulkException hulkException) {
            throw hulkException;
        } catch (Exception e) {
            log.error("SQL执行失败,{}", sql, e);
            throw new HulkException("sql执行失败", ModuleEnum.BOOSTER);
        } finally {
            ExecuteHolder.removeDatasourceName();
            ExecuteHolder.removeUsername();
            ExecuteHolder.clear();
        }
    }

    private String removeFirstAnnotation(String sql) {
        sql = sql.trim();
        if (sql.startsWith("/*")) {
            int index = sql.indexOf("*/") + 2;
            return sql.substring(index);
        } else {
            return sql;
        }
    }
}
