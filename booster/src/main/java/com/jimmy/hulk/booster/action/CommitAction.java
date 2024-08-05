package com.jimmy.hulk.booster.action;

import cn.hutool.core.collection.CollUtil;
import com.jimmy.hulk.actuator.sql.Delete;
import com.jimmy.hulk.actuator.sql.Insert;
import com.jimmy.hulk.actuator.sql.Update;
import com.jimmy.hulk.actuator.support.ExecuteHolder;
import com.jimmy.hulk.actuator.support.SQLBox;
import com.jimmy.hulk.booster.core.Session;
import com.jimmy.hulk.data.transaction.Transaction;
import com.jimmy.hulk.parse.core.result.ParseResultNode;
import com.jimmy.hulk.parse.enums.ResultTypeEnum;
import com.jimmy.hulk.parse.support.SQLParser;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class CommitAction extends BaseAction {

    private final Insert insert;

    private final Update update;

    private final Delete delete;

    public CommitAction() {
        insert = SQLBox.instance().get(Insert.class);
        update = SQLBox.instance().get(Update.class);
        delete = SQLBox.instance().get(Delete.class);
    }

    @Override
    public void action(String sql, Session session, int offset) throws Exception {
        List<String> waitTransactionSQL = session.getWaitTransactionSQL();
        if (CollUtil.isEmpty(waitTransactionSQL)) {
            session.writeOk();
            return;
        }
        //设置手动提交
        ExecuteHolder.manualCommit();

        try {
            Transaction.openTransaction();

            for (String dmlSQL : waitTransactionSQL) {
                ParseResultNode parse = SQLParser.parse(dmlSQL);

                ResultTypeEnum resultType = parse.getResultType();
                switch (resultType) {
                    case INSERT:
                        insert.process(parse);
                        break;
                    case UPDATE:
                        update.process(parse);
                        break;
                    case DELETE:
                        delete.process(parse);
                        break;
                }
            }

            Transaction.commit();
            session.writeOk();
        } catch (Exception e) {
            Transaction.rollback();
            throw e;
        } finally {
            waitTransactionSQL.clear();
            Transaction.close();
        }
    }
}
