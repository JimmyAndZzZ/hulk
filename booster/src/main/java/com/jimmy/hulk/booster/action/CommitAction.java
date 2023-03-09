package com.jimmy.hulk.booster.action;

import cn.hutool.core.collection.CollUtil;
import com.jimmy.hulk.actuator.sql.Delete;
import com.jimmy.hulk.actuator.sql.Insert;
import com.jimmy.hulk.actuator.sql.Update;
import com.jimmy.hulk.actuator.support.ExecuteHolder;
import com.jimmy.hulk.booster.core.Session;
import com.jimmy.hulk.data.transaction.Transaction;
import com.jimmy.hulk.parse.core.result.ParseResultNode;
import com.jimmy.hulk.parse.enums.ResultTypeEnum;
import com.jimmy.hulk.protocol.utils.parse.QueryParse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
public class CommitAction extends BaseAction {

    @Autowired
    private Insert insert;

    @Autowired
    private Update update;

    @Autowired
    private Delete delete;

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
                ParseResultNode parse = sqlParser.parse(dmlSQL);

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

    @Override
    public int type() {
        return QueryParse.COMMIT;
    }
}
