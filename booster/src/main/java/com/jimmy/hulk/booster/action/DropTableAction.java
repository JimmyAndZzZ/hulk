package com.jimmy.hulk.booster.action;

import com.jimmy.hulk.actuator.part.PartSupport;
import com.jimmy.hulk.actuator.support.ExecuteHolder;
import com.jimmy.hulk.booster.core.Session;
import com.jimmy.hulk.common.enums.DatasourceEnum;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.data.actuator.Actuator;
import com.jimmy.hulk.parse.core.element.TableNode;
import com.jimmy.hulk.parse.support.AlterParser;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DropTableAction extends BaseAction {

    private final PartSupport partSupport;

    public DropTableAction() {
        this.partSupport = PartSupport.instance();
    }

    @Override
    public void action(String sql, Session session, int offset) throws Exception {
        Actuator actuator = partSupport.getActuator(ExecuteHolder.getUsername(), ExecuteHolder.getDatasourceName(), false);
        //mysql 直接执行
        if (DatasourceEnum.MYSQL.equals(actuator.getDataSourceProperty().getDs())) {
            actuator.execute(sql);
            return;
        }

        TableNode tableNode = AlterParser.tableParse(sql);
        if (tableNode == null) {
            throw new HulkException("创建表失败", ModuleEnum.BOOSTER);
        }

        actuator.dropTable(tableNode.getTableName());
    }
}
