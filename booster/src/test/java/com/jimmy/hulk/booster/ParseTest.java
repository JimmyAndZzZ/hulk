package com.jimmy.hulk.booster;

import cn.hutool.core.util.IdUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jimmy.hulk.actuator.memory.MemoryPool;
import com.jimmy.hulk.common.enums.DatasourceEnum;
import com.jimmy.hulk.config.support.SystemVariableContext;
import com.jimmy.hulk.data.base.Data;
import com.jimmy.hulk.data.config.DataSourceProperty;
import com.jimmy.hulk.data.support.SessionFactory;
import com.jimmy.hulk.parse.core.result.ParseResultNode;
import com.jimmy.hulk.parse.support.SQLParser;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;
import java.util.Map;


@SpringBootTest(classes = App.class)
@RunWith(SpringRunner.class)
@Slf4j
public class ParseTest {

    @Autowired
    private SQLParser parser;

    @Autowired
    private MemoryPool memoryPool;

    @Autowired
    private SessionFactory sessionFactory;

    @Autowired
    private SystemVariableContext systemVariableContext;

    @Test
    public void excelWrite() {
        DataSourceProperty dataSourceProperty = new DataSourceProperty();
        dataSourceProperty.setDs(DatasourceEnum.EXCEL);
        dataSourceProperty.setUrl("/opt/sumscope/EXCEL生成结果" + System.currentTimeMillis() + ".xlsx");

        Data data = sessionFactory.registeredData(dataSourceProperty, null, null, false);

        List<Map<String, Object>> list = Lists.newArrayList();
        for (int i = 0; i < 20; i++) {
            Map<String, Object> d = Maps.newHashMap();
            d.put("id", i);
            d.put("value", IdUtil.simpleUUID());
            list.add(d);
        }

        data.addBatch(list);
    }

    @Test
    public void ddd() {
        String sql = "SELECT\n" +
                "\tt1.ID,t2.inputer,t3.new_c1_new, t3.value_new ,1+2,count(1) as cs,idGenerator() \n" +
                "FROM\n" +
                "\tgjk_eform.test00 AS t1\n" +
                "\tINNER JOIN test_canal.test00 AS t2  ON t1.ID = t2.ID \n" +
                "\tINNER JOIN test_canal.test1 AS t3 on (t3.id_new = t1.ID or t3.value_new=t1.ID)\n" +
                "order by t3.name_new ,t3.new_c1_new desc";

        System.out.println(sql);

        String sss = "flush to 'test_excel' index 'test_excel' MAPPER '' as '" + sql + "'";
        System.out.println(sss);
    }

    @Test
    public void exe() {
        String sql = "SELECT\n" +
                "\tt1.ID,t2.inputer,t3.new_c1_new, t3.value_new \n" +
                "FROM\n" +
                "\tgjk_eform.test00 AS t1\n" +
                "\tleft JOIN  test_canal.test00 AS t2 ON t1.ID = t2.ID and t1.ID='1111'\n" +
                "\tinner JOIN test_canal.test1 AS t3 ON ( t3.id_new = t1.ID or t3.value_new = t1.ID ) \n" +
                "\tand ( t3.new_c1_new = '5' or t3.new_c1_new = '11' ) \n" +
                "ORDER BY\n" +
                "\tt1.ID";

        log.info("sql:{}", sql);
        ParseResultNode parse = parser.parse(sql);
        System.out.println(parse);
    }
}
