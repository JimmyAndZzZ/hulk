package com.jimmy.hulk.booster;

import cn.hutool.core.date.DateUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jimmy.hulk.common.enums.DatasourceEnum;
import com.jimmy.hulk.data.base.Data;
import com.jimmy.hulk.data.config.DataSourceProperty;
import com.jimmy.hulk.data.core.Page;
import com.jimmy.hulk.data.core.PageResult;
import com.jimmy.hulk.data.core.Wrapper;
import com.jimmy.hulk.data.support.DataSourceFactory;
import com.jimmy.hulk.data.support.SessionFactory;
import com.jimmy.hulk.data.transaction.Transaction;
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
public class DataTest {

    @Autowired
    private SessionFactory sessionFactory;

    @Autowired
    private DataSourceFactory dataSourceFactory;

    @Test
    public void queryById() {
        DataSourceProperty property = new DataSourceProperty();
        property.setDs(DatasourceEnum.MYSQL);
        property.setUrl("jdbc:mysql://192.168.5.215:3306/test_canal?useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&useSSL=true&serverTimezone=GMT%2B8");
        property.setUsername("dev");
        property.setSchema("test_canal");
        property.setPassword("123456");

        Data data = sessionFactory.registeredData(property, "example_par_1", "id", false);
        Map<String, Object> map = data.queryById(1);
        System.out.println(map);
    }

    @Test
    public void query() {
        DataSourceProperty property = new DataSourceProperty();
        property.setDs(DatasourceEnum.MYSQL);
        property.setUrl("jdbc:mysql://192.168.5.215:3306/test_canal?useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&useSSL=true&serverTimezone=GMT%2B8");
        property.setUsername("dev");
        property.setSchema("test_canal");
        property.setPassword("123456");

        Data data = sessionFactory.registeredData(property, "example_par_1", "id", false);

        Wrapper wrapper = Wrapper.build();
        wrapper.eq("id", 3);
        wrapper.or();
        wrapper.eq("trace_id", 44);
        List<Map<String, Object>> maps = data.queryList(wrapper);
        System.out.println(maps);
    }

    @Test
    public void query2() {
        DataSourceProperty property = new DataSourceProperty();
        property.setDs(DatasourceEnum.MYSQL);
        property.setUrl("jdbc:mysql://192.168.5.215:3306/test_canal?useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&useSSL=true&serverTimezone=GMT%2B8");
        property.setUsername("dev");
        property.setSchema("test_canal");
        property.setPassword("123456");

        Data data = sessionFactory.registeredData(property, "bond_fundamental_info", "ID", false);

        Wrapper wrapper = Wrapper.build();
        wrapper.ge("create_date", DateUtil.parse("2022-12-30 13:16:02", "yyyy-MM-dd HH:mm:ss"));
        wrapper.or();
        wrapper.eq("ID", "zzzc7zkc03isa2o");
        PageResult<Map<String, Object>> mapPageResult = data.queryPage(wrapper, new Page(0, 10));
        System.out.println(mapPageResult);
    }

    @Test
    public void clickHouseAdd() {
        DataSourceProperty property = new DataSourceProperty();
        property.setDs(DatasourceEnum.CLICK_HOUSE);
        property.setUrl("jdbc:clickhouse://192.168.1.241:23100/ss_kv");
        property.setUsername("default");
        property.setSchema("ss_kv");
        property.setClusterName("perftest_3shards_1replicas");

        Data data = sessionFactory.registeredData(property, "ss_doc_analysis_kv", "ID", false);

        Map<String, Object> param = Maps.newHashMap();
        param.put("modal_id", 11111111);
        param.put("data_set_id", 0);
        param.put("field_id", 222223323);
        param.put("modal_field_id", 0);
        param.put("file_id", 123123123);
        param.put("value", "[{\"title\":\"营业'外收入表0\"}]");
        param.put("format_value", "[{\"title\":\"营业'外收入表0\"}]");
        data.add(param);
    }

    @Test
    public void clickHouseAdd1() {
        DataSourceProperty property = new DataSourceProperty();
        property.setDs(DatasourceEnum.CLICK_HOUSE);
        property.setUrl("jdbc:clickhouse://192.168.1.241:23100/ss_kv");
        property.setUsername("default");
        property.setSchema("ss_kv");
        property.setClusterName("perftest_3shards_1replicas");

        Data data = sessionFactory.registeredData(property, "ss_doc_analysis_kv", "ID", false);

        Map<String, Object> param = Maps.newHashMap();
        param.put("modal_id", 4444444555L);
        param.put("data_set_id", 0);
        param.put("field_id", 7264515278L);
        param.put("modal_field_id", 0);
        param.put("file_id", 62251452672387282L);
        param.put("value", "[{\"title\":\"营业\\\\外收入表0\"}]");
        param.put("format_value", "[{\"title\":\"营业\\\\外收入表0\"}]");
        data.add(param);
    }

    @Test
    public void clickHouseQuery() {
        DataSourceProperty property = new DataSourceProperty();
        property.setDs(DatasourceEnum.CLICK_HOUSE);
        property.setUrl("jdbc:clickhouse://192.168.1.241:23100/ss_kv");
        property.setUsername("default");
        property.setSchema("ss_kv");
        property.setClusterName("perftest_3shards_1replicas");

        Data data = sessionFactory.registeredData(property, "ss_doc_analysis_kv_view", "ID", false);

        Wrapper wrapper = Wrapper.build();
        wrapper.merge(Wrapper.build().eq("file_id", 123123123L).and().eq("field_id", 222223323L));
        wrapper.merge(Wrapper.build().eq("file_id", 62251452672387282L).and().eq("field_id", 7264515278L).or());
        List<Map<String, Object>> maps = data.queryList(wrapper);
        System.out.println(maps);
    }

    @Test
    public void clickHouseDelete() {
        DataSourceProperty property = new DataSourceProperty();
        property.setDs(DatasourceEnum.CLICK_HOUSE);
        property.setUrl("jdbc:clickhouse://192.168.1.241:23100/ss_kv");
        property.setUsername("default");
        property.setSchema("ss_kv");
        property.setClusterName("perftest_3shards_1replicas");

        Data data = sessionFactory.registeredData(property, "ss_doc_analysis_kv", "ID", false);

        Wrapper wrapper = Wrapper.build();
        wrapper.merge(Wrapper.build().eq("file_id", 123123123L).and().eq("field_id", 222223323L));
        wrapper.merge(Wrapper.build().eq("file_id", 62251452672387282L).and().eq("field_id", 7264515278L).or());
        data.delete(wrapper);
    }


    @Test
    public void add() {
        DataSourceProperty property = new DataSourceProperty();
        property.setDs(DatasourceEnum.MYSQL);
        property.setUrl("jdbc:mysql://192.168.5.215:3306/test_canal?useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&useSSL=true&serverTimezone=GMT%2B8");
        property.setUsername("dev");
        property.setSchema("test_canal");
        property.setPassword("123456");

        Data data = sessionFactory.registeredData(property, "example_par_1", "id", false);
        Map<String, Object> param = Maps.newHashMap();
        param.put("name", "1");
        param.put("trace_id", 22);
        data.add(param);
    }

    @Test
    public void update() {
        DataSourceProperty property = new DataSourceProperty();
        property.setDs(DatasourceEnum.MYSQL);
        property.setUrl("jdbc:mysql://192.168.5.215:3306/test_canal?useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&useSSL=true&serverTimezone=GMT%2B8");
        property.setUsername("dev");
        property.setSchema("test_canal");
        property.setPassword("123456");

        Data data = sessionFactory.registeredData(property, "example_par_1", "id", false);
        Map<String, Object> param = Maps.newHashMap();
        param.put("name", null);
        param.put("trace_id", 44);
        data.updateById(param, 1);
    }

    @Test
    public void delete() {
        DataSourceProperty property = new DataSourceProperty();
        property.setDs(DatasourceEnum.MYSQL);
        property.setUrl("jdbc:mysql://192.168.5.215:3306/test_canal?useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&useSSL=true&serverTimezone=GMT%2B8");
        property.setUsername("dev");
        property.setSchema("test_canal");
        property.setPassword("123456");

        Data data = sessionFactory.registeredData(property, "example_par_1", "id", false);
        data.delete(2);
    }

    @Test
    public void transaction() {
        DataSourceProperty property = new DataSourceProperty();
        property.setDs(DatasourceEnum.MYSQL);
        property.setUrl("jdbc:mysql://192.168.5.215:3306/test_canal?useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&useSSL=true&serverTimezone=GMT%2B8");
        property.setUsername("dev");
        property.setSchema("test_canal");
        property.setPassword("123456");

        Data data1 = sessionFactory.registeredData(property, "example_par_1", "id", false);
        Data data2 = sessionFactory.registeredData(property, "example_par_2", "id", false);

        try {
            Transaction.openTransaction();

            Map<String, Object> param = Maps.newHashMap();
            param.put("name", "12");
            param.put("trace_id", 222);
            param.put("id", 42);

            data1.add(param);
            data2.add(param);
            Transaction.commit();
        } catch (Exception e) {
            e.printStackTrace();
            Transaction.rollback();
        } finally {
            Transaction.close();
        }
    }

    @Test
    public void transaction2() {
        DataSourceProperty property1 = new DataSourceProperty();
        property1.setDs(DatasourceEnum.MYSQL);
        property1.setUrl("jdbc:mysql://localhost:3306/test_canal?useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&useSSL=true&serverTimezone=GMT%2B8");
        property1.setUsername("test");
        property1.setSchema("test_canal");
        property1.setPassword("123456");

        DataSourceProperty property2 = new DataSourceProperty();
        property2.setDs(DatasourceEnum.MYSQL);
        property2.setUrl("jdbc:mysql://192.168.5.215:3306/test_report?useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&useSSL=true&serverTimezone=GMT%2B8");
        property2.setUsername("dev");
        property2.setSchema("test_report");
        property2.setPassword("123456");

        Data data1 = sessionFactory.registeredData(property1, "example_par_1", "id", false);
        Data data2 = sessionFactory.registeredData(property2, "example_par_2", "id", true);

        try {
            Transaction.openTransaction();

            Map<String, Object> param1 = Maps.newHashMap();
            param1.put("name", "11");
            param1.put("trace_id", 221);

            data1.add(param1);
            System.out.println(param1);

            Map<String, Object> param2 = Maps.newHashMap();
            param2.put("name", "22");
            param2.put("trace_id", 333);

            data2.add(param2);
            System.out.println(param2);

            Transaction.commit();
        } catch (Exception e) {
            e.printStackTrace();
            Transaction.rollback();
        } finally {
            Transaction.close();
        }
    }

    @Test
    public void transaction3() {
        DataSourceProperty property1 = new DataSourceProperty();
        property1.setDs(DatasourceEnum.MYSQL);
        property1.setUrl("jdbc:mysql://localhost:3306/test_canal?useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&useSSL=true&serverTimezone=GMT%2B8");
        property1.setUsername("test");
        property1.setSchema("test_canal");
        property1.setPassword("123456");

        DataSourceProperty property2 = new DataSourceProperty();
        property2.setDs(DatasourceEnum.MYSQL);
        property2.setUrl("jdbc:mysql://192.168.5.215:3306/test_report?useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&useSSL=true&serverTimezone=GMT%2B8");
        property2.setUsername("dev");
        property2.setSchema("test_report");
        property2.setPassword("123456");

        Data data1 = sessionFactory.registeredData(property1, "example_par_1", "id", true);
        Data data2 = sessionFactory.registeredData(property2, "example_par_2", "id", true);

        try {
            Transaction.openTransaction();

            Map<String, Object> param1 = Maps.newHashMap();
            param1.put("name", "11");
            param1.put("trace_id", 221);

            data1.add(param1);
            System.out.println(param1);

            if (1 == 1) {
                throw new RuntimeException("!23");
            }

            Map<String, Object> param2 = Maps.newHashMap();
            param2.put("name", "22");
            param2.put("trace_id", 333);

            data2.add(param2);
            System.out.println(param2);

            Transaction.commit();
        } catch (Exception e) {
            e.printStackTrace();
            Transaction.rollback();
        } finally {
            Transaction.close();
        }
    }

    @Test
    public void transaction4() {
        DataSourceProperty property1 = new DataSourceProperty();
        property1.setDs(DatasourceEnum.MYSQL);
        property1.setUrl("jdbc:mysql://192.168.5.215:3306/test_canal?useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&useSSL=true&serverTimezone=GMT%2B8");
        property1.setUsername("dev");
        property1.setSchema("test_canal");
        property1.setPassword("123456");

        DataSourceProperty property2 = new DataSourceProperty();
        property2.setDs(DatasourceEnum.MYSQL);
        property2.setUrl("jdbc:mysql://192.168.5.215:3306/test_report?useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&useSSL=true&serverTimezone=GMT%2B8");
        property2.setUsername("dev");
        property2.setSchema("test_report");
        property2.setPassword("123456");

        Data data1 = sessionFactory.registeredData(property1, "example_par_1", "id", true);
        Data data2 = sessionFactory.registeredData(property2, "example_par_2", "id", true);

        try {
            Transaction.openTransaction();

            Map<String, Object> param1 = Maps.newHashMap();
            param1.put("name", "11");
            param1.put("trace_id", 221);

            data1.add(param1);
            System.out.println(param1);

            if (1 == 1) {
                throw new RuntimeException("!23");
            }

            Map<String, Object> param2 = Maps.newHashMap();
            param2.put("name", "22");
            param2.put("trace_id", 333);

            data2.add(param2);
            System.out.println(param2);

            Transaction.commit();
        } catch (Exception e) {
            e.printStackTrace();
            Transaction.rollback();
        } finally {
            Transaction.close();
        }
    }

    @Test
    public void transaction5() {
        DataSourceProperty property1 = new DataSourceProperty();
        property1.setDs(DatasourceEnum.MYSQL);
        property1.setUrl("jdbc:mysql://192.168.5.215:3306/test_canal?useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&useSSL=true&serverTimezone=GMT%2B8");
        property1.setUsername("dev");
        property1.setSchema("test_canal");
        property1.setPassword("123456");

        Data data1 = sessionFactory.registeredData(property1, "example_par_1", "id", true);
        try {
            Transaction.openTransaction();

            Map<String, Object> param1 = Maps.newHashMap();
            param1.put("name", "11");
            param1.put("trace_id", 11);

            Map<String, Object> param2 = Maps.newHashMap();
            param2.put("name", "22");
            param2.put("trace_id", 22);

            List<Map<String, Object>> batch = Lists.newArrayList();
            batch.add(param1);
            batch.add(param2);
            data1.addBatch(batch);

            System.out.println(batch);
            Transaction.commit();
        } catch (Exception e) {
            e.printStackTrace();
            Transaction.rollback();
        } finally {
            Transaction.close();
        }
    }

    @Test
    public void excel() {
        DataSourceProperty property1 = new DataSourceProperty();
        property1.setDs(DatasourceEnum.EXCEL);
        property1.setUrl("/tmp");

        Data data1 = sessionFactory.registeredData(property1, "example_par_1.xlsx", "id", false);

        Map<String, Object> param1 = Maps.newHashMap();
        param1.put("name", "11");
        param1.put("trace_id", 221);
        data1.add(param1);
    }

    @Test
    public void excelBatch() {
        try {
            Transaction.openTransaction();
            DataSourceProperty property1 = new DataSourceProperty();
            property1.setDs(DatasourceEnum.EXCEL);
            property1.setUrl("/tmp");

            Data data1 = sessionFactory.registeredData(property1, "example_par_3.xlsx", "id", false);

            Map<String, Object> param1 = Maps.newHashMap();
            param1.put("name", "44");
            param1.put("trace_id", 55555);
            data1.add(param1);

            Map<String, Object> param2 = Maps.newHashMap();
            param2.put("name", "44");
            param2.put("trace_id", 33333);
            data1.add(param2);
            Transaction.commit();
        } catch (Exception e) {
            e.printStackTrace();
            Transaction.rollback();
        } finally {
            Transaction.close();
        }
    }

    @Test
    public void excelBatch2() {
        try {
            Transaction.openTransaction();
            DataSourceProperty property1 = new DataSourceProperty();
            property1.setDs(DatasourceEnum.EXCEL);
            property1.setUrl("/tmp");

            Data data1 = sessionFactory.registeredData(property1, "example_par_4.xlsx", "id", false);
            Data data2 = sessionFactory.registeredData(property1, "example_par_5.xlsx", "id", false);

            Map<String, Object> param1 = Maps.newHashMap();
            param1.put("name4", "44");
            param1.put("trace_id4", 444);
            data1.add(param1);

            Map<String, Object> param2 = Maps.newHashMap();
            param2.put("name5", "55");
            param2.put("trace_id5", 555);
            data2.add(param2);
            Transaction.commit();
        } catch (Exception e) {
            e.printStackTrace();
            Transaction.rollback();
        } finally {
            Transaction.close();
        }
    }
}
