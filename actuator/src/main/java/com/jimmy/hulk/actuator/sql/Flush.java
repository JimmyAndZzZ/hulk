package com.jimmy.hulk.actuator.sql;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.util.StrUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import com.jimmy.hulk.actuator.enums.FlushTypeEnum;
import com.jimmy.hulk.actuator.support.SQLBox;
import com.jimmy.hulk.common.other.FileReader;
import com.jimmy.hulk.actuator.support.ExecuteHolder;
import com.jimmy.hulk.common.constant.Constants;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.data.base.Data;
import com.jimmy.hulk.data.core.Wrapper;
import com.jimmy.hulk.data.transaction.Transaction;
import com.jimmy.hulk.parse.core.result.ExtraNode;
import com.jimmy.hulk.parse.core.result.ParseResultNode;
import lombok.extern.slf4j.Slf4j;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.springframework.util.Assert;

import java.io.InputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

@Slf4j
public class Flush extends SQL<Integer> {

    private final Select select;

    public Flush() {
        this.select = SQLBox.instance().get(Select.class);
    }

    @Override
    public Integer process(ParseResultNode parseResultNode) throws Exception {
        FileReader bigFileReader = null;
        ExtraNode extraNode = parseResultNode.getExtraNode();
        //目标操作数据类
        Data targetData = this.getTargetData(extraNode.getDsName(), extraNode.getIndex());
        //解析映射规则
        Mapper mapper = this.mapperXmlParser(extraNode.getMapper());
        try {
            //结果集
            List<Map<String, Object>> content = select.process(parseResultNode);

            if (CollUtil.isEmpty(content)) {
                log.error("查询结果为空");
                return 0;
            }
            //数据写入
            if (CollUtil.isNotEmpty(content)) {
                //打开事务
                Transaction.openTransaction();

                switch (mapper.getType()) {
                    case INSERT:
                        this.insert(mapper, content, targetData);
                        break;
                    case UPDATE:
                        //判断映射是否为空
                        if (CollUtil.isEmpty(mapper.getFields())) {
                            log.error("映射字段为空");
                            return 0;
                        }
                        this.update(mapper, content, targetData);
                        break;
                    case REPLACE:
                        //判断映射是否为空
                        if (CollUtil.isEmpty(mapper.getFields())) {
                            log.error("映射字段为空");
                            return 0;
                        }
                        this.replace(mapper, content, targetData);
                        break;
                }
            }

            Transaction.commit();
            return content.size();
        } catch (Exception e) {
            log.error("数据写入失败", e);
            Transaction.rollback();
            throw new HulkException("数据写入失败", ModuleEnum.ACTUATOR);
        } finally {
            if (bigFileReader != null) {
                bigFileReader.shutdown();
            }

            Transaction.close();
        }
    }

    /**
     * 更新
     *
     * @param mapper
     * @param content
     * @param targetData
     */
    private void replace(Mapper mapper, List<Map<String, Object>> content, Data targetData) {
        List<Field> fields = mapper.getFields();
        Expression condition = mapper.getCondition();
        //没有配置主键则直接改为insert
        if (fields.stream().filter(bean -> bean.getIsPrimary()).count() == 0) {
            this.insert(mapper, content, targetData);
        }

        for (Map<String, Object> map : content) {
            //验证不通过
            if (condition != null && !Convert.toBool(condition.execute(map), false)) {
                continue;
            }
            //条件构建
            Wrapper wrapper = Wrapper.build();
            //字段映射
            for (Field field : fields) {
                String name = field.getName();
                String alias = field.getAlias();

                map.put(alias, map.get(name));
                map.remove(name);
                //主键
                if (field.getIsPrimary()) {
                    Object o = map.get(alias);
                    if (o == null) {
                        throw new HulkException("主键为空", ModuleEnum.ACTUATOR);
                    }
                    wrapper.eq(alias, o);
                }
            }

            if (targetData.count(wrapper) > 0) {
                targetData.update(map, wrapper);
            } else {
                targetData.add(map);
            }
        }
    }

    /**
     * 更新
     *
     * @param mapper
     * @param content
     * @param targetData
     */
    private void update(Mapper mapper, List<Map<String, Object>> content, Data targetData) {
        List<Field> fields = mapper.getFields();
        Expression condition = mapper.getCondition();

        if (fields.stream().filter(bean -> bean.getIsPrimary()).count() == 0) {
            throw new HulkException("未标识主键字段", ModuleEnum.ACTUATOR);
        }

        for (Map<String, Object> map : content) {
            //验证不通过
            if (condition != null && !Convert.toBool(condition.execute(map), false)) {
                continue;
            }
            //条件构建
            Wrapper wrapper = Wrapper.build();
            //字段映射
            for (Field field : fields) {
                String name = field.getName();
                String alias = field.getAlias();

                map.put(alias, map.get(name));
                map.remove(name);
                //主键
                if (field.getIsPrimary()) {
                    Object o = map.get(alias);
                    if (o == null) {
                        throw new HulkException("主键为空", ModuleEnum.ACTUATOR);
                    }
                    wrapper.eq(alias, o);
                }
            }

            targetData.update(map, wrapper);
        }
    }

    /**
     * 新增
     *
     * @param mapper
     * @param content
     * @param targetData
     */
    private void insert(Mapper mapper, List<Map<String, Object>> content, Data targetData) {
        List<Field> fields = mapper.getFields();
        Expression condition = mapper.getCondition();
        List<Map<String, Object>> buffer = Lists.newArrayList();

        for (Map<String, Object> map : content) {
            //大于1000条写入
            if (buffer.size() >= 1000) {
                targetData.addBatch(buffer);
                buffer.clear();
            }
            //验证不通过
            if (condition != null && !Convert.toBool(condition.execute(map), false)) {
                continue;
            }
            //字段映射
            if (CollUtil.isNotEmpty(fields)) {
                for (Field field : fields) {
                    String name = field.getName();
                    String alias = field.getAlias();

                    map.put(alias, map.get(name));
                    map.remove(name);
                }
            }

            buffer.add(map);
        }
        //最后写入
        if (CollUtil.isNotEmpty(buffer)) {
            targetData.addBatch(buffer);
            buffer = null;
        }
    }

    /**
     * 解析xml
     *
     * @return
     */
    private Mapper mapperXmlParser(String path) {
        Mapper mapper = new Mapper();

        if (StrUtil.isEmpty(path) || !FileUtil.exist(path)) {
            return mapper;
        }

        try (InputStream inputStream = Flush.class.getClassLoader().getResourceAsStream(path)) {
            // 创建saxReader对象
            SAXReader reader = new SAXReader();
            // 通过read方法读取一个文件 转换成Document对象
            Document document = reader.read(inputStream);
            //获取根节点元素对象
            Element node = document.getRootElement();

            List<Element> nodes = node.elements(Constants.Actuator.XmlNode.FIELD_NODE);
            if (CollUtil.isNotEmpty(nodes)) {
                for (Element element : nodes) {
                    String source = element.attributeValue(Constants.Actuator.XmlNode.SOURCE_FIELD_ATTRIBUTE);
                    String target = element.attributeValue(Constants.Actuator.XmlNode.TARGET_FIELD_ATTRIBUTE);
                    Boolean isPrimary = Convert.toBool(element.attributeValue(Constants.Actuator.XmlNode.IS_PRIMARY_ATTRIBUTE), false);

                    if (StrUtil.isAllNotBlank(source, target)) {
                        Field field = new Field();
                        field.setName(source);
                        field.setAlias(target);
                        field.setIsPrimary(isPrimary);
                        mapper.getFields().add(field);
                    }
                }
            }

            String condition = node.attributeValue(Constants.Actuator.XmlNode.CONDITION_ATTRIBUTE);
            if (StrUtil.isNotBlank(condition)) {
                mapper.setCondition(AviatorEvaluator.compile(condition));
            }

            String type = node.attributeValue(Constants.Actuator.XmlNode.TYPE_ATTRIBUTE);
            if (StrUtil.isNotBlank(type)) {
                FlushTypeEnum flushTypeEnum = FlushTypeEnum.valueOf(type);
                if (flushTypeEnum == null) {
                    throw new HulkException("不支持该类型" + type, ModuleEnum.ACTUATOR);
                }

                mapper.setType(flushTypeEnum);
            }

            return mapper;
        } catch (Exception e) {
            log.error("xml解析失败", e);
            throw new HulkException("映射XML解析失败", ModuleEnum.ACTUATOR);
        }

    }

    /**
     * 获取目标数据类
     *
     * @param dsName
     * @return
     */
    private Data getTargetData(String dsName, String index) {
        Assert.isTrue(StrUtil.isNotBlank(index), "目标源为空");

        if (StrUtil.isEmpty(dsName)) {
            dsName = ExecuteHolder.getDatasourceName();
        }

        return partSupport.getData(ExecuteHolder.getUsername(), dsName, index, "ID", false);
    }

    @lombok.Data
    private class Mapper implements Serializable {

        private Expression condition;

        private FlushTypeEnum type = FlushTypeEnum.INSERT;

        private List<Field> fields = Lists.newArrayList();
    }

    @lombok.Data
    private class Field implements Serializable {

        private String name;

        private String alias;

        private Boolean isPrimary = false;
    }
}
