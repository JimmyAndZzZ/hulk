package com.jimmy.hulk.actuator.sql;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import com.jimmy.hulk.actuator.part.PartSupport;
import com.jimmy.hulk.actuator.base.Operate;
import com.jimmy.hulk.actuator.support.ExecuteHolder;
import com.jimmy.hulk.common.constant.Constants;
import com.jimmy.hulk.common.enums.ColumnTypeEnum;
import com.jimmy.hulk.common.enums.DatasourceEnum;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.data.config.DataSourceProperty;
import com.jimmy.hulk.parse.core.element.ColumnNode;
import com.jimmy.hulk.parse.core.element.TableNode;
import com.jimmy.hulk.parse.core.result.ParseResultNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
abstract class SQL<T> implements Operate {

    @Autowired
    protected PartSupport partSupport;

    @Override
    public T execute(ParseResultNode parseResultNode) {
        try {
            String sql = parseResultNode.getSql();
            //前置验证
            boolean verification = this.verification(parseResultNode);
            if (!verification) {
                throw new HulkException(sql + "该SQL不允许被执行", ModuleEnum.ACTUATOR);
            }
            //核心方法调用
            T process = this.process(parseResultNode);
            //后置回调
            this.after(parseResultNode);
            return process;
        } catch (HulkException e) {
            throw e;
        } catch (Exception e) {
            return this.error(parseResultNode, e);
        } finally {
            ExecuteHolder.clear();
            this.ultimate(parseResultNode);
        }
    }

    public boolean verification(ParseResultNode parseResultNode) {
        return true;
    }

    public abstract T process(ParseResultNode parseResultNode) throws Exception;

    public void after(ParseResultNode parseResultNode) {

    }

    public T error(ParseResultNode parseResultNode, Exception e) {
        if (!(e instanceof HulkException)) {
            log.error("SQL执行失败,{}", parseResultNode.getSql(), e);
        }

        throw new HulkException("执行失败", ModuleEnum.ACTUATOR);
    }

    public void ultimate(ParseResultNode parseResultNode) {

    }

    /**
     * 写入磁盘
     *
     * @param filePath
     */
    protected void writeToDisk(String filePath, List<Map<String, Object>> result) {
        //写入磁盘
        if (CollUtil.isNotEmpty(result)) {
            //写入数据
            for (int i = 0; i < result.size(); i++) {
                StringBuilder content = new StringBuilder();
                //加换行符
                if (i > 0) {
                    content.append(StrUtil.CRLF);
                }

                content.append(JSON.toJSONStringWithDateFormat(result.get(i), Constants.Actuator.DATE_FORMAT, SerializerFeature.WriteMapNullValue));
                this.appendContent(filePath, content.toString(), null);
            }
        }
    }

    /**
     * 写入文件
     *
     * @param file       文件路径
     * @param content    待写入的文件内容
     * @param bufferSize 单次写入缓冲区大小 默认4M 1024 * 1024 * 4
     */
    protected void appendContent(String file, String content, Integer bufferSize) {
        bufferSize = null == bufferSize ? 4194304 : bufferSize;
        ByteBuffer buf = ByteBuffer.allocate(bufferSize);
        try (FileChannel channel = new FileOutputStream(file, true).getChannel()) {
            buf.put(content.getBytes());
            buf.flip();   // 切换为读模式

            while (buf.hasRemaining()) {
                channel.write(buf);
            }
        } catch (Exception e) {
            log.error("文件写入失败", e);
            throw new HulkException("文件写入失败", ModuleEnum.ACTUATOR);
        }
    }

    /**
     * 获取赋值
     *
     * @param columnNode
     * @param data
     * @return
     */
    protected Object getValue(ColumnNode columnNode, Map<String, Object> data) {
        ColumnTypeEnum type = columnNode.getType();
        switch (type) {
            case CONSTANT:
                return columnNode.getConstant();
            case FUNCTION:
                Expression functionExp = this.getFunctionExp(columnNode);
                return functionExp.execute(data);
            case EXPRESSION:
                Expression compile = AviatorEvaluator.compile(columnNode.getExpression());
                return compile.execute(data);
            default:
                throw new HulkException("不支持该类型取值", ModuleEnum.ACTUATOR);
        }
    }

    /**
     * @param functionColumnNode
     * @return
     */
    protected Expression getFunctionExp(ColumnNode functionColumnNode) {
        String function = functionColumnNode.getFunction();
        List<ColumnNode> functionParam = functionColumnNode.getFunctionParam();

        StringBuilder stringBuilder = new StringBuilder(function).append("(");

        if (CollUtil.isNotEmpty(functionParam)) {
            for (int i = 0; i < functionParam.size(); i++) {
                if (i > 0) {
                    stringBuilder.append(",");
                }

                ColumnNode columnNode = functionParam.get(i);
                ColumnTypeEnum type = columnNode.getType();
                switch (type) {
                    case FIELD:
                        stringBuilder.append(columnNode.getTableNode().getAlias()).append(".").append(columnNode.getName());
                        break;
                    case CONSTANT:
                        stringBuilder.append("\"").append(columnNode.getConstant()).append("\"");
                        break;
                    default:
                        throw new HulkException("函数字段不支持该类型", ModuleEnum.ACTUATOR);
                }
            }
        }

        return AviatorEvaluator.compile(stringBuilder.append(")").toString());
    }


    /**
     * 是否可以直接通过SQL运行
     *
     * @param parseResultNode
     * @return
     */
    protected boolean isExecuteBySQL(ParseResultNode parseResultNode) {
        List<ColumnNode> columns = parseResultNode.getColumns();
        List<TableNode> tableNodes = parseResultNode.getTableNodes();
        //单一数据源则考虑是否用原生SQL方式执行
        Set<String> dsNames = tableNodes.stream().map(TableNode::getDsName).collect(Collectors.toSet());
        if (dsNames.size() > 1) {
            return false;
        }
        //数据源是否支持sql运行
        String dsName = dsNames.stream().findFirst().get();
        DataSourceProperty byName = partSupport.getDataSourceProperty(ExecuteHolder.getUsername(), dsName, true);
        if (!byName.getDs().equals(DatasourceEnum.MYSQL)) {
            return false;
        }
        //是否包含表分区
        for (TableNode tableNode : tableNodes) {
            if (partSupport.isConfigPartition(dsName, tableNode.getTableName())) {
                return false;
            }
        }
        //函数判断
        List<ColumnNode> collect = columns.stream().filter(bean -> bean.getType().equals(ColumnTypeEnum.FUNCTION)).collect(Collectors.toList());
        if (CollUtil.isNotEmpty(collect)) {
            for (ColumnNode columnNode : collect) {
                String function = columnNode.getFunction();
                if (AviatorEvaluator.containsFunction(function)) {
                    return false;
                }
            }
        }

        return true;
    }

}
