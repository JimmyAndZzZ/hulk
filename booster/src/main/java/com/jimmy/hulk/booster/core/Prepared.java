package com.jimmy.hulk.booster.core;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.util.ArrayUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.google.common.collect.Maps;
import com.google.common.escape.Escaper;
import com.google.common.escape.Escapers;
import com.jimmy.hulk.booster.support.SQLExecutor;
import com.jimmy.hulk.common.constant.Constants;
import com.jimmy.hulk.config.support.SystemVariableContext;
import com.jimmy.hulk.parse.core.element.PrepareParamNode;
import com.jimmy.hulk.parse.core.result.ParseResultNode;
import com.jimmy.hulk.parse.support.SQLParser;
import com.jimmy.hulk.protocol.core.BindValue;
import com.jimmy.hulk.protocol.core.PreparedStatement;
import com.jimmy.hulk.protocol.packages.ExecutePacket;
import com.jimmy.hulk.protocol.reponse.select.PreparedStmtResponse;
import com.jimmy.hulk.protocol.utils.HexFormatUtil;
import com.jimmy.hulk.protocol.utils.constant.Fields;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@Component
@Slf4j
public class Prepared {

    private final static String PREPARED_STATEMENT_FILE_PATH = "preparedStatement";

    private final Map<Long, PreparedStatement> preparedMap = Maps.newHashMap();

    private final AtomicLong statementIdSequence = new AtomicLong(999);

    private final Escaper varcharEscaper;

    @Autowired
    private SQLParser sqlParser;

    @Autowired
    private SQLExecutor sqlExecutor;

    @Autowired
    private SystemVariableContext systemVariableContext;

    public Prepared() {
        Escapers.Builder escapeBuilder = Escapers.builder();
        escapeBuilder.addEscape('\'', "\\'");
        escapeBuilder.addEscape('$', "\\$");
        varcharEscaper = escapeBuilder.build();
    }

    public void clear() {
        String fileStorePath = systemVariableContext.getFileStorePath();
        if (!StrUtil.endWith(fileStorePath, Constants.Booster.SEPARATOR)) {
            fileStorePath = fileStorePath + Constants.Booster.SEPARATOR;
        }
        //??????????????????
        String filePath = StrUtil.builder().append(fileStorePath).append(PREPARED_STATEMENT_FILE_PATH).toString();
        if (FileUtil.exist(filePath)) {
            FileUtil.del(filePath);
        }
    }

    public void stmtPrepare(String sql, Session session) {
        log.info("??????????????????SQL???{}", sql);
        //SQL??????
        ParseResultNode parseResultNode = sqlParser.parse(sql);
        List<PrepareParamNode> prepareParamNodes = parseResultNode.getPrepareParamNodes();
        //???????????????
        long statementId = statementIdSequence.incrementAndGet();

        log.info("?????????????????????????????????:{},??????sql:{}", statementId, sql);
        //????????????
        PreparedStatement preparedStatement = new PreparedStatement(statementId, sql, parseResultNode.getColumns().size(), prepareParamNodes.size());
        preparedMap.put(statementId, preparedStatement);
        //????????????
        this.storePreparedStatement(statementId, preparedStatement);
        //???????????????
        PreparedStmtResponse.response(preparedStatement, session);
    }

    public PreparedStatement getPreparedStatement(long statementId) {
        PreparedStatement preparedStatement = preparedMap.get(statementId);
        if (preparedStatement != null) {
            return preparedStatement;
        }

        return this.loadPreparedStatement(statementId);
    }

    public void stmtExecute(ExecutePacket packet, Session session) {
        BindValue[] bindValues = packet.values;
        PreparedStatement preparedStatement = packet.preparedStatement;
        String sql = preparedStatement.getSql();
        int[] paramTypes = preparedStatement.getParametersType();
        if (ArrayUtil.isEmpty(paramTypes)) {
            //??????sql
            sqlExecutor.execute(sql, session);
            return;
        }
        //???????????????
        StringBuilder sb = new StringBuilder();
        int idx = 0;
        for (int i = 0, len = sql.length(); i < len; i++) {
            char c = sql.charAt(i);
            if (c != '?') {
                sb.append(c);
                continue;
            }
            // ????????????????
            int paramType = paramTypes[idx];
            BindValue bindValue = bindValues[idx];
            idx++;
            // ???????????????????????????
            if (bindValue.isNull) {
                sb.append("NULL");
                continue;
            }
            // ????????????, ???????????????????????????
            switch (paramType & 0xff) {
                case Fields.FIELD_TYPE_TINY:
                    sb.append(String.valueOf(bindValue.byteBinding));
                    break;
                case Fields.FIELD_TYPE_SHORT:
                    sb.append(String.valueOf(bindValue.shortBinding));
                    break;
                case Fields.FIELD_TYPE_LONG:
                    sb.append(bindValue.intBinding);
                    break;
                case Fields.FIELD_TYPE_LONGLONG:
                    sb.append(bindValue.longBinding);
                    break;
                case Fields.FIELD_TYPE_FLOAT:
                    sb.append(bindValue.floatBinding);
                    break;
                case Fields.FIELD_TYPE_DOUBLE:
                    sb.append(bindValue.doubleBinding);
                    break;
                case Fields.FIELD_TYPE_VAR_STRING:
                case Fields.FIELD_TYPE_STRING:
                case Fields.FIELD_TYPE_VARCHAR:
                    bindValue.value = varcharEscaper.asFunction().apply(String.valueOf(bindValue.value));
                    sb.append("'" + bindValue.value + "'");
                    break;
                case Fields.FIELD_TYPE_TINY_BLOB:
                case Fields.FIELD_TYPE_BLOB:
                case Fields.FIELD_TYPE_MEDIUM_BLOB:
                case Fields.FIELD_TYPE_LONG_BLOB:
                    if (bindValue.value instanceof ByteArrayOutputStream) {
                        byte[] bytes = ((ByteArrayOutputStream) bindValue.value).toByteArray();
                        sb.append("X'" + HexFormatUtil.bytesToHexString(bytes) + "'");
                    } else {
                        sb.append("'" + bindValue.value + "'");
                    }
                    break;
                case Fields.FIELD_TYPE_TIME:
                case Fields.FIELD_TYPE_DATE:
                case Fields.FIELD_TYPE_DATETIME:
                case Fields.FIELD_TYPE_TIMESTAMP:
                    sb.append("'" + bindValue.value + "'");
                    break;
                default:
                    bindValue.value = varcharEscaper.asFunction().apply(String.valueOf(bindValue.value));
                    sb.append(bindValue.value.toString());
                    break;
            }
        }
        //??????sql
        sqlExecutor.execute(sb.toString(), session);
    }

    public void close(long statementId) {
        log.info("???????????????:{}", statementId);

        preparedMap.remove(statementId);
    }

    /**
     * ????????????
     *
     * @param statementId
     * @param preparedStatement
     */
    private void storePreparedStatement(long statementId, PreparedStatement preparedStatement) {
        String fileStorePath = systemVariableContext.getFileStorePath();
        if (!StrUtil.endWith(fileStorePath, Constants.Booster.SEPARATOR)) {
            fileStorePath = fileStorePath + Constants.Booster.SEPARATOR;
        }

        String filePath = StrUtil.builder().append(fileStorePath).append(PREPARED_STATEMENT_FILE_PATH).toString();
        //???????????????
        if (!FileUtil.exist(filePath)) {
            synchronized (Prepared.class) {
                if (!FileUtil.exist(filePath)) {
                    FileUtil.mkdir(filePath);
                }
            }
        }

        String fileName = filePath + Constants.Booster.SEPARATOR + statementId + ".pre";
        FileUtil.writeUtf8String(JSON.toJSONString(preparedStatement, SerializerFeature.WriteMapNullValue), fileName);
    }

    /**
     * ????????????
     *
     * @param statementId
     * @return
     */
    private PreparedStatement loadPreparedStatement(long statementId) {
        String fileStorePath = systemVariableContext.getFileStorePath();
        if (!StrUtil.endWith(fileStorePath, Constants.Booster.SEPARATOR)) {
            fileStorePath = fileStorePath + Constants.Booster.SEPARATOR;
        }

        String filePath = StrUtil.builder()
                .append(fileStorePath)
                .append(PREPARED_STATEMENT_FILE_PATH)
                .append(Constants.Booster.SEPARATOR)
                .append(statementId)
                .append(".pre")
                .toString();
        //??????????????????
        if (!FileUtil.exist(filePath)) {
            return null;
        }

        String s = FileUtil.readUtf8String(filePath);
        return StrUtil.isEmpty(s) ? null : JSON.parseObject(s, PreparedStatement.class);
    }
}