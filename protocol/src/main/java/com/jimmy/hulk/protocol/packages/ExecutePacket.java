package com.jimmy.hulk.protocol.packages;

import com.jimmy.hulk.protocol.core.PreparedStatement;
import com.jimmy.hulk.protocol.core.BindValue;
import com.jimmy.hulk.protocol.core.MySQLMessage;
import com.jimmy.hulk.protocol.utils.BindValueUtil;

import java.io.UnsupportedEncodingException;

public class ExecutePacket extends MySQLPacket {

    public byte status;
    public long statementId;
    public byte flags;
    public long iterationCount;
    public byte[] nullBitMap;
    public byte newParameterBoundFlag;
    public BindValue[] values;
    public PreparedStatement preparedStatement;

    public ExecutePacket(PreparedStatement preparedStatement) {
        this.preparedStatement = preparedStatement;
        this.values = new BindValue[preparedStatement.getParametersNumber()];
    }

    public void read(byte[] data, String charset) throws UnsupportedEncodingException {
        MySQLMessage mm = new MySQLMessage(data);
        status = mm.read();
        statementId = mm.readUB4();
        flags = mm.read();
        iterationCount = mm.readUB4();
        // 读取NULL指示器数据
        int parameterCount = values.length;
        if(parameterCount > 0) {
	        nullBitMap = new byte[(parameterCount + 7) / 8];
	        for (int i = 0; i < nullBitMap.length; i++) {
	            nullBitMap[i] = mm.read();
	        }

	        newParameterBoundFlag = mm.read();
        }
        // 当newParameterBoundFlag==1时，更新参数类型。
        if (newParameterBoundFlag == (byte) 1) {
            for (int i = 0; i < parameterCount; i++) {
                preparedStatement.getParametersType()[i] = mm.readUB2();
            }
        }
        // 设置参数类型和读取参数值
        byte[] nullBitMap = this.nullBitMap;
        for (int i = 0; i < parameterCount; i++) {
            BindValue bv = new BindValue();
            bv.type = preparedStatement.getParametersType()[i];
            if ((nullBitMap[i / 8] & (1 << (i & 7))) != 0) {
                bv.isNull = true;
            } else {
                BindValueUtil.read(mm, bv, charset);
                if(bv.isLongData) {
                	bv.value = preparedStatement.getLongData(i);
                }
            }
            values[i] = bv;
        }
    }

    @Override
    public int calcPacketSize() {
        
        return 0;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL Execute Packet";
    }

}