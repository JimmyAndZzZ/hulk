package com.jimmy.hulk.protocol.reponse.select;

import com.jimmy.hulk.protocol.core.Context;
import com.jimmy.hulk.protocol.core.PreparedStatement;
import com.jimmy.hulk.protocol.packages.PreparedOkPacket;

public class PreparedStmtResponse {

    public static void response(PreparedStatement preparedStatement, Context c) {
        byte packetId = 0;
        // write preparedOk packet
        PreparedOkPacket preparedOk = new PreparedOkPacket();
        preparedOk.packetId = ++packetId;
        preparedOk.statementId = preparedStatement.getId();
        preparedOk.columnsNumber = preparedStatement.getColumnsNumber();
        preparedOk.parametersNumber = preparedStatement.getParametersNumber();
        preparedOk.write(c.getChannelHandlerContext());
    }
}