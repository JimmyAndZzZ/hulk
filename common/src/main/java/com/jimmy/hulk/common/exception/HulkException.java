package com.jimmy.hulk.common.exception;

import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.constant.ErrorCode;

public class HulkException extends RuntimeException {

    private int code = ErrorCode.ER_UNKNOWN_COM_ERROR;

    private String message;

    private ModuleEnum module;

    public HulkException(String message) {
        super(message);
        this.message = message;
    }

    public HulkException(String message, ModuleEnum module) {
        super(message);
        this.message = message;
        this.module = module;
    }

    public HulkException(int code, String message, ModuleEnum module) {
        super(message);
        this.code = code;
        this.message = message;
        this.module = module;
    }

    public HulkException(int code, String desc, Exception e, ModuleEnum module) {
        super(e);
        this.code = code;
        this.message = desc;
        this.module = module;
    }

    public HulkException() {
        super();
    }

    public HulkException(Throwable cause) {
        super(cause);
    }

    public int getCode() {
        return code;
    }

    @Override
    public String getMessage() {
        return message;
    }

    public ModuleEnum getModule() {
        return module;
    }
}
