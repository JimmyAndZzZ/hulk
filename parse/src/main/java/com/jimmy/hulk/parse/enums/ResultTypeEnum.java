package com.jimmy.hulk.parse.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum ResultTypeEnum {

    SELECT, INSERT, UPDATE, DELETE, JOB, FLUSH, NATIVE, CACHE

}
