package com.jimmy.hulk.actuator.enums;

import com.jimmy.hulk.actuator.base.Serializer;
import com.jimmy.hulk.actuator.part.serializer.KryoSerializer;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum SerializerTypeEnum {

    KRYO("kryo", new KryoSerializer());

    private String name;

    private Serializer serializer;

    public static SerializerTypeEnum getByName(String name) {
        for (SerializerTypeEnum value : SerializerTypeEnum.values()) {
            if (value.name.equalsIgnoreCase(name)) {
                return value;
            }
        }

        return null;
    }
}
