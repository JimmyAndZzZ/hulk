package com.jimmy.hulk.actuator.base;

import java.util.Map;

public interface Serializer {

    byte[] serialize(Map<String, Object> map);

    Map<String, Object> deserialize(byte[] bytes);
}
