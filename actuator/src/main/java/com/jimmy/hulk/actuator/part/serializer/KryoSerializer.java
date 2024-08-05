package com.jimmy.hulk.actuator.part.serializer;

import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;
import com.esotericsoftware.kryo.kryo5.objenesis.strategy.StdInstantiatorStrategy;
import com.esotericsoftware.kryo.kryo5.serializers.MapSerializer;
import com.google.common.collect.Maps;
import com.jimmy.hulk.actuator.base.Serializer;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class KryoSerializer implements Serializer {

    //由于Kryo是线程不安全的，所以我们这里使用ThreadLocal来解决线程安全问题
    public static ThreadLocal<Kryo> kryoThreadLocal = ThreadLocal.withInitial(() -> {
        Kryo kryo = new Kryo();
        kryo.register(HashMap.class, new MapSerializer());
        kryo.register(Timestamp.class);
        kryo.register(BigInteger.class);
        kryo.register(BigDecimal.class);
        kryo.reference(false);
        kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
        return kryo;
    });

    @Override
    public byte[] serialize(Map<String, Object> map) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             final Output output = new Output(baos, 8192)) {
            Kryo kryo = kryoThreadLocal.get();
            //进行序列化
            kryo.writeObject(output, map);
            kryoThreadLocal.remove();
            return output.toBytes();
        } catch (Exception e) {
            log.error("序列化失败", e);
            throw new HulkException("序列化失败", ModuleEnum.ACTUATOR);
        }
    }

    @Override
    public Map<String, Object> deserialize(byte[] bytes) {
        if (bytes == null) {
            return Maps.newHashMap();
        }

        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
             Input input = new Input(byteArrayInputStream, 8192)) {
            Kryo kryo = kryoThreadLocal.get();
            Map obj = kryo.readObject(input, HashMap.class);
            kryoThreadLocal.remove();
            return obj;
        } catch (Exception e) {
            log.error("反序列化失败", e);
            throw new HulkException("反序列化失败", ModuleEnum.ACTUATOR);
        }
    }
}
