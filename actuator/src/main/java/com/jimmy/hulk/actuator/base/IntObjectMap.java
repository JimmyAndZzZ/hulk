package com.jimmy.hulk.actuator.base;

import java.util.Map;

public interface IntObjectMap<V> extends Map<Integer, V> {

    interface PrimitiveEntry<V> {
        int key();

        V value();

        void setValue(V value);
    }

    V get(int key);

    V put(int key, V value);

    V remove(int key);

    Iterable<PrimitiveEntry<V>> entries();

    boolean containsKey(int key);
}
