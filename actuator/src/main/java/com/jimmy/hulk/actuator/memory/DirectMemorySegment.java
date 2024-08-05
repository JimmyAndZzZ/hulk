package com.jimmy.hulk.actuator.memory;

import cn.hutool.core.lang.Assert;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;

public class DirectMemorySegment extends BaseSegment {

    private final ByteBuffer byteBuffer;

    DirectMemorySegment(int capacity) {
        this.byteBuffer = ByteBuffer.allocateDirect(capacity);
    }

    @Override
    public boolean write(byte[] bytes) {
        if (!free.compareAndSet(true, false)) {
            return false;
        }

        this.byteBuffer.put(bytes);
        return true;
    }

    @Override
    public byte[] read() {
        Assert.isTrue(!free.get(), "该内存块空闲");

        this.byteBuffer.flip();
        int len = this.byteBuffer.limit() - this.byteBuffer.position();

        byte[] bytes = new byte[len];

        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = this.byteBuffer.get();
        }

        return bytes;
    }

    @Override
    public void free() {
        Assert.isTrue(free.compareAndSet(false, true), "该内存块空闲");
        byteBuffer.clear();
        ((DirectBuffer) byteBuffer).cleaner().clean();
    }
}
