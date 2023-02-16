package com.jimmy.hulk.actuator.memory;

import org.springframework.util.Assert;

import java.nio.ByteBuffer;

public class HeapMemorySegment extends BaseSegment {

    private final ByteBuffer byteBuffer;

    HeapMemorySegment(int capacity) {
        this.byteBuffer = ByteBuffer.allocate(capacity);
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
        if(free.get()){
            System.out.println("123");
        }
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
    }
}
