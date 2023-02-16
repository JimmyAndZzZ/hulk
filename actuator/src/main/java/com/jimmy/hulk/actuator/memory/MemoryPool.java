package com.jimmy.hulk.actuator.memory;


import cn.hutool.core.collection.CollUtil;
import com.google.common.collect.Lists;
import com.jimmy.hulk.actuator.base.Segment;
import com.jimmy.hulk.actuator.other.IntObjectHashMap;
import com.jimmy.hulk.actuator.support.ExecuteHolder;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import lombok.extern.slf4j.Slf4j;
import org.xerial.snappy.Snappy;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class MemoryPool {

    private final static int BUFFER_SIZE = 4096 * 128;

    private final static int DEFAULT_CAPACITY = 32;

    private final AtomicInteger index = new AtomicInteger(BUFFER_SIZE + 1);

    private final List<Segment> bufferPool = Lists.newArrayList();

    private final IntObjectHashMap<Segment> extraPool = new IntObjectHashMap<>(216);

    public MemoryPool() {
        for (int i = 0; i < BUFFER_SIZE; i++) {
            bufferPool.add(new HeapMemorySegment(DEFAULT_CAPACITY));
        }
    }

    public List<Integer> allocate(byte[] bytes) {
        try {
            bytes = Snappy.compress(bytes);
            //压缩
            List<Integer> index = Lists.newArrayList();
            //切割
            List<byte[]> spilt = this.splitByteArray(bytes);
            //缓冲区已用完
            if (bufferPool.stream().filter(buffer -> buffer.isFree()).count() == 0) {
                DiskSegment diskSegment = new DiskSegment();
                diskSegment.write(bytes);

                int l = this.index.incrementAndGet();
                extraPool.put(l, diskSegment);
                index.add(l);
                ExecuteHolder.addIndex(index);
                return index;
            }

            for (byte[] b : spilt) {
                Integer freeSegmentNext = this.getFreeSegmentNext(b);
                if (freeSegmentNext != null) {
                    index.add(freeSegmentNext);
                    continue;
                }
                //缓冲区已用完
                DiskSegment diskSegment = new DiskSegment();
                diskSegment.write(b);
                int l = this.index.incrementAndGet();
                extraPool.put(l, diskSegment);
                index.add(l);
            }

            ExecuteHolder.addIndex(index);
            return index;
        } catch (Exception e) {
            throw new HulkException(e.getMessage(), ModuleEnum.ACTUATOR);
        }
    }

    public Integer allocateDirect(byte[] bytes) {
        try {
            bytes = Snappy.compress(bytes);
            //压缩
            DirectMemorySegment directMemorySegment = new DirectMemorySegment(bytes.length);
            directMemorySegment.write(bytes);
            int l = this.index.incrementAndGet();
            extraPool.put(l, directMemorySegment);
            ExecuteHolder.addIndex(l);
            return l;
        } catch (Exception e) {
            throw new HulkException(e.getMessage(), ModuleEnum.ACTUATOR);
        }
    }

    public byte[] getAndFree(List<Integer> indices) {
        byte[] bytes = this.get(indices);
        this.free(indices);
        return bytes;
    }

    public byte[] get(List<Integer> indices) {
        try {
            if (CollUtil.isEmpty(indices)) {
                return null;
            }

            Integer first = indices.get(0);
            byte[] bytes = this.get(first);
            if (bytes == null) {
                throw new HulkException("数组为空", ModuleEnum.ACTUATOR);
            }

            if (indices.size() == 1) {
                return Snappy.uncompress(bytes);
            }

            for (int i = 1; i < indices.size(); i++) {
                byte[] other = this.get(indices.get(i));
                if (other == null) {
                    throw new HulkException("数组为空", ModuleEnum.ACTUATOR);
                }

                bytes = this.mergeByteArray(bytes, other);
            }

            return Snappy.uncompress(bytes);
        } catch (Exception e) {
            throw new HulkException(e.getMessage(), ModuleEnum.ACTUATOR);
        }
    }

    public void free(Collection<Integer> indices) {
        if (CollUtil.isEmpty(indices)) {
            return;
        }

        for (Integer index : indices) {
            this.free(index);
        }
    }

    public void free(Integer index) {
        if (index > BUFFER_SIZE) {
            Segment segment = extraPool.get(index);
            if (segment != null) {
                segment.free();
                extraPool.remove(index);
            }

            return;
        }

        Segment segment = bufferPool.get(index);
        if (segment != null) {
            segment.free();
        }
    }

    /**
     * 根据下标获取数据
     *
     * @param index
     * @return
     */
    private byte[] get(Integer index) {
        return index > BUFFER_SIZE ? extraPool.get(index).read() : bufferPool.get(index).read();
    }

    /**
     * 获取空闲内存下标，若用完则返回空
     *
     * @param bytes
     * @return
     */
    private Integer getFreeSegmentNext(byte[] bytes) {
        for (int i = 0; i < bufferPool.size(); i++) {
            Segment segment = bufferPool.get(i);
            if (segment.isFree()) {
                if (segment.write(bytes)) {
                    return i;
                }

                return getFreeSegmentNext(bytes);
            }
        }

        log.error("缓冲区已用完");
        return null;
    }

    /**
     * 切割byte数组
     *
     * @param array
     * @return
     */
    private List<byte[]> splitByteArray(byte[] array) {
        int amount = array.length / DEFAULT_CAPACITY;
        List<byte[]> split = Lists.newLinkedList();
        if (amount == 0) {
            split.add(array);
            return split;
        }
        //判断余数
        int remainder = array.length % DEFAULT_CAPACITY;
        if (remainder != 0) {
            ++amount;
        }

        byte[] arr;
        for (int i = 0; i < amount; i++) {
            if (i == amount - 1 && remainder != 0) {
                // 有剩余，按照实际长度创建
                arr = new byte[remainder];
                System.arraycopy(array, i * DEFAULT_CAPACITY, arr, 0, remainder);
            } else {
                arr = new byte[DEFAULT_CAPACITY];
                System.arraycopy(array, i * DEFAULT_CAPACITY, arr, 0, DEFAULT_CAPACITY);
            }

            split.add(arr);
        }
        return split;
    }

    /**
     * 合并byte数组
     *
     * @param bt1
     * @param bt2
     * @return
     */
    private byte[] mergeByteArray(byte[] bt1, byte[] bt2) {
        byte[] bt3 = new byte[bt1.length + bt2.length];
        System.arraycopy(bt1, 0, bt3, 0, bt1.length);
        System.arraycopy(bt2, 0, bt3, bt1.length, bt2.length);
        return bt3;
    }
}
