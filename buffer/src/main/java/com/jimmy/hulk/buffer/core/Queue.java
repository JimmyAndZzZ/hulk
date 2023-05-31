package com.jimmy.hulk.buffer.core;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.io.FileUtil;
import com.google.common.collect.Queues;
import com.jimmy.hulk.common.constant.Constants;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.config.support.SystemVariableContext;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

public class Queue {

    private static final String OFFSET_FILE_NAME = "offset.dat";

    private final ConcurrentLinkedQueue<Message> queue = Queues.newConcurrentLinkedQueue();

    private String topic;

    private boolean isRunning;

    private long currentOffset;

    private SegmentFile segmentFile;

    private SystemVariableContext systemVariableContext;

    public Queue(String topic, SystemVariableContext systemVariableContext, int batchSize, int delay) {
        this.topic = topic;
        this.isRunning = true;
        this.systemVariableContext = systemVariableContext;
        this.segmentFile = new SegmentFile(topic, systemVariableContext, batchSize, delay);


        this.currentOffset = this.readOffset();
        if (this.currentOffset > 0) {
            while (true) {
                List<Message> read = segmentFile.read(1000, this.currentOffset);
                if (CollUtil.isEmpty(read)) {
                    break;
                }

                queue.addAll(read);
            }
        }
    }

    public boolean offer(String message) {
        if (!this.isRunning) {
            throw new HulkException("未运行", ModuleEnum.BUFFER);
        }

        Message write = segmentFile.write(message);
        return queue.offer(write);
    }

    public boolean addAll(Collection<? extends String> c) {
        if (!this.isRunning) {
            throw new HulkException("未运行", ModuleEnum.BUFFER);
        }

        if (CollUtil.isEmpty(c)) {
            return false;
        }

        return queue.addAll(c.stream().map(s -> segmentFile.write(s)).collect(Collectors.toList()));
    }

    public String poll() {
        if (!this.isRunning) {
            throw new HulkException("未运行", ModuleEnum.BUFFER);
        }

        Message poll = queue.poll();
        if (poll == null) {
            return null;
        }

        this.currentOffset = poll.getOffset();
        return poll.getBody();
    }

    public void close() {
        this.isRunning = false;
        this.storeOffset();
        this.segmentFile.close();
    }

    /**
     * 存储offset
     */
    private void storeOffset() {
        String fileStorePath = systemVariableContext.getFileStorePath();

        String dir = fileStorePath + topic;
        if (!FileUtil.exist(dir)) {
            FileUtil.mkdir(dir);
        }

        FileUtil.writeUtf8String(String.valueOf(this.currentOffset), dir + Constants.Booster.SEPARATOR + OFFSET_FILE_NAME);
    }

    /**
     * 读取offset
     *
     * @return
     */
    private long readOffset() {
        String fileStorePath = systemVariableContext.getFileStorePath();

        String dir = fileStorePath + topic + Constants.Booster.SEPARATOR + OFFSET_FILE_NAME;
        if (!FileUtil.exist(dir)) {
            return 0;
        }

        return Convert.toLong(FileUtil.readUtf8String(dir), 0L);
    }
}
