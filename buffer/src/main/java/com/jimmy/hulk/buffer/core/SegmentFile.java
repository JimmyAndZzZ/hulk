package com.jimmy.hulk.buffer.core;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.core.util.StrUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.config.support.SystemVariableContext;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class SegmentFile {

    private static final int BATCH_SIZE = 1000;

    private static final String INDEX_LOG_SUFFIX = ".index.log";

    private final List<Message> buffer = Lists.newArrayList();

    private final ConcurrentLinkedQueue<Message> queue = Queues.newConcurrentLinkedQueue();

    private String topic;

    private boolean isRunning;

    private AtomicLong offsetSeq;

    private ExecutorService executeGroup;

    private ExecutorService listenerGroup;

    private SystemVariableContext systemVariableContext;

    public SegmentFile(String topic, SystemVariableContext systemVariableContext) {
        this(topic, systemVariableContext, 200, 30);
    }

    public SegmentFile(String topic, SystemVariableContext systemVariableContext, int batchSize, int delay) {
        this.topic = topic;
        this.isRunning = true;
        this.offsetSeq = new AtomicLong(0);
        this.systemVariableContext = systemVariableContext;
        this.executeGroup = Executors.newSingleThreadExecutor();
        this.listenerGroup = Executors.newSingleThreadExecutor();

        listenerGroup.submit((Runnable) () -> {
            while (true) {
                Message poll = queue.poll();
                if (poll == null) {
                    ThreadUtil.sleep(100);
                    Thread.yield();
                    continue;
                }

                if (!poll.getPoisonPill()) {
                    buffer.add(poll);
                }

                if (buffer.size() >= batchSize || poll.getPoisonPill()) {
                    ArrayList<Message> messages = Lists.newArrayList(buffer);
                    buffer.clear();

                    executeGroup.submit(() -> this.batchWrite(messages));
                }
            }
        });

        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> queue.add(new Message(true)), 0L, delay, TimeUnit.SECONDS);
    }

    public void close() {
        this.isRunning = false;

        this.listenerGroup.shutdown();
        this.executeGroup.shutdown();

        List<Message> surplus = Lists.newArrayList();
        if (CollUtil.isNotEmpty(this.queue)) {
            surplus.addAll(this.queue);
        }

        if (CollUtil.isNotEmpty(this.buffer)) {
            surplus.addAll(this.buffer);
        }

        this.batchWrite(surplus);
    }

    public List<Message> read(int total, long offset) {
        String fileStorePath = systemVariableContext.getFileStorePath();

        String dir = fileStorePath + topic;
        if (!FileUtil.exist(dir)) {
            return Lists.newArrayList();
        }
        //获取log日志文件
        List<IndexFile> files = this.getFile(dir, offset, total);
        if (CollUtil.isEmpty(files)) {
            log.error("{}目录下,offset:{}不存在", dir, offset);
            return Lists.newArrayList();
        }

        List<Message> messages = Lists.newArrayList();
        for (IndexFile file : files) {
            messages.addAll(this.readFileByLine(file.getFilePath(), offset, total - messages.size()));
        }

        return messages;
    }

    public void write(String message) {
        if (!this.isRunning) {
            throw new HulkException("未开启", ModuleEnum.BUFFER);
        }

        String fileStorePath = systemVariableContext.getFileStorePath();

        String dir = fileStorePath + topic;
        if (!FileUtil.exist(dir)) {
            synchronized (this) {
                if (!FileUtil.exist(dir)) {
                    FileUtil.mkdir(dir);
                }
            }
        }

        this.queue.add(new Message(message, offsetSeq.getAndIncrement()));
    }

    /**
     * 获取文件名
     *
     * @param i
     * @return
     */
    private String getFileName(long i) {
        long result = i / BATCH_SIZE;
        return String.format("%010d", result * 1000);
    }

    /**
     * 批量写入
     *
     * @param messages
     */
    private void batchWrite(List<Message> messages) {
        if (CollUtil.isEmpty(messages)) {
            return;
        }

        for (Message message : messages) {
            String body = message.getBody();
            Long offset = message.getOffset();

            String fileName = this.getFileName(offset);
            this.writeFileWithLine(fileName, body);
        }
    }

    /**
     * 按行追加
     *
     * @param fileName
     * @param message
     */
    private void writeFileWithLine(String fileName, String message) {
        try (FileChannel channel = FileChannel.open(Paths.get(fileName), StandardOpenOption.APPEND)) {
            ByteBuffer buffer = ByteBuffer.wrap(message.getBytes(StandardCharsets.UTF_8));
            long position = channel.size(); // 当前文件大小即为追加写入的起始位置
            channel.position(position);
            channel.write(buffer); // 将数据写入文件
        } catch (IOException e) {
            throw new HulkException(e.getMessage(), ModuleEnum.BUFFER);
        }
    }

    /**
     * 按行读取
     *
     * @param fileName
     * @param offset
     * @param size
     * @return
     */
    private List<Message> readFileByLine(String fileName, long offset, int size) {
        try (FileChannel channel = FileChannel.open(Paths.get(fileName), StandardOpenOption.READ)) {
            int index = this.getIndex(fileName);
            StringBuilder sb = new StringBuilder();
            List<Message> lines = Lists.newArrayList();
            ByteBuffer buffer = ByteBuffer.allocate(1024);

            while (channel.read(buffer) != -1) {
                buffer.flip();

                while (buffer.hasRemaining()) {
                    byte b = buffer.get();

                    if (b == '\n') {
                        String line = sb.toString();

                        if (lines.size() >= size) {
                            return lines;
                        }

                        if (index > offset) {
                            lines.add(new Message(line, new Long(index)));
                        }

                        index++;
                        sb = new StringBuilder();
                    } else {
                        sb.append((char) b);
                    }
                }

                buffer.clear();
            }

            if (lines.size() >= size) {
                return lines;
            }

            if (sb.length() > 0) {
                lines.add(new Message(sb.toString(), new Long(index)));
            }

            return lines;
        } catch (IOException e) {
            throw new HulkException(e.getMessage(), ModuleEnum.BUFFER);
        }
    }

    /**
     * 获取offset对应文件
     *
     * @param dir
     * @param offset
     * @return
     */
    private List<IndexFile> getFile(String dir, long offset, int total) {
        //获取log日志文件
        List<String> files = FileUtil.listFileNames(dir);
        if (CollUtil.isEmpty(files)) {
            return null;
        }

        List<IndexFile> indexFiles = Lists.newArrayList();

        for (String file : files) {
            Integer index = this.getIndex(file);
            if (index >= offset) {
                indexFiles.add(new IndexFile(file, index));

                if (index - offset >= total) {
                    return indexFiles;
                }
            }
        }

        return indexFiles;
    }

    /**
     * 获取索引号
     *
     * @param file
     * @return
     */
    private int getIndex(String file) {
        return Convert.toInt(StrUtil.removeAll(file, INDEX_LOG_SUFFIX));
    }

    @Data
    private class IndexFile {
        private String filePath;

        private Integer index;

        public IndexFile(String filePath, Integer index) {
            this.filePath = filePath;
            this.index = index;
        }
    }

    private class Lock {
        private AtomicReference<Thread> lock = new AtomicReference(false);

        public void lock() {
            while (!lock.compareAndSet(null, Thread.currentThread())) {
                ThreadUtil.sleep(1);
            }
        }

        public void unLock() {
            lock.compareAndSet(Thread.currentThread(), null);
        }
    }
}
