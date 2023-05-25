package com.jimmy.hulk.buffer.core;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.core.util.StrUtil;
import com.google.common.collect.Lists;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.config.support.SystemVariableContext;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class SegmentFile {

    private static final int BATCH_SIZE = 1000;

    private static final String INDEX_LOG_SUFFIX = ".index.log";

    private final List<Message> buffer = Lists.newArrayList();

    private Lock lock;

    private String topic;

    private SystemVariableContext systemVariableContext;

    private int currentIndexLogFileName = 0;

    private AtomicInteger offsetSeq = new AtomicInteger(0);

    public SegmentFile(String topic, SystemVariableContext systemVariableContext) {
        this.topic = topic;
        this.lock = new Lock();
        this.systemVariableContext = systemVariableContext;
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

    public long write(String message) {
        String fileStorePath = systemVariableContext.getFileStorePath();

        String dir = fileStorePath + topic;
        if (!FileUtil.exist(dir)) {
            synchronized (this) {
                if (!FileUtil.exist(dir)) {
                    FileUtil.mkdir(dir);
                    FileUtil.touch(dir + File.separator + this.getFileName(currentIndexLogFileName) + INDEX_LOG_SUFFIX);
                }
            }
        }

        int seq = this.offsetSeq.incrementAndGet();
        if (seq - this.currentIndexLogFileName >= BATCH_SIZE) {
            this.resetIndex(seq);
        }

        return 0L;
    }

    /**
     * 重置当前offset标志
     *
     * @param seq
     */
    private void resetIndex(int seq) {
        lock.lock();
        try {
            if (seq - this.currentIndexLogFileName < BATCH_SIZE) {
                return;
            }

            this.currentIndexLogFileName = this.currentIndexLogFileName + 1000;
        } finally {
            lock.unLock();
        }
    }

    /**
     * 获取文件名
     *
     * @param i
     * @return
     */
    private String getFileName(int i) {
        return String.format("%010d", i);
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
