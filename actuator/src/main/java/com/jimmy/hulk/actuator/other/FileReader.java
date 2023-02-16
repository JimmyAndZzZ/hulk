package com.jimmy.hulk.actuator.other;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.IoUtil;
import com.google.common.collect.Sets;
import com.jimmy.hulk.actuator.base.LineHandle;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class FileReader {
    private int threadPoolSize;
    private Charset charset;
    private int bufferSize;
    private LineHandle handle;
    private ExecutorService executorService;
    private long fileLength;
    private RandomAccessFile rAccessFile;
    private Set<StartEndPair> startEndPairs;
    private CountDownLatch downLatch;

    public FileReader(File file, LineHandle handle, Charset charset, int bufferSize) {
        this.fileLength = file.length();
        this.handle = handle;
        this.charset = charset;
        this.bufferSize = bufferSize;
        //分200MB一个文件处理
        long l = fileLength / 1024 / 1024 / 200;
        this.threadPoolSize = l == 0 ? 1 : (int) l;
        try {
            this.rAccessFile = new RandomAccessFile(file, "r");
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException("文件不存在！");
        }
        this.executorService = Executors.newFixedThreadPool(threadPoolSize);
        startEndPairs = Sets.newHashSet();
    }

    public void start() throws Exception {
        long everySize = this.fileLength / this.threadPoolSize;
        calculateStartEnd(0, everySize);
        downLatch = new CountDownLatch(startEndPairs.size());

        for (StartEndPair pair : startEndPairs) {
            this.executorService.execute(new SliceReaderTask(pair));
        }

        downLatch.await();
    }

    private void calculateStartEnd(long start, long size) throws IOException {
        if (start > fileLength - 1) {
            return;
        }
        StartEndPair pair = new StartEndPair();
        pair.start = start;
        long endPosition = start + size - 1;
        if (endPosition >= fileLength - 1) {
            pair.end = fileLength - 1;
            startEndPairs.add(pair);
            return;
        }

        rAccessFile.seek(endPosition);
        byte tmp = (byte) rAccessFile.read();
        while (tmp != '\n' && tmp != '\r') {
            endPosition++;
            if (endPosition >= fileLength - 1) {
                endPosition = fileLength - 1;
                break;
            }
            rAccessFile.seek(endPosition);
            tmp = (byte) rAccessFile.read();
        }
        pair.end = endPosition;
        startEndPairs.add(pair);

        calculateStartEnd(endPosition + 1, size);
    }

    public void shutdown() {
        IoUtil.close(this.rAccessFile);
        this.executorService.shutdown();
    }

    private void handle(byte[] bytes) {
        String line;
        if (this.charset == null) {
            line = new String(bytes);
        } else {
            line = new String(bytes, charset);
        }
        if (line != null && !"".equals(line)) {
            this.handle.handle(line);
        }
    }

    private static class StartEndPair {
        public long start;
        public long end;

        @Override
        public String toString() {
            return "star=" + start + ";end=" + end;
        }
    }

    private class SliceReaderTask implements Runnable {
        private long start;
        private long sliceSize;
        private byte[] readBuff;

        public SliceReaderTask(StartEndPair pair) {
            this.start = pair.start;
            this.sliceSize = pair.end - pair.start + 1;
            this.readBuff = new byte[bufferSize];
        }

        @Override
        public void run() {
            try {
                MappedByteBuffer mapBuffer = rAccessFile.getChannel().map(FileChannel.MapMode.READ_ONLY, start, this.sliceSize);
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                for (int offset = 0; offset < sliceSize; offset += bufferSize) {
                    int readLength;
                    if (offset + bufferSize <= sliceSize) {
                        readLength = bufferSize;
                    } else {
                        readLength = (int) (sliceSize - offset);
                    }
                    mapBuffer.get(readBuff, 0, readLength);
                    for (int i = 0; i < readLength; i++) {
                        byte tmp = readBuff[i];
                        //碰到换行符
                        if (tmp == '\n' || tmp == '\r') {
                            handle(bos.toByteArray());
                            bos.reset();
                        } else {
                            bos.write(tmp);
                        }
                    }
                }
                if (bos.size() > 0) {
                    handle(bos.toByteArray());
                }
            } catch (Exception e) {
                log.error("读取文件失败", e);
            } finally {
                downLatch.countDown();
            }
        }
    }

    public static class Builder {
        private Charset charset;
        private int bufferSize = 1024 * 1024;
        private LineHandle handle;
        private File file;

        public Builder(String file, LineHandle handle) {
            this.file = FileUtil.newFile(file);
            if (!this.file.exists())
                throw new IllegalArgumentException("文件不存在！");
            this.handle = handle;
        }

        public Builder charset(Charset charset) {
            this.charset = charset;
            return this;
        }

        public Builder bufferSize(int bufferSize) {
            this.bufferSize = bufferSize;
            return this;
        }

        public FileReader build() {
            return new FileReader(this.file, this.handle, this.charset, this.bufferSize);
        }
    }

}
