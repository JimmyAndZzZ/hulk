package com.jimmy.hulk.buffer.core;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.util.StrUtil;
import com.google.common.collect.Lists;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.config.support.SystemVariableContext;
import lombok.Data;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;

@Slf4j
public class SegmentFile {

    private String topic;

    private SystemVariableContext systemVariableContext;

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
        String fileStorePath = systemVariableContext.getFileStorePath();

        String dir = fileStorePath + topic;
        if (!FileUtil.exist(dir)) {
            synchronized (this) {
                if (!FileUtil.exist(dir)) {
                    FileUtil.mkdir(dir);
                }
            }
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
        return Convert.toInt(StrUtil.removeAll(file, ".index.log"));
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
}
