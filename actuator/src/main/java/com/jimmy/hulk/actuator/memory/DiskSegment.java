package com.jimmy.hulk.actuator.memory;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.lang.Assert;
import cn.hutool.core.util.IdUtil;
import cn.hutool.core.util.StrUtil;
import com.jimmy.hulk.actuator.base.Segment;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import lombok.extern.slf4j.Slf4j;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

@Slf4j
public class DiskSegment implements Segment {

    private String fileName;

    DiskSegment() {
        fileName = new StringBuilder("/tmp/").append(IdUtil.simpleUUID()).append(".seg").toString();
        FileUtil.touch(fileName);
    }

    @Override
    public boolean write(byte[] bytes) {
        try (FileChannel channel = FileChannel.open(Paths.get(fileName), StandardOpenOption.WRITE)) {
            ByteBuffer buf = ByteBuffer.allocate(5);
            for (int i = 0; i < bytes.length; ) {
                buf.put(bytes, i, Math.min(bytes.length - i, buf.limit() - buf.position()));
                buf.flip();
                i += channel.write(buf);
                buf.compact();
            }
            channel.force(false);
            return true;
        } catch (Exception e) {
            log.error("文件写入失败", e);
            throw new HulkException("文件写入失败", ModuleEnum.ACTUATOR);
        }
    }

    @Override
    public byte[] read() {
        Assert.isTrue(StrUtil.isNotBlank(fileName), "空闲无法读取");

        try (FileChannel channel = new RandomAccessFile(FileUtil.newFile(fileName), "r").getChannel()) {
            int fileSize = (int) channel.size();
            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize).load();
            byte[] result = new byte[fileSize];
            if (buffer.remaining() > 0) {
                buffer.get(result, 0, fileSize);
            }
            buffer.clear();
            return result;
        } catch (Exception e) {
            log.error("文件读取失败", e);
            throw new HulkException("文件读取失败", ModuleEnum.ACTUATOR);
        }
    }

    @Override
    public void free() {
        FileUtil.del(fileName);
    }

    @Override
    public boolean isFree() {
        return true;
    }
}
