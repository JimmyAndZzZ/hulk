package com.jimmy.hulk.buffer.core;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.util.StrUtil;
import com.google.common.collect.Lists;
import com.jimmy.hulk.config.support.SystemVariableContext;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class SegmentFile {

    private String topic;

    private SystemVariableContext systemVariableContext;

    public List<Message> load(int total, long offset) {
        String fileStorePath = systemVariableContext.getFileStorePath();

        String dir = fileStorePath + topic;
        if (!FileUtil.exist(dir)) {
            return Lists.newArrayList();
        }
        //获取log日志文件
        String file = this.getFile(dir, offset);
        if (StrUtil.isEmpty(file)) {
            log.error("{}目录下,offset:{}不存在", dir, offset);
            return Lists.newArrayList();
        }

        return null;
    }

    /**
     * 获取offset对应文件
     *
     * @param dir
     * @param offset
     * @return
     */
    private String getFile(String dir, long offset) {
        //获取log日志文件
        List<String> files = FileUtil.listFileNames(dir);
        if (CollUtil.isEmpty(files)) {
            return null;
        }

        for (String file : files) {
            String s = StrUtil.removeAll(file, "-index.log");
            if (Convert.toInt(s) >= offset) {
                return file;
            }
        }

        return null;
    }

}
