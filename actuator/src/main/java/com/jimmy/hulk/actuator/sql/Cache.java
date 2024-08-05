package com.jimmy.hulk.actuator.sql;

import cn.hutool.core.convert.Convert;
import cn.hutool.core.date.DateUnit;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.crypto.SecureUtil;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jimmy.hulk.actuator.support.SQLBox;
import com.jimmy.hulk.common.other.FileReader;
import com.jimmy.hulk.actuator.support.ExecuteHolder;
import com.jimmy.hulk.common.constant.Constants;
import com.jimmy.hulk.config.support.SystemVariableContext;
import com.jimmy.hulk.parse.core.result.ExtraNode;
import com.jimmy.hulk.parse.core.result.ParseResultNode;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

@Slf4j
public class Cache extends SQL<List<Map<String, Object>>> {

    private final static String CACHE_FILE_PATH = "cache";

    private final Select select;

    private final Map<String, CountDownLatch> wait;

    public Cache() {
        this.wait = Maps.newConcurrentMap();
        this.select = SQLBox.instance().get(Select.class);
    }

    @Override
    public List<Map<String, Object>> process(ParseResultNode parseResultNode) throws Exception {
        try {
            String sql = parseResultNode.getSql();
            ExtraNode extraNode = parseResultNode.getExtraNode();
            //过期时间
            String expire = extraNode.getExpire();
            //数据源判断
            String dsName = extraNode.getDsName();
            if (StrUtil.isEmpty(dsName) && StrUtil.isEmpty(ExecuteHolder.getDatasourceName())) {
                ExecuteHolder.setDatasourceName(dsName);
            }
            //后缀处理
            String fileStorePath = SystemVariableContext.instance().getFileStorePath();
            String dirPath = StrUtil.builder().append(fileStorePath).append(CACHE_FILE_PATH).toString();
            //创建文件夹
            if (!FileUtil.exist(dirPath)) {
                synchronized (Cache.class) {
                    if (!FileUtil.exist(dirPath)) {
                        FileUtil.mkdir(dirPath);
                    }
                }
            }

            String md5 = SecureUtil.md5(sql);
            String filePath = dirPath + Constants.Booster.SEPARATOR + md5 + ".cache";
            return this.loadFromCache(md5, filePath, expire, parseResultNode);
        } catch (Exception e) {
            log.error("缓存查询失败", e);
            throw e;
        }
    }

    /**
     * 缓存读取
     *
     * @param filePath
     * @param expire
     * @return
     */
    private List<Map<String, Object>> loadFromCache(String md5, String filePath, String expire, ParseResultNode parseResultNode) throws Exception {
        Integer expireTime = Convert.toInt(expire, SystemVariableContext.instance().getDefaultExpire());
        //判断是否击中缓存
        if (FileUtil.exist(filePath)) {
            Date now = new Date();
            Date fileDate = new Date(FileUtil.newFile(filePath).lastModified());

            if (DateUtil.compare(fileDate, now) >= 0) {
                return this.loadFromDisk(filePath);
            }
            //判断最后更新时间是否满足过期时间
            if (DateUtil.between(fileDate, now, DateUnit.MINUTE) <= expireTime) {
                return this.loadFromDisk(filePath);
            }
        }
        //阻塞，防止重复读取
        CountDownLatch downLatch = new CountDownLatch(1);
        CountDownLatch put = wait.put(md5, downLatch);
        if (put != null) {
            downLatch = null;
            put.await();
            return this.loadFromCache(md5, filePath, expire, parseResultNode);
        }

        List<Map<String, Object>> process = select.process(parseResultNode);
        //清空原有数据
        FileUtil.writeUtf8String(StrUtil.EMPTY, filePath);
        this.writeToDisk(filePath, process);
        downLatch.countDown();
        wait.remove(md5);
        return process;
    }

    /**
     * 从磁盘获取
     *
     * @param path
     * @return
     * @throws Exception
     */
    private List<Map<String, Object>> loadFromDisk(String path) throws Exception {
        FileReader bigFileReader = null;
        try {
            if (StrUtil.isEmpty(path)) {
                return Lists.newArrayList();
            }
            //获取文件大小
            List<Map<String, Object>> content = Lists.newArrayList();
            //读取文件
            FileReader.Builder builder = new FileReader.Builder(path, line -> {
                try {
                    content.add(JSON.parseObject(line.trim()));
                } catch (Exception e) {
                    throw e;
                }
            });
            bigFileReader = builder.charset(StandardCharsets.UTF_8).bufferSize(1024).build();
            bigFileReader.start();
            return content;
        } catch (Exception e) {
            log.error("SQL查询失败", e);
            throw e;
        } finally {
            if (bigFileReader != null) {
                bigFileReader.shutdown();
            }

            if (StrUtil.isNotBlank(path)) {
                FileUtil.del(path);
            }
        }
    }
}
