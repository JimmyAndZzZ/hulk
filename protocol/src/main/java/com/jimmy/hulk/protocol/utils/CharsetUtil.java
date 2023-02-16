package com.jimmy.hulk.protocol.utils;

import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Slf4j
public class CharsetUtil {

    private static final String INDEX_CHARSET_FILE_NAME = "index_to_charset.properties";

    private static final Map<Integer, String> INDEX_TO_CHARSET = new HashMap<>();

    private static final Map<String, Integer> CHARSET_TO_INDEX = new HashMap<>();

    static {
        // index_to_charset.properties
        INDEX_TO_CHARSET.put(1, "big5");
        INDEX_TO_CHARSET.put(8, "latin1");
        INDEX_TO_CHARSET.put(9, "latin2");
        INDEX_TO_CHARSET.put(14, "cp1251");
        INDEX_TO_CHARSET.put(28, "gbk");
        INDEX_TO_CHARSET.put(24, "gb2312");
        INDEX_TO_CHARSET.put(33, "utf8");
        INDEX_TO_CHARSET.put(45, "utf8mb4");

        Properties prop = new Properties();
        try (InputStream resourceAsStream = CharsetUtil.class.getClassLoader().getResourceAsStream(INDEX_CHARSET_FILE_NAME);
             InputStreamReader inputStreamReader = new InputStreamReader(resourceAsStream, StandardCharsets.UTF_8)) {
            prop.load(inputStreamReader);
            for (Object index : prop.keySet()) {
                INDEX_TO_CHARSET.put(Integer.parseInt((String) index), prop.getProperty((String) index));
            }
        } catch (Exception e) {
            log.error("error:", e);
        }
        // charset --> index
        for (Integer key : INDEX_TO_CHARSET.keySet()) {
            String charset = INDEX_TO_CHARSET.get(key);
            if (charset != null && CHARSET_TO_INDEX.get(charset) == null) {
                CHARSET_TO_INDEX.put(charset, key);
            }
        }

        CHARSET_TO_INDEX.put("iso-8859-1", 14);
        CHARSET_TO_INDEX.put("iso_8859_1", 14);
        CHARSET_TO_INDEX.put("utf-8", 33);
    }

    public static final String getCharset(int index) {
        return INDEX_TO_CHARSET.get(index);
    }

    public static final int getIndex(String charset) {
        if (charset == null || charset.length() == 0) {
            return 0;
        } else {
            Integer i = CHARSET_TO_INDEX.get(charset.toLowerCase());
            return (i == null) ? 0 : i;
        }
    }
}
