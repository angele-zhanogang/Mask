package com.angle.utils;

import java.io.*;
import java.util.Properties;
import java.util.logging.Logger;

public class Resources {
    private static final Logger log = Logger.getLogger("resources");
    private static Properties prop;

    static {
        initLocal();
    }
    private static void initLocal() {
        prop = new Properties();
        InputStream in = null;
        try {
            String fileName = "jdbc.properties";
            in = Resources.class.getClassLoader().getResourceAsStream(fileName);
            prop.load(in);
        } catch (FileNotFoundException e) {
            log.info("配置文件不存在！！！");
        } catch (IOException e) {
            log.info("读取文件配置文件异常！！！");
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    log.info("流关闭异常！！！");
                }
            }
        }
    }

    public static String getProperty(String key) {
        return prop.getProperty(key);
    }

}
