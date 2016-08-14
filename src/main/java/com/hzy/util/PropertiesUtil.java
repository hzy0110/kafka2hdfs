package com.hzy.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Created by Hzy on 2016/6/14.
 */
public class PropertiesUtil {
    private static Properties properties = null;
    private static final Logger logger = LogManager.getLogger(PropertiesUtil.class);
    public static void main(String[] args) throws IOException {
        //logger.info(getValue("channlelevel"));
        //logger.info("223322");
    }

    public static String getValue(String name){
        try {
            Properties properties = new Properties();
            InputStream input = Thread.currentThread().getContextClassLoader().getResourceAsStream("config.properties");
            String sss = Thread.currentThread().getContextClassLoader().getResource("hadoop/core-site.xml").getFile();
            properties.load(input);// 加载属性文件
            input.close();
            return properties.getProperty(name);
        }
        catch (IOException e){
            return null;
        }

    }
}
