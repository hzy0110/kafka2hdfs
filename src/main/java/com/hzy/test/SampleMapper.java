package com.hzy.test;

/**
 * Created by Hzy on 2016/8/11.
 */

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class SampleMapper extends Mapper<Object, Text, Text, IntWritable> {
    private static final Logger logger = LogManager.getLogger(SampleMapper.class);
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();


    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        System.out.println("进入map");
        logger.info("进入map");
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);

        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            context.write(word, one);
        }
        logger.info("map结束");
        System.out.println("map结束");
    }

}