package com.hzy.test;

/**
 * Created by Hzy on 2016/8/11.
 */
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SampleReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private static final Logger logger = LogManager.getLogger(SampleReducer.class);
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        System.out.println("进入reduce");
        logger.info("进入reduce");
        int sum = 0;

        for (IntWritable val : values) {
            sum += val.get();
        }
        context.write(key, new IntWritable(sum));
        logger.info("reduce结束");
        System.out.println("reduce结束");
    }

}