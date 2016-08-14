package com.hzy.test;

/**
 * Created by Hzy on 2016/8/11.
 */
import com.hzy.util.HdfsDAO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;


public class SampleOozieActionConfigurator  {
    private static final Logger logger = LogManager.getLogger(SampleOozieActionConfigurator.class);
    //@Override
    public void configure(Job actionConf) throws IOException {
    }

    public static void main(String[] args) throws Exception {


        Configuration conf = HdfsDAO.config();
        Job job = Job.getInstance(conf, "SampleOozieActionConfigurator");
        job.setJarByClass(SampleOozieActionConfigurator.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(SampleMapper.class);
        job.setReducerClass(SampleReducer.class);
        //job.setNumReduceTasks(1);
        //actionConf.setNumMapTasks(1);
        FileInputFormat.addInputPath(job, new Path("/tmp/data/file0"));
        FileOutputFormat.setOutputPath(job, new Path("/tmp/t3"));
//        FileInputFormat.addInputPath(job, new Path("/user/root/examples/input-data/text"));
//        FileOutputFormat.setOutputPath(job, new Path("/user/root/examples/output-data/t2"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}

