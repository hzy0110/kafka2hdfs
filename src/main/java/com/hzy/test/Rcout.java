package com.hzy.test;

import com.hzy.util.DateUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Hzy on 2016/7/27.
 */
public class Rcout {

    public static void main(String[] args) throws IOException, Exception {
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "order total");
            //设置每行的列簇数
            RCFileOutputFormat.setColumnNumber(conf, 4);

//            FileInputFormat.setInputPaths(job, new Path("e:/a.txt"));
//            RCFileOutputFormat.setOutputPath(job, new Path("e:/b.txt"));
//
//            job.setInputFormatClass(TextInputFormat.class);
//            job.setOutputFormatClass(RCFileOutputFormat.class);

            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(BytesRefArrayWritable.class);

            job.setMapperClass(OutPutTestMapper.class);

            conf.set("date", DateUtil.getDate());
            //设置压缩参数
            conf.setBoolean("mapred.output.compress", true);
            conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");

            //code = (job.waitForCompletion(true)) ? 0 : 1;

        }
        catch (Exception e){

        }
    }

    public class OutPutTestMapper extends Mapper<LongWritable, Text, LongWritable, BytesRefArrayWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String day = context.getConfiguration().get("date");
            if (!line.equals("")) {
                String[] lines = line.split(" ", -1);
                if (lines.length > 3) {
                    String time_temp = lines[1];
                    String times =DateUtil.getDate();
                    String d = times.substring(0, 10);
                    if (day.equals(d)) {
                        byte[][] record = {lines[0].getBytes("UTF-8"), lines[1].getBytes("UTF-8"), lines[2].getBytes("UTF-8"), lines[3].getBytes("UTF-8")};

                        BytesRefArrayWritable bytes = new BytesRefArrayWritable(record.length);

                        for (int i = 0; i < record.length; i++) {
                            BytesRefWritable cu = new BytesRefWritable(record[i], 0, record[i].length);
                            bytes.set(i, cu);
                        }
                        context.write(key, bytes);
                    }
                }
            }
        }
    }
}
