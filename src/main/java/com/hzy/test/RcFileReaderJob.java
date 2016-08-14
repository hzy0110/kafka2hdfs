package com.hzy.test;

/**
 * Created by Hzy on 2016/7/27.
 */
import com.hzy.util.HdfsDAO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hive.hcatalog.rcfile.RCFileMapReduceInputFormat;

import java.io.IOException;

public class RcFileReaderJob {
    static class RcFileMapper extends Mapper<Object, BytesRefArrayWritable, Text, NullWritable> {
        @Override
        protected void map(Object key, BytesRefArrayWritable value,
                           Context context)
                throws IOException, InterruptedException {
            Text txt = new Text();
            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < value.size(); i++) {
                BytesRefWritable v = value.get(i);
                txt.set(v.getData(), v.getStart(), v.getLength());
                if (i == value.size() - 1) {
                    sb.append(txt.toString());
                } else {
                    sb.append(txt.toString() + "\t");
                }
            }
            context.write(new Text(sb.toString()), NullWritable.get());
        }

        @Override
        protected void cleanup(Context context) throws IOException,
                InterruptedException {
            super.cleanup(context);
        }

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            super.setup(context);

        }
    }

    static class RcFileReduce extends Reducer<Text, NullWritable, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values,
                              Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    public static boolean runLoadMapReducue(Configuration conf, Path input, Path output) throws IOException,
            ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(conf);
        job.setJarByClass(RcFileReaderJob.class);
        job.setJobName("RcFileReaderJob");
        job.setNumReduceTasks(1);
        job.setMapperClass(RcFileMapper.class);
        job.setReducerClass(RcFileReduce.class);
        job.setInputFormatClass(RCFileMapReduceInputFormat.class);
//        MultipleInputs.addInputPath(job, input, RCFileInputFormat.class);
        RCFileMapReduceInputFormat.addInputPath(job, input);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileOutputFormat.setOutputPath(job, output);
        return job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HdfsDAO.config();
//        if (args.length != 2) {
//            System.err.println("Usage: rcfile <in> <out>");
//            System.exit(2);
//        }
        RcFileReaderJob.runLoadMapReducue(conf, new Path("/tmp/kafka/advertall/16/08/05/rcall7/part-r-00000"),
                new Path("/tmp/kafka/adverttest/rcr7"));
    }
}