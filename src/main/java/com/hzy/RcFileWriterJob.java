package com.hzy;

import com.hzy.util.DateUtil;
import com.hzy.util.HdfsDAO;
import com.hzy.util.PropertiesUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.rcfile.RCFileMapReduceInputFormat;
import org.apache.hive.hcatalog.rcfile.RCFileMapReduceOutputFormat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URI;

public class RcFileWriterJob extends Configured implements Tool{
    private static final Logger logger = LogManager.getLogger(RcFileWriterJob.class);
    public static class Map extends Mapper<Object, Text, NullWritable, BytesRefArrayWritable>{
        private byte[] fieldData;
        private int numCols;
        private BytesRefArrayWritable bytes;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            numCols = context.getConfiguration().getInt("hive.io.rcfile.column.number.conf", 1);
            bytes = new BytesRefArrayWritable(numCols);
        }

        public void map(Object key, Text line, Context context
        ) throws IOException, InterruptedException {
            bytes.clear();
            String[] cols = line.toString().split(",", -1);
            logger.info("SIZE : " + cols.length);
            for (int i=0; i<numCols; i++){
                fieldData = cols[i].getBytes("UTF-8");
                BytesRefWritable cu = new BytesRefWritable(fieldData, 0, fieldData.length);
                bytes.set(i, cu);
            }
            context.write(NullWritable.get(), bytes);
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = HdfsDAO.config();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        /*if(otherArgs.length < 2){
            logger.info("Usage: " +
                    "hadoop jar RCFileLoader.jar <main class> " +
                    "-tableName <tableName> -numCols <numberOfColumns> -input <input path> " +
                    "-output <output path> -rowGroupSize <rowGroupSize> -ioBufferSize <ioBufferSize>");
            logger.info("For test");
            logger.info("$HADOOP jar RCFileLoader.jar edu.osu.cse.rsam.rcfile.mapreduce.LoadTable " +
                    "-tableName test1 -numCols 10 -input RCFileLoaderTest/test1 " +
                    "-output RCFileLoaderTest/RCFile_test1");
            logger.info("$HADOOP jar RCFileLoader.jar edu.osu.cse.rsam.rcfile.mapreduce.LoadTable " +
                    "-tableName test2 -numCols 5 -input RCFileLoaderTest/test2 " +
                    "-output RCFileLoaderTest/RCFile_test2");
            return 2;
        }*/

        //String tableName = "";
        int numCols = 0;
        String inputPath = "";
        String outputPath = "";
        String compress = "";
        int rowGroupSize = 16 *1024*1024;
        int ioBufferSize = 128*1024;


        //获取上一个小时的时间,年份后2位
        String date = DateUtil.getDate();
        String year = date.substring(2, 4);
        String month = date.substring(5, 7);
        String day = date.substring(8, 10);
        String appendWriteHdfsPath = PropertiesUtil.getValue("AppendWriteHdfsPath");
        String writeHdfsStr = appendWriteHdfsPath + year + "/" + month + "/" + day;

        numCols = Integer.parseInt(PropertiesUtil.getValue("NumCols"));

        inputPath = writeHdfsStr + "/all.txt";
        logger.info("inputPath:"+inputPath);

        outputPath = writeHdfsStr + "/rcall";
        logger.info("outputPath:"+outputPath);

/*

        for (int i=0; i<otherArgs.length - 1; i++){
//            if("-tableName".equals(otherArgs[i])){
//                tableName = otherArgs[i+1];
//            }
            if ("-numCols".equals(otherArgs[i])){
                //numCols = Integer.parseInt(otherArgs[i+1]);

            }
            else if ("-input".equals(otherArgs[i])){
                //inputPath = otherArgs[i+1];

            }
            else if("-output".equals(otherArgs[i])){
                //outputPath = otherArgs[i+1];

            }
//            else if("-compress".equals(otherArgs[i])){
//                compress = otherArgs[i+1];
//            }
//            else if("-rowGroupSize".equals(otherArgs[i])){
//                rowGroupSize = Integer.parseInt(otherArgs[i+1]);
//            }
//            else if("-ioBufferSize".equals(otherArgs[i])){
//                ioBufferSize = Integer.parseInt(otherArgs[i+1]);
//            }

        }
*/


/*
        1、为map中间输出启用压缩。

        一般对于中间输出压缩采用低压缩比，高压缩解压缩速度的压缩算法，如LZO,Snappy

        set hive.exec.compress.intermediate=true;

        set mapred.map.output.compression.codec=com.hadoop.compression.lzo.LzoCodec;



        2、为最终输出结果启用压缩

        需要注意的是：有些压缩格式是不支持切分的，这样后续mapre-reduce任务将不能并行处理。

        set hive.exec.compress.output=true;

        set mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;

        3、为输出使用sequence file 文件格式

        create table tname stored as sequencefile;

        为sequence file 文件开启压缩

        set mapred.output.compression.type=BLOCK;



        常见的压缩格式：

        DEFLATE org.apache.hadoop.io.compress.DefaultCodec
        gzip org.apache.hadoop.io.compress.GzipCodec
        bzip org.apache.hadoop.io.compress.BZip2Codec
        Snappy org.apache.hadoop.io.compress.SnappyCodec
        lzo    com.hadoop.compression.lzo.LzoCodec

        */

        conf.setInt("hive.io.rcfile.record.buffer.size", 16777216);// 16 * 1024 * 1024
        conf.setInt("io.file.buffer.size", 131072);// 缓冲区大小 128 * 1024


        Job job = Job.getInstance(conf);
        job.setJobName("RcFileWriterJob");
        job.setJarByClass(RcFileWriterJob.class);
        job.setMapperClass(Map.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(BytesRefArrayWritable.class);
//      job.setNumReduceTasks(0);


        RCFileMapReduceInputFormat.addInputPath(job, new Path(inputPath));
        RCFileMapReduceOutputFormat.setColumnNumber(job.getConfiguration(), numCols);
        RCFileMapReduceOutputFormat.setOutputPath(job, new Path(outputPath));
        job.setOutputFormatClass(RCFileMapReduceOutputFormat.class);
        RCFileMapReduceOutputFormat.setCompressOutput(job, true);
        //RCFileMapReduceOutputFormat.setOutputCompressorClass(job,  GzipCodec.class);
        //RCFileMapReduceOutputFormat.setOutputCompressorClass(job,  DefaultCodec.class);
        //RCFileMapReduceOutputFormat.setOutputCompressorClass(job,  Lz4Codec.class);
        RCFileMapReduceOutputFormat.setOutputCompressorClass(job,  SnappyCodec.class);



        //logger.info("Loading table " + tableName + " from " + inputPath + " to RCFile located at " + outputPath);
        logger.info("Loading  from " + inputPath + " to RCFile located at " + outputPath);
        logger.info("number of columns:" + job.getConfiguration().get("hive.io.rcfile.column.number.conf"));
        logger.info("RCFile row group size:" + job.getConfiguration().get("hive.io.rcfile.record.buffer.size"));
        logger.info("io bufer size:" + job.getConfiguration().get("io.file.buffer.size"));

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new RcFileWriterJob(), args);
        System.exit(res);
    }

}
