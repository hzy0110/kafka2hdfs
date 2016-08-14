package com.hzy;

/**
 * Created by Hzy on 2016/7/27.
 */
import com.hzy.test.WordCount;
import com.hzy.util.DateUtil;
import com.hzy.util.HdfsDAO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hive.hcatalog.rcfile.RCFileMapReduceInputFormat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;

public class CountJob {
    private static final Logger logger = LogManager.getLogger(CountJob.class);


    static class RcFileMapper extends Mapper<Object, BytesRefArrayWritable, Text, Text> {
        @Override
        protected void map(Object key, BytesRefArrayWritable value,
                           Context context)
                throws IOException, InterruptedException {
            Text txt = new Text();
            StringBuffer sb = new StringBuffer();
            String str = "";
            //IntWritable result = new IntWritable();
            String apid = "";
            //1是URL，0是PIC
            String type = "";
            String rowkey = "";
            for (int i = 0; i < value.size(); i++) {
                BytesRefWritable v = value.get(i);
                txt.set(v.getData(), v.getStart(), v.getLength());
                str = txt.toString();

                //查找合約
                if(str.startsWith("ap:")){
                    apid = str.substring(3, str.length());
                    logger.info("apid:"+apid);
                }
                if (str.startsWith("t:")) {
                    String t = str.substring(2, str.length());
                    if(t.equals("PIC"))
                        type = "0";
                    else
                        type = "1";
                }
                if (i == value.size() - 1) {
                    sb.append(txt.toString());
                }
                else {
                    sb.append(txt.toString() + "\t");
                }

                rowkey = type+addAdidZero(apid.toString()) + DateUtil.getDateBySimpleDateFormat("yyMMddHH");


                //result.set(apid);
            }
            //context.write(new Text(apid), new Text(sb.toString()));
            context.write(new Text(rowkey), new Text(sb.toString()));
        }

        private String addAdidZero(String adid){
            int maxsize = 7;
            //String adid = Integer.toString(i);
            int idlan = adid.length();
            int addlan =  maxsize-idlan;
            for(int i =0;i<addlan;i++){
                adid ="0"+ adid;
            }
            return adid;
        }



/*        @Override
        protected void cleanup(Context context) throws IOException,
                InterruptedException {
            super.cleanup(context);
        }

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            super.setup(context);

        }*/
    }


    static class AdvCountReducer extends TableReducer<Text,Text,NullWritable> {

        public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
            //Hbase列族
            String family = "c";


            Integer pv = 0;
            Integer click = 0;
            Integer mac = 0;
            Integer ip = 0;


            for (Text val : values) {
                //logger.info("val:" + val);
                String[] advs = val.toString().split("\t");
                for (String adv : advs) {
                    //计算pv,click,mac,ip
                    if (adv.startsWith("ic:")) {
                        String ic = adv.substring(2, adv.length());
                        if(ic.equals("0"))
                            click ++;
                        else
                            pv ++;
                        //logger.info("ic:"+adv);
                    }
                    if (adv.startsWith("m:")) {
                        HashMap<String,String> macHashMap = new HashMap<>();
                        macHashMap.put(adv, "");
                        mac = macHashMap.size();
                    }
                    if (adv.startsWith("ip:")) {
                        HashMap<String,String> ipHashMap = new HashMap<>();
                        ipHashMap.put(adv,"");
                        ip = ipHashMap.size();
                    }

                }
            }

            //String rowkey = type+addAdidZero(key.toString()) + DateUtil.getDateBySimpleDateFormat("yyMMddHH");
            String rowkey = key.toString();
            Put put = new Put(rowkey.getBytes());
            //put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(data));
            //put.add(family.getBytes(), "pv".getBytes(), pv.toString().getBytes());

            put.addColumn(family.getBytes(), "p".getBytes(), pv.toString().getBytes());
            put.addColumn(family.getBytes(), "c".getBytes(), click.toString().getBytes());
            put.addColumn(family.getBytes(), "i".getBytes(), ip.toString().getBytes());
            put.addColumn(family.getBytes(), "m".getBytes(), mac.toString().getBytes());


            context.write(NullWritable.get(), put);

            String count = "adid:"+key+ ",p:" + pv + ",c:" + click + ",m:" + mac + ",i:" + ip;
            logger.info(count);
        }




    }




    public static boolean runLoadMapReducue(Configuration conf, Path input) throws IOException,
            ClassNotFoundException, InterruptedException {


        //設置Hbase的表名和其他屬性
        conf.set(TableOutputFormat.OUTPUT_TABLE, "adv");
        conf.set("hbase.zookeeper.quorum", "192.168.34.51");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        Job job = Job.getInstance(conf,"RcFileReaderJob");
        TableMapReduceUtil.addDependencyJars(job);
        job.setJarByClass(CountJob.class);


        //job.setNumReduceTasks(0);

        //mapper，reducer的处理逻辑类
        job.setMapperClass(RcFileMapper.class);
        //本地聚集
        //job.setCombinerClass(RcFileReduce.class);
        job.setReducerClass(AdvCountReducer.class);
        //job.setReducerClass(RcFileReduce.class);

//        MultipleInputs.addInputPath(job, input, RCFileInputFormat.class);

        //mapper的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        //輸入輸出文件format
        job.setInputFormatClass(RCFileMapReduceInputFormat.class);
        job.setOutputFormatClass(TableOutputFormat.class);

        //輸入輸出目標
        RCFileMapReduceInputFormat.addInputPath(job, input);
        //FileOutputFormat.setOutputPath(job, output);

        return job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HdfsDAO.config();
//        if (args.length != 2) {
//            System.err.println("Usage: rcfile <in> <out>");
//            System.exit(2);
//        }

        String date = DateUtil.getDate();
        String year = date.substring(2, 4);
        String month = date.substring(5, 7);
        String day = date.substring(8, 10);

        CountJob.runLoadMapReducue(conf, new Path("/tmp/kafka/advertall/"+year+"/"+month+"/"+day+"/rcall/part-r-00000"));

    }
}