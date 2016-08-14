package com.hzy;

import com.hzy.util.DateUtil;
import com.hzy.util.HdfsDAO;
import com.hzy.util.PropertiesUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.net.URI;
/**
 * Created by Hzy on 2016/7/27.
 * 文件追加
 */
public class AppendContent {
    private static final Logger logger = LogManager.getLogger(AppendContent.class);
    public static void main(String[] args)  throws IOException {
        HdfsDAO hdfsDAO = new HdfsDAO();


        Configuration conf = HdfsDAO.config();
        conf.setBoolean("dfs.support.append", true);
        conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");


        //获取上一个小时的时间,年份后2位
        String date = DateUtil.addHour(DateUtil.getDate(),Integer.parseInt(PropertiesUtil.getValue("AddHour")));
        String year = date.substring(2, 4);
        String month = date.substring(5, 7);
        String day = date.substring(8, 10);
        String hour = date.substring(11, 13);

        String appendWriteHdfsPath = PropertiesUtil.getValue("AppendWriteHdfsPath");
        String consumerSaveHdfsPath = PropertiesUtil.getValue("ConsumerSaveHdfsPath");

        //写入的文件目录
        String writeHdfsStr = appendWriteHdfsPath + year + "/" + month + "/" + day;

        //判断all是否存在，不存在就创建
        if (!hdfsDAO.exists(writeHdfsStr)) {
            hdfsDAO.mkdirs(writeHdfsStr);
            hdfsDAO.createFile(writeHdfsStr + "/all.txt", "");
        }

        Path loadHdfsFile = new Path(consumerSaveHdfsPath + year + "/" + month + "/" + day + "/" + hour + "/");
        Path writeHdfsDir = new Path(writeHdfsStr + "/all.txt");


        //被追加的文件
        FileSystem writeHdfs = FileSystem.get(writeHdfsDir.toUri(), conf);
        //需要追加的文件
        FileSystem loadHdfs = FileSystem.get(loadHdfsFile.toUri(), conf);

        try {
            if (loadHdfs.exists(loadHdfsFile)) {
                FileStatus[] inputFiles = loadHdfs.listStatus(loadHdfsFile);
                logger.info("inputFiles.length=" + inputFiles.length);
                FSDataOutputStream out = writeHdfs.append(writeHdfsDir);

                //追加文件
                for (int i = 0; i < inputFiles.length; i++) {
                    logger.info(inputFiles[i].getPath().toString());
                    if (!inputFiles[i].isDirectory()) {
                        FSDataInputStream in = loadHdfs.open(inputFiles[i].getPath());
                        byte buffer[] = new byte[1024];
                        int bytesRead = 0;
                        while ((bytesRead = in.read(buffer)) > 0) {
                            out.write(buffer, 0, bytesRead);
                        }
                        in.close();
                    }
                }
                //所有文件追加完成後，刪除
                for (int i = 0; i < inputFiles.length; i++) {
                    hdfsDAO.rmr(inputFiles[i].getPath().toString());
                }
                out.close();

            }


        } catch (IOException e) {
            e.printStackTrace();
        }





                /*單個文件追加*/
/*
        String hdfs_path = args[0];//文件路径
        String inpath = args[1];
        InputStream in = null;
        OutputStream out = null;
        try {
            FileSystem fs = FileSystem.get(URI.create(hdfs_path), conf);
            //要追加的文件流，inpath为文件
            if(inpath.startsWith("hdfs://"))
                in =  fs.open(new Path(inpath));
            else
                in = new BufferedInputStream(new FileInputStream(inpath));


            out = fs.append(new Path(hdfs_path));
            IOUtils.copyBytes(in, out, 4096, true);
            fs.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            IOUtils.closeStream(in);
            IOUtils.closeStream(out);
        }*/
    }
}