package com.hzy.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URI;

/**
 * Created by Hzy on 2016/4/1.
 */
public class HdfsDAO {
    private static final Logger logger = LogManager.getLogger(HdfsDAO.class);
    public HdfsDAO() {
        this.conf =  config();
    }

    //hdfs路径
    //private String hdfsPath;
    //Hadoop系统配置
    private Configuration conf =  config();

    //启动函数
    public static void main(String[] args) throws IOException {
        HdfsDAO hdfs = new HdfsDAO();
        hdfs.mkdirs("/tmp/kafka/advert");
        hdfs.ls("/tmp/kafka/advert");
        hdfs.exists("/tmp/kafka/advert");
//        hdfs.upload("./t1.txt", "/tmp/kafka/advert");
//        hdfs.download("/tmp/kafka/test.txt", "./t2.txt");
//        hdfs.createFile("/tmp/kafka/advert/002.txt", "11111");
//        hdfs.cat("/tmp/kafka/advert/002.txt");
//        hdfs.getBlockInfo("/tmp/kafka/advert/002.txt");
//        hdfs.getHDFSNode();
//        hdfs.rmr("/tmp/kafka/advert");
//        hdfs.getFileSingle("/tmp/kafka/advert/");
//        hdfs.getFileMulti("/tmp/kafka/advert/");

    }

    //加载Hadoop配置文件
    public static Configuration config(){
        Configuration conf = new Configuration();
        conf.addResource("hadoop/hdfs-site.xml");
        conf.addResource("hadoop/core-site.xml");
        conf.addResource("hadoop/mapred-site.xml");

        //logger.info(conf.get("fs.default.name"));
        //logger.info(conf.get("hadoop.tmp.dir"));
        //logger.info(conf.get("mapred.child.tmp"));

        return conf;
    }

    public void ls(String folder) throws IOException {
        Path path = new Path(folder);
        String cc = conf.get("fs.default.name");
        logger.info("cc: " + cc);

        FileSystem fs = FileSystem.get(URI.create(folder),conf);
        FileStatus[] list = fs.listStatus(path);
        logger.info("ls: " + folder);
        logger.info("==========================================================");
        for (FileStatus f : list) {
            System.out.printf("name: %s, folder: %s, size: %d\n", f.getPath(), f.isDirectory(), f.getLen());
        }
        logger.info("==========================================================");
        fs.close();
    }

    public void mkdirs(String folder) throws IOException {
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(URI.create(folder),conf);
        if (!fs.exists(path)) {
            fs.mkdirs(path);
            logger.info("Create: " + folder);
        }
        fs.close();
    }

    public Boolean exists(String file) throws IOException {
        Path path = new Path(file);
        FileSystem fs = FileSystem.get(URI.create(file),conf);
        Boolean bool = fs.exists(path);
        fs.close();
        return bool;
    }

    public Boolean isDir(String file) throws IOException {
        Path path = new Path(file);
        FileSystem fs = FileSystem.get(URI.create(file),conf);
        Boolean bool = fs.getFileStatus(path).isDir();
        fs.close();
        return bool;
    }


    public void rmr(String folder) throws IOException {
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(URI.create(folder),conf);
        fs.deleteOnExit(path);
        logger.info("Delete: " + folder);
        fs.close();
    }

    public void rmr(Path path) throws IOException {
        //Path path = new Path(folder);
        FileSystem fs = FileSystem.get(path.toUri(),conf);
        fs.deleteOnExit(path);
        logger.info("Delete: " + path.toUri().getPath());
        fs.close();
    }

    public void upload(String local, String remote) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(remote),conf);
        fs.copyFromLocalFile(new Path(local), new Path(remote));
        logger.info("copy from: " + local + " to " + remote);
        fs.close();
    }

//    public void copyFile(String local, String remote) throws IOException {
//        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
//        fs.copyToLocalFile(new Path(local), new Path(remote));
//        logger.info("copy from: " + local + " to " + remote);
//        fs.close();
//    }


    public void cat(String remoteFile) throws IOException {
        Path path = new Path(remoteFile);
        FileSystem fs = FileSystem.get(URI.create(remoteFile),conf);
        FSDataInputStream fsdis = null;
        logger.info("cat: " + remoteFile);
        try {
            fsdis =fs.open(path);
            IOUtils.copyBytes(fsdis, System.out, 4096, false);
        } finally {
            IOUtils.closeStream(fsdis);
            fs.close();
        }
    }


    public void download(String remote, String local) throws IOException {
        Path path = new Path(remote);
        FileSystem fs = FileSystem.get(URI.create(remote),conf);
        fs.copyToLocalFile(path, new Path(local));
        logger.info("download: from" + remote + " to " + local);
        fs.close();
    }

    public void createFile(String file, String content) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(file),conf);
        byte[] buff = content.getBytes();
        FSDataOutputStream os = null;
        try {


            os = fs.create(new Path(file));
            os.write(buff, 0, buff.length);
            logger.info("Create: " + file);
        } finally {
            if (os != null)
                os.close();
        }
        fs.close();
    }

    /**
     * 重名名文件夹或者文件
     * @param path
     * @param renamePath
     * @return
     * @throws IOException
     */
    public boolean renameDir(Path path,Path renamePath)throws IOException{
        FileSystem fs = FileSystem.get(path.toUri(),conf);
        Boolean bool = fs.rename(path, renamePath);
        fs.close();
        return bool;
    }

    /**
     * 查看某个文件夹下面的所有文件,只有一层
     * @param folder
     * @throws IOException
     */
    public  FileStatus[] getFileSingle(String folder) throws IOException {
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(URI.create(folder),conf);
        FileStatus[] files = fs.listStatus(path);
//        for (FileStatus file : files) {
//            logger.info(file.getPath().toString());
//        }
        fs.close();
        return files;
    }

    /**
     * 获取给定目录下的所有子目录以及子文件
     * @param folder
     * @throws IOException
     */
    public void getFileMulti(String folder) throws IOException {
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(URI.create(folder),conf);
        FileStatus[] files = fs.listStatus(path);
        for(int i=0;i<files.length;i++){
            if(files[i].isDirectory()){
                Path p = new Path(files[i].getPath().toString());
                getFileMulti(p.toString());
            }else{
                logger.info(files[i].getPath().toString());
            }
        }
        fs.close();
    }

    /**
     * 查看某个文件的数据块信息
     * @param folder
     * @throws Exception
     */
    public void getBlockInfo(String folder)throws IOException{
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(URI.create(folder),conf);
        FileStatus filestatus = fs.getFileStatus(path);
        BlockLocation[] blkLoc = fs.getFileBlockLocations
                (filestatus, 0, filestatus.getLen());
        for (BlockLocation loc : blkLoc) {
            for (int i = 0; i < loc.getHosts().length; i++) {
                //获取数据块在哪些主机上
                logger.info(loc.getHosts()[i]);//获取文件块的主机名
                //由于这个文件只有一个块，所以输出结果为:slave2、slave1、slave5
            }
        }
        fs.close();
    }


    /**
     * HDFS集群上所有节点名称信息
     * @Title: aaa
     * @Description: bbb
     * @return
     * @throws
     */
    public void getHDFSNode() throws IOException{
        FileSystem fs = FileSystem.get(conf);
        DistributedFileSystem dfs = (DistributedFileSystem)fs;
        DatanodeInfo[] dataNodeStats = dfs.getDataNodeStats();

        for(int i=0;i<dataNodeStats.length;i++){
            logger.info("DataNode_" + i + "_Node:" + dataNodeStats[i].getHostName());
        }
        fs.close();
    }


}
