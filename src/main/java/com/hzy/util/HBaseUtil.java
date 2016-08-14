package com.hzy.util;

/**
 * Created by Hzy on 2016/8/3.
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class HBaseUtil {
    private static final Logger logger = LogManager.getLogger(HBaseUtil.class);
    /**
     * HBASE 表名称
     */
    public static final String TABLE_NAME = "spider";
    /**
     * 列簇1 商品信息
     */
    public static final String COLUMNFAMILY_1 = "goodsinfo";
    /**
     * 列簇1中的列
     */
    public static final String COLUMNFAMILY_1_DATA_URL = "data_url";
    public static final String COLUMNFAMILY_1_PIC_URL = "pic_url";
    public static final String COLUMNFAMILY_1_TITLE = "title";
    public static final String COLUMNFAMILY_1_PRICE = "price";
    /**
     * 列簇2 商品规格
     */
    public static final String COLUMNFAMILY_2 = "spec";
    public static final String COLUMNFAMILY_2_PARAM = "param";


    HBaseAdmin admin=null;
    Configuration conf=null;
    /**
     * 构造函数加载配置
     */
    public HBaseUtil(){
        conf = new Configuration();
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.34.51");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            admin = new HBaseAdmin(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) throws Exception {
        HBaseUtil hbase = new HBaseUtil();
        //创建一张表
		//String[] columns = {"count","attr"};
		//hbase.createTable("adv",columns);

        int maxsize = 5;

		//往表中添加一条记录
//        for(int i=0;i<1;i++){
//            String adid = Integer.toString(i);
//            int idlan = adid.length();
//            int addlan =  maxsize-idlan;
//            switch (addlan){
//                case 1 :
//                    adid ="0"+ adid;
//                    break;
//                case 2 :
//                    adid ="00"+ adid;
//                    break;
//                case 3 :
//                    adid ="000"+ adid;
//                    break;
//                case 4 :
//                    adid ="0000"+ adid;
//                    break;
//                default :
//                    break;
//
//            }
//            hbase.putRecord("adv",adid+"16080310","count","mac","1000");
//            hbase.putRecord("adv",adid+"16080310","count","ip","1000");
//            hbase.putRecord("adv",adid+"16080310","count","pv","1000");
//            hbase.putRecord("adv",adid+"16080310","count","click","1000");
//            hbase.putRecord("adv",adid+"16080310","attr","place","160803");
//        }
//


        hbase.putRecord("adv","00000431"+"16080310","count","mac","10000");
        hbase.putRecord("adv","00000431"+"16080210","count","mac","10000");
        hbase.putRecord("adv","00000431"+"16080110","count","mac","10000");
        hbase.putRecord("adv","00000431"+"16080410","count","mac","10000");
//        hbase.putRecord("adv","999"+"16080310"+"002","count","mac","10000");
//        hbase.putRecord("adv","323"+"16080310"+"001"+"3302","count","mac","22222");
//        hbase.putRecord("adv","545"+"16080310"+"002"+"3302","count","mac","22222");


//		//删除一条记录
//		hbase.deleteOneRecord("hbaseTest1","key1");

//		//删除多条记录
//		String[] rowKeys = {"key1","key2"};
//		hbase.deleteMultipleRecord("test1",rowKeys);

        //查询所有表名
//		hbase.getALLTable();

//		//查询一条记录
//		hbase.getRecord("spider", "jd_967021");

        //rowkey范围查询,scan过滤器的使用
		hbase.Scan("adv", "0000043116080111", "0000043116080111");

        //rowkey范围查询,过滤某些列,scan过滤器的使用
//		String[] columnName = {"title","price"};
//		hbase.Scan("spider", "jd_875722 ", "jd_979281 ", "goodsinfo",columnName );

        //条件查询，指定列与值,scan过滤器的使用
//		String[] columnName = {"title","price"};
//		hbase.Query("spider",  "goodsinfo",columnName,"price","1999.00" );

        //根据rowkey模糊查询,scan过滤器的使用
//		String[] columnName = {"mac","a3302","3302mac"};
//		hbase.QueryByRowKey("adv", "00001_*", "count", columnName);


        //获取表的所有数据
    //    hbase.getALLData("adv");

//		//删除表
//		hbase.deleteTable("stu");

        //要是多条件查询就要设置多个filter和一个filter的集合
    }


    public void deleteTable(String tableName) {
        try {
            if (admin.tableExists(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
                logger.info(tableName + "表删除成功！");
            }
        } catch (IOException e) {
            e.printStackTrace();
            logger.info(tableName + "表删除失败！");
        }

    }
    /**
     * 删除一条记录
     * @param tableName
     * @param rowKey
     */
    public void deleteOneRecord(String tableName, String rowKey) {
        HConnection connection = null;
        HTableInterface table = null;
        try {
            connection = HConnectionManager.createConnection(conf);
            table = connection.getTable(tableName);
            Delete delete = new Delete(rowKey.getBytes());
            table.delete(delete);
            logger.info(rowKey + "记录删除成功！");
        } catch (IOException e) {
            e.printStackTrace();
            logger.info(rowKey + "记录删除失败！");
        }finally{
            try {
                table.close();
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    /**
     * 删除多条记录
     * @param tableName
     * @param rowKeys
     */
    public void deleteMultipleRecord(String tableName, String[] rowKeys){
        HConnection connection = null;
        HTableInterface table = null;
        List<Delete> deleteList = null;
        try {
            connection = HConnectionManager.createConnection(conf);
            table = connection.getTable(tableName);
            deleteList = new ArrayList<Delete>();
            for(String rowKey:rowKeys){
                Delete delete = new Delete(rowKey.getBytes());
                deleteList.add(delete);
            }
            table.delete(deleteList);
            logger.info("记录删除成功！");
        } catch (IOException e) {
            e.printStackTrace();
            logger.info("记录删除失败！");
        }finally{
            try {
                table.close();
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    /**
     * 获取表的所有数据
     * @param tableName
     */
    public void getALLData(String tableName) {
        HConnection connection = null;
        HTableInterface table = null;
        Scan scan = null;
        Cell[] cells = null;
        try {
            connection = HConnectionManager.createConnection(conf);
            table = connection.getTable(tableName);
            scan = new Scan();
            ResultScanner scanner = table.getScanner(scan);
            int num=0;
            for(Result result : scanner){
                num++;
                cells = result.rawCells();
                for(Cell cell :cells){
                    System.out.print("行健: "+new String(CellUtil.cloneRow(cell)));
                    System.out.print(" 列簇: "+new String(CellUtil.cloneFamily(cell)));
                    System.out.print(" 列: "+new String(CellUtil.cloneQualifier(cell)));
                    System.out.print(" 值: " + new String(CellUtil.cloneValue(cell)));
                    logger.info(" 时间戳: " + cell.getTimestamp());

                }
            }
            logger.info("总共" + num + "条记录");
        } catch (IOException e) {
            e.printStackTrace();
            logger.info("查询记录失败！");
        }finally{
            try {
                table.close();
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    /**
     * 获取一条记录
     * @param tableName
     * @param rowKey
     */
    public void getRecord(String tableName,String rowKey){
        HConnection connection = null;
        HTableInterface table = null;
        try {
            connection = HConnectionManager.createConnection(conf);
            table = connection.getTable(tableName);
            Get get = new Get(rowKey.getBytes());
            Result result = table.get(get);
            Cell[] cells = result.rawCells();
            for(Cell cell :cells){
                System.out.print("行健: "+new String(CellUtil.cloneRow(cell)));
                System.out.print(" 列簇: "+new String(CellUtil.cloneFamily(cell)));
                System.out.print(" 列: "+new String(CellUtil.cloneQualifier(cell)));
                System.out.print(" 值: "+new String(CellUtil.cloneValue(cell)));
                logger.info(" 时间戳: " + cell.getTimestamp());
            }
        } catch (IOException e) {
            e.printStackTrace();
            logger.info("获取记录失败！");
        }finally{
            try {
                table.close();
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 添加一条记录
     * @param tableName
     * @param rowKey
     */
    public  void putRecord(String tableName, String rowKey, String columnFamily,
                           String column, String data) throws IOException {
        HConnection connection = null;
        HTableInterface table = null;
        try {
            connection = HConnectionManager.createConnection(conf);
            table = connection.getTable(tableName);
            Put p1 = new Put(Bytes.toBytes(rowKey));
            p1.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column),
                    Bytes.toBytes(data));
            table.put(p1);
            logger.info(tableName + "添加记录成功:" + "put'" + rowKey + "'," + columnFamily + ":" + column
                    + "','" + data + "'");
        } catch (IOException e) {
            e.printStackTrace();
            logger.info(rowKey + "添加记录失败！");
        }finally{
            try {
                table.close();
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    /**
     * 范围查询
     * @param tableName
     * @param startRow
     * @param stopRow
     */
    public void Scan(String tableName,String startRow,String stopRow){
        HConnection connection = null;
        HTableInterface table = null;
        Scan scan = null;
        Cell[] cells = null;
        try {
            connection = HConnectionManager.createConnection(conf);
            table = connection.getTable(tableName);
            scan = new Scan();
            scan.setStartRow(startRow.getBytes());
            scan.setStopRow(stopRow.getBytes());
            ResultScanner scanner = table.getScanner(scan);
            for(Result result : scanner){
                cells = result.rawCells();
                for(Cell cell :cells){
                    System.out.print("行健: "+new String(CellUtil.cloneRow(cell)));
                    System.out.print(" 列簇: "+new String(CellUtil.cloneFamily(cell)));
                    System.out.print(" 列: "+new String(CellUtil.cloneQualifier(cell)));
                    System.out.print(" 值: " + new String(CellUtil.cloneValue(cell)));
                    logger.info(" 时间戳: " + cell.getTimestamp());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            logger.info("查询记录失败！");
        }finally{
            try {
                table.close();
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 范围查询
     * @param tableName
     * @param startRow
     * @param stopRow
     */
    public void Scan(String tableName,String startRow,String stopRow,String columnFamily,String[] columnName){
        HConnection connection = null;
        HTableInterface table = null;
        Scan scan = null;
        Cell[] cells = null;
        try {
            connection = HConnectionManager.createConnection(conf);
            table = connection.getTable(tableName);
            scan = new Scan();
            for(String name : columnName){
                scan.addColumn(columnFamily.getBytes(), name.getBytes());
            }
            scan.setStartRow(startRow.getBytes());
            scan.setStopRow(stopRow.getBytes());
            ResultScanner scanner = table.getScanner(scan);
            for(Result result : scanner){
                cells = result.rawCells();
                for(Cell cell :cells){
                    System.out.print("行健: "+new String(CellUtil.cloneRow(cell)));
                    System.out.print(" 列簇: "+new String(CellUtil.cloneFamily(cell)));
                    System.out.print(" 列: "+new String(CellUtil.cloneQualifier(cell)));
                    System.out.print(" 值: " + new String(CellUtil.cloneValue(cell)));
                    logger.info(" 时间戳: " + cell.getTimestamp());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            logger.info("查询记录失败！");
        }finally{
            try {
                table.close();
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 条件查询
     * @param tableName
     * @param columnFamily
     * @param columnName
     * @param queryColumnName
     * @param queryColumnValue
     */
    public void Query(String tableName,String columnFamily,String[] columnName,String queryColumnName,String queryColumnValue){
        HConnection connection = null;
        HTableInterface table = null;
        Scan scan = null;
        Cell[] cells = null;
        try {
            connection = HConnectionManager.createConnection(conf);
            table = connection.getTable(tableName);
            scan = new Scan();
            for(String name : columnName){
                scan.addColumn(columnFamily.getBytes(), name.getBytes());
            }
            Filter filter = new SingleColumnValueFilter(columnFamily.getBytes(), queryColumnName.getBytes(), CompareOp.EQUAL, queryColumnValue.getBytes());
            scan.setFilter(filter);
            ResultScanner scanner = table.getScanner(scan);
            for(Result result : scanner){
                cells = result.rawCells();
                for(Cell cell :cells){
                    System.out.print("行健: "+new String(CellUtil.cloneRow(cell)));
                    System.out.print(" 列簇: "+new String(CellUtil.cloneFamily(cell)));
                    System.out.print(" 列: "+new String(CellUtil.cloneQualifier(cell)));
                    System.out.print(" 值: " + new String(CellUtil.cloneValue(cell)));
                    logger.info(" 时间戳: " + cell.getTimestamp());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            logger.info("查询记录失败！");
        }finally{
            try {
                table.close();
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * 条件查询
     * @param tableName
     * @param rowKeyRegex
     */
    public void QueryByRowKey(String tableName,String rowKeyRegex,String columnFamily,String[] columnName){
        HConnection connection = null;
        HTableInterface table = null;
        Scan scan = null;
        Cell[] cells = null;
        try {
            connection = HConnectionManager.createConnection(conf);
            table = connection.getTable(tableName);
            scan = new Scan();
            for(String name : columnName){
                scan.addColumn(columnFamily.getBytes(), name.getBytes());
            }
            Filter filter =	new RowFilter(CompareOp.EQUAL, new RegexStringComparator(rowKeyRegex));
            scan.setFilter(filter);
            ResultScanner scanner = table.getScanner(scan);
            for(Result result : scanner){
                cells = result.rawCells();
                for(Cell cell :cells){
                    System.out.print("行健: "+new String(CellUtil.cloneRow(cell)));
                    System.out.print(" 列簇: "+new String(CellUtil.cloneFamily(cell)));
                    System.out.print(" 列: "+new String(CellUtil.cloneQualifier(cell)));
                    System.out.print(" 值: " + new String(CellUtil.cloneValue(cell)));
                    logger.info(" 时间戳: " + cell.getTimestamp());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            logger.info("查询记录失败！");
        }finally{
            try {
                table.close();
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 查询所有表名
     * @return
     * @throws Exception
     */
    public List<String> getALLTable() throws Exception {
        ArrayList<String> tables = new ArrayList<String>();
        if(admin!=null){
            HTableDescriptor[] listTables = admin.listTables();
            if (listTables.length>0) {
                for (HTableDescriptor tableDesc : listTables) {
                    tables.add(tableDesc.getNameAsString());
                    logger.info(tableDesc.getNameAsString());
                }
            }
        }
        return tables;
    }
    /**
     * 创建一张表
     * @param tableName
     * @param columns
     * @throws Exception
     */
    public void createTable(String tableName, String[] columns) throws Exception {
        if(admin.tableExists(tableName)){
            logger.info(tableName + "表已经存在！先删除...");
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            logger.info(tableName + "删除成功!");

        }else{
            HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
            for(String column :columns){
                tableDesc.addFamily(new HColumnDescriptor(column.getBytes()));
            }
            admin.createTable(tableDesc);
            logger.info(tableName + "表创建成功！");
        }
        admin.close();
    }

    // select ... from so where end_user_id=? and order_status = in (?,?)
//	public static void Query3(String tableName, String end_user_id, String order_status1,String order_status2) throws Exception
//	{
//		HTable table = new HTable(configuration, tableName) ;
//		Scan scan = new Scan();
//		scan.addColumn("o".getBytes(), "ID".getBytes()) ;
//		scan.addColumn("o".getBytes(), "END_USER_ID".getBytes()) ;
//		scan.addColumn("o".getBytes(), "ORDER_STATUS".getBytes()) ;
//
//		SingleColumnValueFilter filter1 = new SingleColumnValueFilter("o".getBytes(),"END_USER_ID".getBytes(), CompareFilter.CompareOp.EQUAL,end_user_id.getBytes());
//
//		SingleColumnValueFilter filter2 = new SingleColumnValueFilter("o".getBytes(),"ORDER_STATUS".getBytes(), CompareFilter.CompareOp.EQUAL,order_status1.getBytes());
//		SingleColumnValueFilter filter3 = new SingleColumnValueFilter("o".getBytes(),"ORDER_STATUS".getBytes(), CompareFilter.CompareOp.EQUAL,order_status2.getBytes());
//
//		FilterList filterAll = new FilterList();
//		filterAll.addFilter(filter1) ;
//
//		FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
//		filterList.addFilter(filter2) ;
//		filterList.addFilter(filter3) ;
//
//		filterAll.addFilter(filterList);
//		scan.setFilter(filterAll);
//
//		ResultScanner scanner = table.getScanner(scan);
//		for(Result result : scanner)
//		{
//			logger.info("rowKey:"+new String(result.getRow()));
//			for(KeyValue keyValue : result.raw())
//			{
//				logger.info(new String(keyValue.getFamily())+":"+new String(keyValue.getQualifier())+"="+new String(keyValue.getValue()));
//			}
//			logger.info();
//		}
//
//	}
//	// 用普通字段查询
//	public static void Query4(String tableName, String so_id) throws Exception
//	{
//		String rowKeyRegex = StringUtils.reverse(so_id)+"_" ;
//		HTable table = new HTable(configuration, tableName) ;
////		RowFilter filter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator(rowKeyRegex)) ;
//		PrefixFilter filter = new PrefixFilter(rowKeyRegex.getBytes());
//		Scan scan = new Scan();
//		scan.addColumn("d".getBytes(), "ID".getBytes()) ;
//		scan.addColumn("d".getBytes(), "ORDER_ID".getBytes()) ;
//		scan.addColumn("d".getBytes(), "ORDER_ITEM_AMOUNT".getBytes()) ;
//		scan.addColumn("d".getBytes(), "ORDER_ITEM_NUM".getBytes()) ;
//		scan.setFilter(filter);
//
//		ResultScanner scanner = table.getScanner(scan);
//		for(Result result : scanner)
//		{
//			logger.info("rowKey:"+new String(result.getRow()));
//			for(KeyValue keyValue : result.raw())
//			{
//				logger.info(new String(keyValue.getFamily())+":"+new String(keyValue.getQualifier())+"="+new String(keyValue.getValue()));
//			}
//			logger.info();
//		}
//
//	}
//
//
//
//	    // 读取一条记录
//		@SuppressWarnings({ "deprecation", "resource" })
//		public Goods get(String tableName, String row) {
//			HTablePool hTablePool = new HTablePool(conf, 1000);
//			HTableInterface table = hTablePool.getTable(tableName);
//			Get get = new Get(row.getBytes());
//			Goods goods = null;
//			try {
//				Result result = table.get(get);
//				KeyValue[] raw = result.raw();
//				if (raw.length == 5) {
//					goods = new Goods();
//					goods.setId(row);
//					goods.setDataurl(new String(raw[0].getValue()));
//					goods.setPicpath(new String(raw[1].getValue()));
//					String str = new String(raw[2].getValue());
//					if (str == null || "".equals(str)) {
//						str = "0.00";
//					}
//					goods.setPrice(Float.parseFloat(str));
//					goods.setName(new String(raw[3].getValue(),"GBK"));
//					goods.setSpec(new String(raw[4].getValue(),"UTF-8"));
//				}
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//			return goods;
//		}
}