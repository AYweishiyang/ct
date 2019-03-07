package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import utils.ConnectionInstance;
import utils.HbaseUtil;
import utils.PropertiesUtil;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class HBaseDAO {
    private int regions;
    private String namespace;
    private String tableName;
    public static final Configuration conf;
    private HTable table;
    private Connection connection;
    private SimpleDateFormat sdf1 = new SimpleDateFormat("yy-MM-dd HH:mm:ss");
    private SimpleDateFormat sdf2 = new SimpleDateFormat("yyMMddHHmmss");
    //缓存多条put
    private List<Put> cacheList = new ArrayList<>();

    static {
        conf = HBaseConfiguration.create();
    }

    /**
     * hbase.calllog.regions=6
     * hbase.calllog.namespace=ns_ct
     * hbase.calllog.tablename=ns_ct:calllog
     */
    public HBaseDAO() {
        try {

            regions = Integer.valueOf(PropertiesUtil.getProperty("hbase.calllog.regions"));
            namespace = PropertiesUtil.getProperty("hbase.calllog.namespace");
            tableName = PropertiesUtil.getProperty("hbase.calllog.tablename");


            if (!HbaseUtil.isExistTable(conf, tableName)) {
                HbaseUtil.initNamespace(conf, namespace);
                HbaseUtil.createTable(conf, tableName, regions, "f1","f2");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * ori格式: 15596505995,17519874292,2017-03-11 00:30:19,0652
     * rowKey格式: 01_15596505995_20170311003019_17519874292_1_0652
     * HBase表的列： call1,call2,buid_Time,build_Time_ts,flag,duration
     * @param ori
     */
    public void put(String ori) {

        try {

            if(cacheList.size() == 0){
                connection = ConnectionInstance.getConnection(conf);
                table = (HTable) connection.getTable(TableName.valueOf(tableName));
                table.setAutoFlushTo(false);

                table.setWriteBufferSize(2 * 1024 * 1024);
            }



            String[] splits = ori.split(",");

            String callee = splits[0];
            String caller = splits[1];
            String buildTime = splits[2];
            String duration = splits[3];
            String regionCode = HbaseUtil.genRegionCode(caller, buildTime, regions);
            String buildTimeReplace = sdf2.format(sdf1.parse(buildTime));
            String buildTimeTs = String.valueOf(sdf1.parse(buildTime).getTime());
            //生成rowKey
            String rowkey =HbaseUtil.genRowKey(regionCode, caller, buildTimeReplace, callee, "1", duration);
            //向表中插入数据
            Put put = new Put(Bytes.toBytes(rowkey));
            put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("call1"),Bytes.toBytes(caller));
            put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("call2"),Bytes.toBytes(callee));
            put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("buid_Time"),Bytes.toBytes(buildTime));
            put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("build_Time_ts"),Bytes.toBytes(buildTimeTs));
            put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("flag"),Bytes.toBytes("1"));
            put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("duration"),Bytes.toBytes(duration));

            cacheList.add(put);

            if(cacheList.size() >= 30){
                table.put(cacheList);
                table.flushCommits();

                table.close();
                cacheList.clear();
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }


    }
}
