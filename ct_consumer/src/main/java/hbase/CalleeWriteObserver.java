package hbase;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import utils.HbaseUtil;
import utils.PropertiesUtil;

import java.io.IOException;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * 遇到问题尝试重启
 */
public class CalleeWriteObserver extends BaseRegionObserver {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        super.postPut(e, put, edit, durability);
        //1、获取需要操作的表
        String targetTableName = PropertiesUtil.getProperty("hbase.calllog.tablename");
        //2、获取当前操作的表
        String currentTableName = e.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString();
        //3、判断需要操作的表是否就是当前表，如果不是，则return
        if (!StringUtils.equals(targetTableName, currentTableName)) return;
//        if(!targetTableName.equals(currentTableName)) return;
        //4、得到当前插入数据的值并封装新的数据，oriRowkey举例：01_15369468720_20170727081033_13720860202_1_0180
        String oriRowKey = Bytes.toString(put.getRow());
        String[] splitOriRowKey = oriRowKey.split("_");
        String flag = splitOriRowKey[4];
        //因为协处理器会重复调用put函数
        if(StringUtils.equals(flag, "0")) return;

       // String oldFlag = splitOriRowKey[4];
       // if(oldFlag.equals("0")) return;

        int regions = Integer.valueOf(PropertiesUtil.getProperty("hbase.calllog.regions"));
        String caller = splitOriRowKey[1];
        String callee = splitOriRowKey[3];
        String buildTime = splitOriRowKey[2];
        String newflag = "0";
        String duration = splitOriRowKey[5];
        String regionCode = HbaseUtil.genRegionCode(callee, buildTime, regions);

        //生成时间戳
        String buildTimeTs = "";
        try {
            buildTimeTs = String.valueOf(sdf.parse(buildTime).getTime());
        } catch (ParseException e1) {
            e1.printStackTrace();
        }
//        new SimpleDateFormat("yyyyMMddHHmmss").parse(buildTime).getTime()



        String calleeRowKey = HbaseUtil.genRowKey(regionCode, callee, buildTime, caller, flag, duration);
        Put calleePut = new Put(Bytes.toBytes(calleeRowKey));
        calleePut.addColumn(Bytes.toBytes("f2"),Bytes.toBytes("call1"),Bytes.toBytes(callee));
        calleePut.addColumn(Bytes.toBytes("f2"),Bytes.toBytes("call2"),Bytes.toBytes(caller));
        calleePut.addColumn(Bytes.toBytes("f2"),Bytes.toBytes("buildTime"),Bytes.toBytes(buildTime));
        calleePut.addColumn(Bytes.toBytes("f2"),Bytes.toBytes("flag"),Bytes.toBytes(newflag));
        calleePut.addColumn(Bytes.toBytes("f2"),Bytes.toBytes("duration"),Bytes.toBytes(duration));
        calleePut.addColumn(Bytes.toBytes("f2"),Bytes.toBytes("buildTime_ts"),Bytes.toBytes(buildTimeTs));


        Table table = e.getEnvironment().getTable(TableName.valueOf(targetTableName));
        table.put(calleePut);
        table.close();





    }
}

