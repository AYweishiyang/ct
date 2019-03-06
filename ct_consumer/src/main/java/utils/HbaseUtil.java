package utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.TreeSet;

public class HbaseUtil {
    /**
     * 判断HBase表是否存在
     *
     * @throws IOException
     */
    public static boolean isExistTable(Configuration conf, String tableName) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        boolean result = admin.tableExists(TableName.valueOf(tableName));

        admin.close();
        connection.close();

        return result;
    }

    /**
     * 初始化命名空间
     *
     * @param conf
     * @param namespace
     */
    public static void initNamespace(Configuration conf, String namespace) throws IOException {

        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        NamespaceDescriptor nd = NamespaceDescriptor.create(namespace)
                .addConfiguration("createTime", String.valueOf(System.currentTimeMillis()))
                .addConfiguration("author", "ay").build();
        admin.createNamespace(nd);

        admin.close();
        connection.close();
    }


    public static void createTable(Configuration conf, String tableName, int regions, String... columnFamily) throws IOException {

        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        if (isExistTable(conf, tableName)) return;

        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
        for (String cf : columnFamily) {
            htd.addFamily(new HColumnDescriptor(cf));
        }
        htd.addCoprocessor("hbase.CalleeWriteObserver");
        admin.createTable(htd, genSplitKeys(regions));
        admin.close();
        connection.close();
    }

    private static byte[][] genSplitKeys(int regions) {
        String[] keys = new String[regions];
        DecimalFormat df = new DecimalFormat("00");
        for (int i = 0; i < regions; i++) {
            keys[i] = df.format(i) + "|";

        }
        byte[][] splitKeys = new byte[regions][];

        TreeSet<byte[]> treeSet = new TreeSet<>(Bytes.BYTES_COMPARATOR);

        for (int i = 0; i < regions; i++) {
            treeSet.add(Bytes.toBytes(keys[i]));

        }
        Iterator<byte[]> splitKeysIterator = treeSet.iterator();
        int index = 0;
        while (splitKeysIterator.hasNext()) {
            byte[] b = splitKeysIterator.next();
            splitKeys[index ++] = b;
        }
        return splitKeys;
    }

    /**
     * @param regionCode
     * @param call1
     * @param buildTime
     * @param call2
     * @param flag
     * @param duration
     * @return
     */
    public static String genRowKey(String regionCode, String call1, String buildTime, String call2, String flag, String duration) {
        StringBuilder sb = new StringBuilder();
        sb.append(regionCode + "_")
                .append(call1 + "_")
                .append(buildTime + "_")
                .append(call2 + "_")
                .append(flag + "_")
                .append(duration);
        return sb.toString();

    }

    public static String genRegionCode(String call1, String buildTime, int regions){
        int len = call1.length();
        //取出后四位
        String lastPhone = call1.substring(len-4);

        String ym = buildTime
                .replaceAll("-","")
                .replaceAll(":","")
                .replaceAll(" ","")
                .substring(0,6);
        //离散操作1
        Integer x = Integer.valueOf(lastPhone) ^ Integer.valueOf(ym);
        //离散操作2
        int y = x.hashCode();
        int regionCode = y % regions;

        DecimalFormat df = new DecimalFormat("00");
        return df.format(regionCode);

    }
}
