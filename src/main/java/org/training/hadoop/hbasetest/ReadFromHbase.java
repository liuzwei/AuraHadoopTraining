package org.training.hadoop.hbasetest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class ReadFromHbase {

    public static void readData(Configuration conf) throws Exception{
        Connection connection = ConnectionFactory.createConnection(conf);

        Table table = connection.getTable(TableName.valueOf(TableInformation.TABLE_NAME));
//        int userId = 2775779;
        int userId = 1786102;

        Scan scan = new Scan();
//        scan.setStartRow(Bytes.toBytes(String.format("%08d", userId)+"_20160509"));
//        scan.setStopRow(Bytes.toBytes(String.format("%08d", userId)+"_20160521"));
        scan.addFamily(Bytes.toBytes(TableInformation.COLUMN_FAMILY_2));
//        scan.addFamily(Bytes.toBytes(TableInformation.COLUMN_FAMILY_1));
        scan.setCaching(100);

        //增加过滤器
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(TableInformation.COLUMN_FAMILY_2), Bytes.toBytes(TableInformation.QUALIFIER_NAME_2_3), CompareFilter.CompareOp.EQUAL, Bytes.toBytes("1"));

        SingleColumnValueFilter onlineFilter = new SingleColumnValueFilter(Bytes.toBytes(TableInformation.COLUMN_FAMILY_2), Bytes.toBytes(TableInformation.QUALIFIER_NAME_2_2), CompareFilter.CompareOp.EQUAL, Bytes.toBytes("41306"));
        SingleColumnValueFilter onlineFilter2 = new SingleColumnValueFilter(Bytes.toBytes(TableInformation.COLUMN_FAMILY_2), Bytes.toBytes(TableInformation.QUALIFIER_NAME_2_6), CompareFilter.CompareOp.NOT_EQUAL, Bytes.toBytes("null"));

//        SingleColumnValueFilter offLinefilter = new SingleColumnValueFilter(Bytes.toBytes(TableInformation.COLUMN_FAMILY_1), Bytes.toBytes(TableInformation.QUALIFIER_NAME_1_6), CompareFilter.CompareOp.NOT_EQUAL, Bytes.toBytes("null"));
        list.addFilter(onlineFilter);
        list.addFilter(onlineFilter2);
        scan.setFilter(list);

        ResultScanner results = table.getScanner(scan);

        int sum =0;
        for (Result result : results) {
            if (result != null) {
                for (Cell cell : result.listCells()) {
                    System.out.println("QUALIFIER_NAME:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                    System.out.println("value:" + Bytes.toString(CellUtil.cloneValue(cell)));
                }
                sum +=1;
                System.out.println();
            }
        }
        System.out.println("发送线上优惠券总数："+sum);
        table.close();
        connection.close();
    }

    public static void main(String[] args) throws Exception {
        readData(TableInformation.getHbaseConfiguration());
    }
}
