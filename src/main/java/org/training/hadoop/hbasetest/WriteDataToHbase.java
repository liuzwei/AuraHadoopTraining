package org.training.hadoop.hbasetest;

import com.csvreader.CsvReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class WriteDataToHbase {

    public static List<String[]> readCsvData(String filePath){

        try {
            ArrayList<String[]> csvList = new ArrayList<String[]>();
            CsvReader reader = new CsvReader(filePath,',', Charset.forName("GBK"));
//          reader.readHeaders(); //跳过表头,不跳可以注释掉

            while(reader.readRecord()){
                csvList.add(reader.getValues()); //按行读取，并把每一行的数据添加到list集合
            }
            reader.close();
            System.out.println("读取的行数："+csvList.size());
            return csvList;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static String getRandom(){
        int random = new Random().nextInt(1000);
        return String.format("%03d", random);
    }

    public static void asyncOfflineBatchPut(List<String[]> offlineDataList,Configuration conf) throws IOException {
        //Connection to the cluster.
        Connection connection = null;
        //a async batch handler
        BufferedMutator bufferedMutator = null;

        //establish the connection to the cluster.
        try {
            connection = ConnectionFactory.createConnection(conf);
            bufferedMutator = connection.getBufferedMutator(TableName.valueOf(org.training.hadoop.hbasetest.TableInformation.TABLE_NAME));
            //describe the data
            for (int i = 0; i < offlineDataList.size(); i++) {
                String[] item = offlineDataList.get(i);
                String rowkey = String.format("%08d", Integer.parseInt(item[0]))+"_"+item[6]+"_"+getRandom();

                Put put = new Put(Bytes.toBytes(rowkey));

                put.addColumn(Bytes.toBytes(org.training.hadoop.hbasetest.TableInformation.COLUMN_FAMILY_1), Bytes.toBytes(org.training.hadoop.hbasetest.TableInformation.QUALIFIER_NAME_1_1), Bytes.toBytes(item[0]));
                put.addColumn(Bytes.toBytes(org.training.hadoop.hbasetest.TableInformation.COLUMN_FAMILY_1), Bytes.toBytes(org.training.hadoop.hbasetest.TableInformation.QUALIFIER_NAME_1_2), Bytes.toBytes(item[1]));
                put.addColumn(Bytes.toBytes(org.training.hadoop.hbasetest.TableInformation.COLUMN_FAMILY_1), Bytes.toBytes(org.training.hadoop.hbasetest.TableInformation.QUALIFIER_NAME_1_3), Bytes.toBytes(item[2]));
                put.addColumn(Bytes.toBytes(org.training.hadoop.hbasetest.TableInformation.COLUMN_FAMILY_1), Bytes.toBytes(org.training.hadoop.hbasetest.TableInformation.QUALIFIER_NAME_1_4), Bytes.toBytes(item[3]));
                put.addColumn(Bytes.toBytes(org.training.hadoop.hbasetest.TableInformation.COLUMN_FAMILY_1), Bytes.toBytes(org.training.hadoop.hbasetest.TableInformation.QUALIFIER_NAME_1_5), Bytes.toBytes(item[4]));
                put.addColumn(Bytes.toBytes(org.training.hadoop.hbasetest.TableInformation.COLUMN_FAMILY_1), Bytes.toBytes(org.training.hadoop.hbasetest.TableInformation.QUALIFIER_NAME_1_6), Bytes.toBytes(item[5]));
                put.addColumn(Bytes.toBytes(org.training.hadoop.hbasetest.TableInformation.COLUMN_FAMILY_1), Bytes.toBytes(org.training.hadoop.hbasetest.TableInformation.QUALIFIER_NAME_1_7), Bytes.toBytes(item[6]));
                //add data to buffer
                bufferedMutator.mutate(put);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            //close
            if (bufferedMutator != null) bufferedMutator.close();
            if (connection != null) connection.close();
        }
    }

    public static void asyncOnlineBatchPut(List<String[]> onlineDataList,Configuration conf) throws IOException {
        //Connection to the cluster.
        Connection connection = null;
        //a async batch handler
        BufferedMutator bufferedMutator = null;

        //establish the connection to the cluster.
        try {
            connection = ConnectionFactory.createConnection(conf);
            bufferedMutator = connection.getBufferedMutator(TableName.valueOf(org.training.hadoop.hbasetest.TableInformation.TABLE_NAME));
            //describe the data
            for (int i = 0; i < onlineDataList.size(); i++) {
                String[] item = onlineDataList.get(i);
                String rowkey = String.format("%08d", Integer.parseInt(item[0]))+"_"+item[6]+"_"+getRandom();

                Put put = new Put(Bytes.toBytes(rowkey));

                put.addColumn(Bytes.toBytes(org.training.hadoop.hbasetest.TableInformation.COLUMN_FAMILY_2), Bytes.toBytes(org.training.hadoop.hbasetest.TableInformation.QUALIFIER_NAME_2_1), Bytes.toBytes(item[0]));
                put.addColumn(Bytes.toBytes(org.training.hadoop.hbasetest.TableInformation.COLUMN_FAMILY_2), Bytes.toBytes(org.training.hadoop.hbasetest.TableInformation.QUALIFIER_NAME_2_2), Bytes.toBytes(item[1]));
                put.addColumn(Bytes.toBytes(org.training.hadoop.hbasetest.TableInformation.COLUMN_FAMILY_2), Bytes.toBytes(org.training.hadoop.hbasetest.TableInformation.QUALIFIER_NAME_2_3), Bytes.toBytes(item[2]));
                put.addColumn(Bytes.toBytes(org.training.hadoop.hbasetest.TableInformation.COLUMN_FAMILY_2), Bytes.toBytes(org.training.hadoop.hbasetest.TableInformation.QUALIFIER_NAME_2_4), Bytes.toBytes(item[3]));
                put.addColumn(Bytes.toBytes(org.training.hadoop.hbasetest.TableInformation.COLUMN_FAMILY_2), Bytes.toBytes(org.training.hadoop.hbasetest.TableInformation.QUALIFIER_NAME_2_5), Bytes.toBytes(item[4]));
                put.addColumn(Bytes.toBytes(org.training.hadoop.hbasetest.TableInformation.COLUMN_FAMILY_2), Bytes.toBytes(org.training.hadoop.hbasetest.TableInformation.QUALIFIER_NAME_2_6), Bytes.toBytes(item[5]));
                put.addColumn(Bytes.toBytes(org.training.hadoop.hbasetest.TableInformation.COLUMN_FAMILY_2), Bytes.toBytes(org.training.hadoop.hbasetest.TableInformation.QUALIFIER_NAME_2_7), Bytes.toBytes(item[6]));
                //add data to buffer
                bufferedMutator.mutate(put);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            //close
            if (bufferedMutator != null) bufferedMutator.close();
            if (connection != null) connection.close();
        }
    }



    public static void main(String[] args) throws Exception{
        String offlineFilePath = "/Users/liuzhaowei/Downloads/o2o_offline_data_sample.csv";
        List<String[]> offlineList = readCsvData(offlineFilePath);

        //将offline数据写入hbase
        asyncOfflineBatchPut(offlineList, TableInformation.getHbaseConfiguration());

        String onlineFilePath = "/Users/liuzhaowei/Downloads/o2o_online_data_sample.csv";
        List<String[]> onlineList = readCsvData(onlineFilePath);

        asyncOnlineBatchPut(onlineList, TableInformation.getHbaseConfiguration());

    }
}
