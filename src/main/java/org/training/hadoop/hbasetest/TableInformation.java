package org.training.hadoop.hbasetest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class TableInformation {

    public static final String TABLE_NAME="train_data";
    public static final String COLUMN_FAMILY_1="offline";
    public static final String COLUMN_FAMILY_2="online";
    public static final String QUALIFIER_NAME_1_1="user_id";
    public static final String QUALIFIER_NAME_1_2="merchant_id";
    public static final String QUALIFIER_NAME_1_3="coupon_id";
    public static final String QUALIFIER_NAME_1_4="discount_rate";
    public static final String QUALIFIER_NAME_1_5="distance";
    public static final String QUALIFIER_NAME_1_6="date_received";
    public static final String QUALIFIER_NAME_1_7="date";

    public static final String QUALIFIER_NAME_2_1="user_id";
    public static final String QUALIFIER_NAME_2_2="merchant_id";
    public static final String QUALIFIER_NAME_2_3="action";
    public static final String QUALIFIER_NAME_2_4="coupon_id";
    public static final String QUALIFIER_NAME_2_5="discount_rate";
    public static final String QUALIFIER_NAME_2_6="date_received";
    public static final String QUALIFIER_NAME_2_7="date";


    public static Configuration getHbaseConfiguration(){
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "hadoop");
        configuration.set("zookeeper.znode.parent", "/hbase");
        return configuration;
    }

    public static void createTable() throws Exception{
        Connection connection = ConnectionFactory.createConnection(getHbaseConfiguration());
        Admin admin = connection.getAdmin();

        //判断表存在不存在
        if (!admin.tableExists(TableName.valueOf(TABLE_NAME))){
            //不存在则创建表
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
            HColumnDescriptor offline = new HColumnDescriptor(COLUMN_FAMILY_1);
            HColumnDescriptor online = new HColumnDescriptor(COLUMN_FAMILY_2);
            tableDescriptor.addFamily(offline);
            tableDescriptor.addFamily(online);
            admin.createTable(tableDescriptor);
        }
    }

    public static void main(String[] args) throws Exception{
        TableInformation.createTable();
    }
}
