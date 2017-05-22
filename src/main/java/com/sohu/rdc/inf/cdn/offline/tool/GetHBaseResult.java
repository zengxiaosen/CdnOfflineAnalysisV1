package com.sohu.rdc.inf.cdn.offline.tool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Created by yunhui li on 2017/5/16.
 */
public class GetHBaseResult {

    public static void main(String[] args) throws Exception {

        if (args.length != 1) {
            System.err.println("Usage: GetHBaseResult <table>");
            System.exit(2);
        }

        Configuration configuration = HBaseConfiguration.create();
        configuration.addResource(new Path("file:///etc/hbase/conf/hbase-site.xml"));

//        configuration.set("keytab.file", "/home/hivetest/hivetest.keytab");
//        configuration.set("kerberos.principal", "hivetest/cdh246.test@OTOCYON.COM");
//        UserGroupInformation.setConfiguration(configuration);
//        UserGroupInformation.loginUserFromKeytab("hivetest/cdh246.test@OTOCYON.COM", "/home/hivetest/hivetest.keytab");


        HTable table = new HTable(configuration, args[0]);

        Get get = new Get(Bytes.toBytes("1484668800000|00000|00"));
        Result result = table.get(get);
        // 输出结果
        for (KeyValue rowKV : result.raw()) {
            System.out.print("Row Name: " + new String(rowKV.getRow()) + " ");
            System.out.print("Timestamp: " + rowKV.getTimestamp() + " ");
            System.out.print("column Family: " + new String(rowKV.getFamily()) + " ");
            System.out.print("Row Name:  " + new String(rowKV.getQualifier()) + " ");
            System.out.println("Value: " + new String(rowKV.getValue()) + " ");
        }
    }
}
