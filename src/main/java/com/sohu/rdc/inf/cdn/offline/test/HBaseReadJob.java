package com.sohu.rdc.inf.cdn.offline.test;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;

import java.io.IOException;

/**
 * Created by yunhui li on 2017/5/16.
 */
public class HBaseReadJob {
    public static class MyMapper extends TableMapper<ImmutableBytesWritable, Put> {

        public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
            // this example is just copying the data from the source table...
            context.write(row, resultToPut(row, value));
        }

        private static Put resultToPut(ImmutableBytesWritable key, Result result) throws IOException {
            Put put = new Put(key.get());
            for (KeyValue kv : result.raw()) {
                put.add(kv);
            }
            return put;
        }
    }

    public static void main(String[] args) {

        // set other scan attrs
//        TableMapReduceUtil.initTableMapperJob(
//            "test",      // input table
//            scan,              // Scan instance to control CF and attribute selection
//            MyMapper.class,   // mapper class
//            ImmutableBytesWritable.class,              // mapper output key
//            Put.class,              // mapper output value
//            job);

    }
}
