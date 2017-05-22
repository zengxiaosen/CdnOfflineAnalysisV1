package com.sohu.rdc.inf.cdn.offline;

import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Created by yunhui li on 2017/5/18.
 */
public class CDNComputeToHBaseReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {

    private static final Logger LOG = LoggerFactory.getLogger(CDNComputeToHBaseReducer.class);

    private static Map<Integer, Long> requestNumResult = Maps.newHashMap();
    private static Map<Integer, Double> responseTimeResult = Maps.newHashMap();
    private static Map<Integer, Long> bodySizeResult = Maps.newHashMap();

    private static Map<Integer, Long> XX2_Result = Maps.newHashMap();
    private static Map<Integer, Long> XX3_Result = Maps.newHashMap();
    private static Map<Integer, Long> XX4_Result = Maps.newHashMap();
    private static Map<Integer, Long> XX5_Result = Maps.newHashMap();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for (Text value : values) {
            String[] fields = StringUtils.split(value.toString(), "|");
            long ts = Long.valueOf(fields[0]);
            double responseTime = Double.valueOf(fields[1]);
            long bodySize = Long.valueOf(fields[2]);
            String statusCode = fields[3];

            DateTime dt = new DateTime(ts);

            // 每5分钟1个点，这里计算是当天的第几个点
            int pointOfDay = dt.getHourOfDay() * 12 + dt.getMinuteOfHour() / 5 + 1;

            saveToMap(requestNumResult, pointOfDay, 1L);
            saveToMap(responseTimeResult, pointOfDay, responseTime);
            saveToMap(bodySizeResult, pointOfDay, bodySize);

            if (StringUtils.startsWith(statusCode, "2")) {
                saveToMap(XX2_Result, pointOfDay, 1L);
            } else if (StringUtils.startsWith(statusCode, "3")) {
                saveToMap(XX3_Result, pointOfDay, 1L);
            } else if (StringUtils.startsWith(statusCode, "4")) {
                saveToMap(XX4_Result, pointOfDay, 1L);
            } else if (StringUtils.startsWith(statusCode, "5")) {
                saveToMap(XX5_Result, pointOfDay, 1L);
            }
        }

        String tableName = context.getConfiguration().get("tableName");

        HTable table = new HTable(context.getConfiguration(), tableName);

        String historyKey = key.toString();
        Get get = new Get(Bytes.toBytes(historyKey));

        Result result = table.get(get);
        if (!result.isEmpty()) {
            LOG.warn("key=" + historyKey + " is not null, update history result");
            //
            for (int i = 1; i <= 288; i++) {
                updateToLongMap(requestNumResult, i, result, "request");
                updateToDoubleMap(responseTimeResult, i, result, "latency");
                updateToLongMap(bodySizeResult, i, result, "flow");
                updateToLongMap(XX2_Result, i, result, "2XX");
                updateToLongMap(XX3_Result, i, result, "3XX");
                updateToLongMap(XX4_Result, i, result, "4XX");
                updateToLongMap(XX5_Result, i, result, "5XX");
            }
        }

        Put put = new Put(Bytes.toBytes(key.toString()));

        for (int i = 1; i <= 288; i++) {
            saveToPut(requestNumResult, "request", i, put);
            saveToPut(responseTimeResult, "latency", i, put);
            saveToPut(bodySizeResult, "flow", i, put);
            saveToPut(XX2_Result, "2XX", i, put);
            saveToPut(XX3_Result, "3XX", i, put);
            saveToPut(XX4_Result, "4XX", i, put);
            saveToPut(XX5_Result, "5XX", i, put);
        }
        context.write(null, put);
    }

    private Map<Integer, Long> saveToMap(Map<Integer, Long> map, int key, long delta) {
        if (map.get(key) != null) {
            map.put(key, map.get(key) + delta);
        } else {
            map.put(key, delta);
        }
        return map;
    }

    private Map<Integer, Double> saveToMap(Map<Integer, Double> map, int key, double delta) {
        if (map.get(key) != null) {
            map.put(key, map.get(key) + delta);
        } else {
            map.put(key, delta);
        }
        return map;
    }

    private void saveToPut(Map map, String cf, int column, Put put) {
        if (map.get(column) != null) {
            put.add(Bytes.toBytes(cf), Bytes.toBytes(String.valueOf(column)), Bytes
                .toBytes(map.get(column).toString()));
        } else {
            put.add(Bytes.toBytes(cf), Bytes.toBytes(String.valueOf(column)), Bytes
                .toBytes("0"));
        }
    }

    private void updateToLongMap(Map<Integer, Long> map, int key, Result result, String cf) {

        if (map.get(key) != null) {
            map.put(key, map.get(key)
                + Long.valueOf(new String(result.getValue(cf.getBytes(), String.valueOf(key)
                .getBytes()))));
        } else {
            map.put(key, Long.valueOf(new String(result.getValue(cf.getBytes(), String.valueOf(key)
                .getBytes()))));
        }
    }

    private void updateToDoubleMap(Map<Integer, Double> map, int key, Result result, String cf) {

        if (map.get(key) != null) {
            map.put(key, map.get(key)
                + Double.valueOf(new String(result.getValue(cf.getBytes(), String.valueOf(key)
                .getBytes()))));
        } else {
            map.put(key, Double.valueOf(new String(result.getValue(cf.getBytes(), String.valueOf(key)
                .getBytes()))));
        }
    }

}