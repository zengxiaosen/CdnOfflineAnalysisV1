package com.sohu.rdc.inf.cdn.offline;

import com.hadoop.mapreduce.LzoTextInputFormat;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by yunhui li on 2017/5/19.
 */
public class RegWordCount {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        private static String REGEX = "(.*\\d+:\\d+:\\d+)? ?(\\S+[@| ]\\S+) (\\d+.?\\d+) (\\d+" +
            ".?\\d+)" +
            " (\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}) (-/\\d+) (\\d+) (\\w{3,6})" +
            " (\\w{3,5}://\\S+) - (\\S+) (\\S+[;|; ]?[^\"]*?[;|; ]?[^\"]*?)" +
            " ([\"]\\S*[\"])( [\"].*[\"])?";

        private static Pattern pattern = Pattern.compile(REGEX);

        private int errorCauseMissField;
        private int errorCauseNotNum;
        private int errorCauseStatusCode;
        private int errorCauseURL;
        private int errorCauseMethod;
        private int totalNum;

        // 30s
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());

            String nginxLog = value.toString();
            totalNum++;

            Matcher matcher = pattern.matcher(nginxLog);

            String timeStr = "";
            String machine = "";
            String tsStr = "";
            String responseTimeStr = "";
            String srcIPStr = "";
            String statusStr = "";
            String bodySizeStr = "";
            String method = "";
            String url = "";
            String dstIPStr = "";
            String contentType = "";
            String referUrl = "";
            String browser = "";

            // 字段个数不匹配
            if (!matcher.find()) {
                errorCauseMissField++;
                return;
            }

            timeStr = matcher.group(1);
            machine = matcher.group(2);
            tsStr = matcher.group(3);
            responseTimeStr = matcher.group(4);
            srcIPStr = matcher.group(5);
            statusStr = matcher.group(6);
            bodySizeStr = matcher.group(7);
            method = matcher.group(8);
            url = matcher.group(9);
            dstIPStr = matcher.group(10);
            contentType = matcher.group(11);
            referUrl = matcher.group(12);
            browser = matcher.group(13);

//            double tsInSecond = 0;
//            long bodySize = 0;
//            double responseTime = 0;
//            try {
//                tsInSecond = Double.valueOf(tsStr);
//                bodySize = Long.valueOf(bodySizeStr);
//                responseTime = Double.valueOf(responseTimeStr);
//            } catch (Exception e) {
//                // ts，bodySize，responseTime字段不是数值类型，认为是非法
//                errorCauseNotNum++;
//                return;
//            }
//
//            String statusCode = StringUtils.removeStart(statusStr, "-/");
//
//            // 状态码不是 2XX,3XX,4XX,5XX 是非法的日志
//            if (!StringUtils.startsWith(statusCode, "2") && !StringUtils.startsWith(statusCode, "3")
//                && !StringUtils.startsWith(statusCode, "4")
//                && !StringUtils.startsWith(statusCode, "5")) {
//
//                errorCauseStatusCode++;
//                return;
//            }
//
//            // 必须是http或https
//            if (!StringUtils.contains(url, "http://") && !StringUtils.contains(url, "https://")) {
//                errorCauseURL++;
//                return;
//            }
//            // 只处理GET和POST,HEAD,DELETE, OPTIONS
//            if (!StringUtils.contains(method, "GET") && !StringUtils.contains(method, "POST")
//                && !StringUtils.contains(method, "HEAD") && !StringUtils.contains(method, "DELETE")
//                && !StringUtils.contains(method, "OPTIONS")) {
//                errorCauseMethod++;
//                return;
//            }
//
//
//            int inteval_5m = 5 * 60;
//            long roundTs_5m = (long) ((Math.floor(tsInSecond / inteval_5m)) * inteval_5m * 1000);
//            DateTime dt_5m = new DateTime(roundTs_5m);
//            DateTime dt_1d = dt_5m.dayOfMonth().roundFloorCopy();
//
//            long requestNum = 1L;
//            long XX2_Result = 0L;
//            long XX3_Result = 0L;
//            long XX4_Result = 0L;
//            long XX5_Result = 0L;
//            if (StringUtils.startsWith(statusCode, "2")) {
//                XX2_Result = 1;
//            } else if (StringUtils.startsWith(statusCode, "3")) {
//                XX3_Result = 1;
//            }
//            if (StringUtils.startsWith(statusCode, "4")) {
//                XX4_Result = 1;
//            }
//            if (StringUtils.startsWith(statusCode, "5")) {
//                XX5_Result = 1;
//            }

            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "reg word count");
        job.setJarByClass(WordCount.class);

        job.setMapperClass(TokenizerMapper.class);

        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        job.setInputFormatClass(LzoTextInputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
            new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static int genDayTS(long ts) {
        int offset = 8 * 60 * 60;
        return (int) ((ts + offset) / (60 * 60 * 24));
    }

}
