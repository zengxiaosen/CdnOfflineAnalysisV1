package com.sohu.rdc.inf.cdn.offline.tool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Created by yunhui li on 2017/5/19.
 */
public class WriteResultToHBase {
    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: WriteResultToHBase <input> <table>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);

        Path path = new Path(args[0]);

        InputStream inputStream = hdfs.open(path);

        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));

        String tempString = null;

        int line = 1;
        // 一次读入一行，直到读入null为文件结束
        while ((tempString = br.readLine()) != null) {
            // 显示行号
            System.out.println("line " + line + ": " + tempString);
            line++;
        }

        br.close();
        inputStream.close();
        hdfs.close();

    }
}
