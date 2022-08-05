package org.example.bigdata.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.example.bigdata.model.*;

import java.io.IOException;


/**
 * @Author: Fermi.Tang
 * @Date: Created in 11:28,2022/7/22
 */
public class HadoopTest {

    private static Logger logger = LogManager.getLogger(HadoopTest.class);

    public static class Map extends Mapper<Object, Text, KeyBean, NetWorkFlow> {
        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, KeyBean, NetWorkFlow>.Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] values = line.split("\t");
            if (values.length == 11) {
                String phone = values[1];
                Long uploadFlow = Long.parseLong(values[8]);
                Long downloadFlow = Long.parseLong(values[9]);
                Long totalFlow = uploadFlow + downloadFlow;
                NetWorkFlow netWorkFlow = NetWorkFlow.builder()
                    .uploadFlow(uploadFlow)
                    .downloadFlow(downloadFlow)
                    .totalFlow(totalFlow)
                    .build();
                KeyBean keyBean = KeyBean.builder().phone(phone).build();
                context.write(keyBean, netWorkFlow);
            }
        }
    }

    public static class Reduce extends Reducer<KeyBean, NetWorkFlow, String, String> {
        @Override
        protected void reduce(KeyBean key, Iterable<NetWorkFlow> values, Reducer<KeyBean, NetWorkFlow, String, String>.Context context) throws IOException, InterruptedException {
            Long uploadSum = 0L;
            Long downloadSum = 0L;
            Long totalSum = 0L;
            for (NetWorkFlow val : values) {
                uploadSum += val.getUploadFlow();
                downloadSum += val.getDownloadFlow();
                totalSum += val.getTotalFlow();
            }
            context.write(key.getPhone(), new NetWorkFlow(uploadSum, downloadSum, totalSum).toString());
        }
    }

    public void getNetworkFlow(String inputFile, String outputDir) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(HadoopTest.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setMapOutputKeyClass(KeyBean.class);
        job.setOutputValueClass(NetWorkFlow.class);
        FileInputFormat.addInputPath(job, new Path(inputFile));
        FileOutputFormat.setOutputPath(job, new Path(outputDir));
        job.waitForCompletion(true);
    }

    public void upload()throws IOException{
        FileSystem fileSystem = FileSystem.get(getConf());
        fileSystem.copyFromLocalFile(new Path("/Users/ftang/Downloads/users.dat"), new Path("/user/hive/warehouse/metastore.db/users"));
    }

    public void delete(Path path) throws IOException {
        FileSystem fileSystem = FileSystem.get(getConf());
        fileSystem.delete(path, true);
    }

    private Configuration getConf() {
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "1");
        conf.set("dfs.client.use.datanode.hostname", "false");
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        System.setProperty("HADOOP_USER_NAME", "root");
        return conf;
    }

    public static void main(String[] args) {
       // HadoopTest hadoopTest = new HadoopTest();
        try {
           Double a = 0.0;
          //  hadoopTest.upload();
          //  hadoopTest.getNetworkFlow("/user/HTTP_20130313143750.dat", "/output1");
        } catch (Exception e) {
            logger.error("Failed to get network flow: " + e.getMessage());
        }
    }
}
