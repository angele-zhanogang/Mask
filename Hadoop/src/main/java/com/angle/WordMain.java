package com.angle;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;

public class WordMain extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME","bigdata");
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "yarn");
//        conf.set("hbase.zookeeper.quorum", "MASTER:2181");
        conf.set("fs.default.name", "hdfs://hadoop:9100");
//        conf.set("yarn.resourcemanager.resource-tracker.address", "hadoop:8031");
//        conf.set("yarn.resourcemanager.address", "hadoop:8032");
//        conf.set("yarn.resourcemanager.scheduler.address", "hadoop:8030");
//        conf.set("yarn.resourcemanager.admin.address", "hadoop:8033");
//        conf.set("yarn.application.classpath", "$HADOOP_CONF_DIR,"
//                +"$HADOOP_COMMON_HOME/*,$HADOOP_COMMON_HOME/lib/*,"
//                +"$HADOOP_HDFS_HOME/*,$HADOOP_HDFS_HOME/lib/*,"
//                +"$HADOOP_MAPRED_HOME/*,$HADOOP_MAPRED_HOME/lib/*,"
//                +"$YARN_HOME/*,$YARN_HOME/lib/*");
//        conf.set("mapreduce.jobhistory.address", "hadoop:10020");
//        conf.set("mapreduce.jobhistory.webapp.address", "hadoop:19888");
//        conf.set("mapred.child.java.opts", "-Xmx1024m");
        conf.set("mapreduce.app-submission.cross-platform","true");
        conf.addResource("src/main/resources/core-site.xml");
        conf.addResource("src/main/resources/hdfs-site.xml");
        conf.addResource("src/main/resources/mapred-site.xml");
        conf.addResource("src/main/resources/yarn-site.xml");
        int run = ToolRunner.run(conf, new WordMain(), args);
        System.exit(run);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = super.getConf();
        conf.set("fs.defaultFS","hdfs://hadoop:9100");
        conf.set("dfs.client.use.datanode.hostname","true");
//        String input = "D:\\data\\words.txt";
//        String output = "D:\\data\\output20210202";
        String input = "hdfs://hadoop:9100/app/input/word.txt";
        String output = "hdfs://hadoop:9100/app/output20210202";
        FileSystem fs = FileSystem.newInstance(conf);
        if(fs.exists(new Path(output))){
            fs.delete(new Path(output),true);
        }
        Job job = Job.getInstance(conf, "wordCount");

        job.setJarByClass(WordMain.class);
        job.setMapperClass(WordMap.class);
        job.setReducerClass(WordReduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(input));
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(output));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        boolean b = job.waitForCompletion(true);

        return b ? 0 : 1;
    }
}
