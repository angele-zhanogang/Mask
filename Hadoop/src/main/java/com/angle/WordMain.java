package com.angle;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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

public class WordMain extends Configured implements Tool{
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int run = ToolRunner.run(conf, new WordMain(), args);
        System.exit(run);
    }

    public int run(String[] args) throws Exception {
        String input = "D:\\data\\words.txt";
        String output = "D:\\data\\output20210202";

        File file = new File(output);
        if(file.isDirectory()) {
            FileUtils.deleteDirectory(file);
        }
        Job job = Job.getInstance(super.getConf(), "wordCount");

        job.setJarByClass(WordMain.class);
        job.setMapperClass(WordMap.class);
        job.setReducerClass(WordReduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path(input));
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path(output));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        boolean b = job.waitForCompletion(true);

        return b?0:1;
    }
}
