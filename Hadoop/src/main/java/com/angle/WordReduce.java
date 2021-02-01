package com.angle;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordReduce extends Reducer<Text,NullWritable,Text,IntWritable> {
    private IntWritable count = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (NullWritable value : values) {
            sum++;
        }
        count.set(sum);
        context.write(key,count);
    }
}
