package com.angle;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordMap extends Mapper<LongWritable,Text,Text,NullWritable> {
    private Text keyWord = new Text();
    private NullWritable valueWord = NullWritable.get();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split(" ");
        for (String word : words) {
            keyWord.set(word);
            context.write(keyWord,valueWord);
        }
    }
}
