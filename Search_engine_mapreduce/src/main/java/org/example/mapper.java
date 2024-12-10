package org.example;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
public class mapper extends Mapper<LongWritable, Text, Text, Text>{

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String filename=((FileSplit) context.getInputSplit()).getPath().getName();
        String DocId= filename.replace(".txt", "");
        String line= value.toString();

        String[] words=line.split(" ");
        int position=0;
        for(String word:words){
            context.write(new Text(word), new Text(DocId+":" + (position+1)));
            position++;
        }
    }
}

