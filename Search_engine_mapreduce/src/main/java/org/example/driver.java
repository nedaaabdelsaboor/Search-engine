package org.example;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class driver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration c = new Configuration();
        Job j = Job.getInstance(c, "ir-first-step");
        j.setMapperClass(mapper.class);
        j.setReducerClass(reducer.class);
        j.setJarByClass(driver.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(j, new Path(args[0]));
        FileOutputFormat.setOutputPath(j, new Path(args[1]));

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }
}
