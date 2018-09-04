package com.ps.hadooptest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.text.SimpleDateFormat;
import java.util.Date;

public class WordCountRunner {

  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf();
    conf.setJobName("wordcount");
    conf.addResource("classpath:/hadoop/core-site.xml");
    conf.addResource("classpath:/hadoop/hdfs-site.xml");
    conf.addResource("classpath:/hadoop/mapred-site.xml");

    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCountRunner.class);
    job.setMapperClass(WordCountMapper.class);
    job.setCombinerClass(WordCountReducer.class);
    job.setReducerClass(WordCountReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path("/demo1"));
    FileOutputFormat.setOutputPath(job, new Path("/demo2"));
    job.waitForCompletion(true);

  }
}