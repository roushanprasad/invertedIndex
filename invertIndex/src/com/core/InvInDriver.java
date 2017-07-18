package com.core;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InvInDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		
		Job job = Job.getInstance(getConf());
		job.setJobName("Inverted Index");
		job.setJarByClass(InvInDriver.class);
		
		job.getConfiguration().set("mapreduce.output.textoutputformat.separator", " | ");
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(InvInMapper.class);
		job.setReducerClass(InvInReducer.class);
		job.setCombinerClass(InvInReducer.class);
		
		Path inputFilePath = new Path(args[0]);
		Path outputFIlePath = new Path(args[1]);
		FileInputFormat.addInputPath(job, inputFilePath);
		FileOutputFormat.setOutputPath(job, outputFIlePath);
		
		FileInputFormat.setInputDirRecursive(job, true);
		
		return job.waitForCompletion(true)?0:1;
	}
	
	public static void main(String args[]) throws Exception{
		int exitCode = ToolRunner.run(new InvInDriver(), args);
		System.exit(exitCode);
	}

}
