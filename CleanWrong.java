package com.liqi.logmapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class CleanWrong {
	
	public static class CleanWrongMapper extends Mapper<LongWritable,Text, Text, Text>{
		Text dateIP=new Text();
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			 Safe safe=Safe.parser(value.toString());
			 if(safe.isVaild()){
				 	StringBuffer stringbuffer=new StringBuffer();
			    	stringbuffer.append(safe.getTime_local());
			    	stringbuffer.append("\t"+safe.getRemote_addr());
			    	dateIP.set(stringbuffer.toString());
			    	context.write(dateIP, new Text(safe.toString()));
			 }
		}
		
	}
	public static class CleanWrongReduce extends Reducer<Text, Text, Text,NullWritable>{
         
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			for(Text value:values){
				StringBuffer sb=new StringBuffer();
				sb.append(key+"\t");
				sb.append(value);
				context.write(new Text(sb.toString()), NullWritable.get());
			}
		}
	}
    public static  void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
    	Configuration conf=new Configuration();
    	
		conf.set("fs.defaultFS", "hdfs://ns1");
		conf.set("dfs.nameservices", "ns1");
		conf.set("dfs.ha.namenodes.ns1", "nn1,nn2");
		conf.set("dfs.namenode.rpc-address.ns1.nn1", "hadoop01:9000");
		conf.set("dfs.namenode.rpc-address.ns1.nn2", "hadoop02:9000");
		//conf.setBoolean(name, value);
		conf.set("dfs.client.failover.proxy.provider.ns1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
		
    	Job job=Job.getInstance(conf, CleanWrong.class.getSimpleName());
    	job.setJarByClass(CleanWrong.class);
    	
    	FileInputFormat.setInputPaths(job, new Path(args[0]));
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(CleanWrongMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(CleanWrongReduce.class);
		//job.setNumReduceTasks(6);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		//String output="hdfs:ns1//log/output";
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    job.setOutputFormatClass(TextOutputFormat.class);
	    job.waitForCompletion(true);
    }


}
