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

public class Clean {
	
	public static class CleanMapper extends Mapper<LongWritable,Text, Text, Text>{
		Text dateIP=new Text();
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			 KPI kpi=KPI.parser(value.toString());
			 if(kpi.isVaild()){
				 	StringBuffer stringbuffer=new StringBuffer();
			    	stringbuffer.append(kpi.getTime_local());
			    	stringbuffer.append("\t"+kpi.getRemote_addr());
			    	dateIP.set(stringbuffer.toString());
			    	context.write(dateIP, new Text(kpi.toString()));
			 }
		}
		
	}
	public static class CleanReduce extends Reducer<Text, Text, Text,NullWritable>{
         
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
    	Job job=Job.getInstance(conf, Clean.class.getSimpleName());
    	job.setJarByClass(Clean.class);
    	
    	FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.12.60:9000/log/input"));
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(CleanMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(CleanReduce.class);
		//job.setNumReduceTasks(6);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		String output="hdfs://192.168.12.60:9000/log/output";
	    FileOutputFormat.setOutputPath(job, new Path(output));
	    job.setOutputFormatClass(TextOutputFormat.class);
	    job.waitForCompletion(true);
    }

}
