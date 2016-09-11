package com.liqi.logmapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class KPIDateIpNum {

	
	public static class DateIpNumMapper extends 
	                   Mapper<LongWritable, Text, Text, LongWritable>{
        Text dateIP=new Text();
        LongWritable one=new LongWritable(1L);
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			    KPI kpi=KPI.parser(value.toString());
			    if(kpi.isVaild()){
			    	StringBuffer stringbuffer=new StringBuffer();
			    	stringbuffer.append(kpi.getTime_local());
			    	stringbuffer.append("\t"+kpi.getRemote_addr());
			    	dateIP.set(stringbuffer.toString());
			    	context.write(dateIP, one);
			     }
			   
		}
	}
    
	public static class DateIPReducde extends 
	                     Reducer<Text, LongWritable, Text, Text>{
        Text dateloacal =new Text();
        Text IpNum=new Text();
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,
				Reducer<Text, LongWritable, Text, Text>.Context context)
				throws IOException, InterruptedException {
			    long sum=0;
			    for(LongWritable value:values){
			    	sum+=value.get();
			    }
			    dateloacal=getDate(key);
			    IpNum=getDateIpNum(key, sum);
			    context.write(dateloacal, IpNum);
		}
	}
	
	//将key 进行分割  形成日期  ip 数量
	public static  Text getDateIpNum(Text key,long value){
		Text ipsum =new Text(); 
		String []arr=key.toString().split("\t");
		ipsum.set(arr[1]+"\t"+String.valueOf(value));
        return ipsum;		 
	}
	
	public static Text getDate(Text key){
		Text datelocal=new Text();
		String []arr=key.toString().split("\t");
		datelocal.set(arr[0]);
		return datelocal;
	}
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf, KPIDateIpNum.class.getSimpleName());
		job.setJarByClass(KPIDateIpNum.class);
		
		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.12.60:9000/log/input"));
		job.setInputFormatClass(TextInputFormat.class);
	    job.setMapperClass(DateIpNumMapper.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(LongWritable.class);
	    
	    job.setReducerClass(DateIPReducde.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    String output="hdfs://192.168.12.60:9000/log/output";
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputFormatClass(TextOutputFormat.class);
		job.waitForCompletion(true);
	    
	} 
	
}
