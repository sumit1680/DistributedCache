package com.sumit.hadoop;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DriverClass extends Configured implements Tool {
	

	@Override
	public int run(String[] args) throws Exception {
		
		if (args.length !=2) {
			System.out.print("Two Parameter is required: <input><output>\n");
			
		return -1;
		
		}
		
		Job job = new Job(getConf());
		Configuration conf = job.getConfiguration();
		job.setJobName("Mapsidejoin with two file in Dcache");
		
		DistributedCache.addCacheFile(new URI("/user/training/deptid.txt"), conf);
		
		job.setJarByClass(DriverClass.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(MapClass.class);
		job.setNumReduceTasks(0);
		
		boolean suscess = job.waitForCompletion(true);
		return suscess ? 0 : 1;
		
	}
		
		
		public static void main(String[] args) throws Exception {
			
		int exitcode = 	ToolRunner.run(new Configuration(),new DriverClass(), args);
		System.exit(exitcode);
		
		}
}




