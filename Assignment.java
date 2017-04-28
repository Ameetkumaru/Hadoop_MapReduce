
/***************************************************************************************************************************
CSCI(Data Science) 680  Spring-2017
Assignment  --> Hadoop Assignment
Ameetkumar Omprakash Upadhyay
Zid : Z1791181
Date due : 25 April,2017

Purpose : Hadoop MapReduce program which outputs the number of occurrences of each letter in a given file using Java

***************************************************************************************************************************/

package edu.niu.bigdata.wordcount;

// List of libraries that have been imported for this program
import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Assignment extends Configured implements Tool 
{
	public static void main(String[] args) throws Exception 
	{
		System.out.println(Arrays.toString(args));
		int res = ToolRunner.run(new Configuration(), new Assignment(), args);
		System.exit(res);
	}
	@Override
	public int run(String[] args) throws Exception 
	{
		System.out.println(Arrays.toString(args));
		Job job = new Job(getConf(), "Assignment");
		job.setJarByClass(Assignment.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		return 0;
	}

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> 
	{		
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException 
		{
			String str = value.toString();
			char[] arrayOfCharacters = str.toCharArray();
			for (char ch : arrayOfCharacters) 
			{
				System.out.println(ch);
				// Checking to print if character is letter or special character
				if(Character.isLetter(ch))
				{
					context.write(new Text(String.valueOf(ch).toLowerCase()), new IntWritable(1));
				}
				else
				{
					context.write(new Text(String.valueOf(ch)), new IntWritable(1));
				}				
			}
		}
	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> 
	{
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException 
		{
			int occurrence = 0;
			IntWritable outputVal = new IntWritable();
			// Calculating the count of occurrence of characters
			for (IntWritable val : values) 
			{
				occurrence +=val.get();
				outputVal.set(occurrence);
			}			
			
			/*
			 *	Below line will help to give all characters in the text file with its count of occurrence.
			*/
			context.write(key, outputVal);	
			
			/*
			 * The below block of code if removed comments, will give only letters as output.  
			 * This block will help to remove the special characters like $%@!~			 
			*/
			
			/*			 
			String found = key.toString();
			char ch=found.charAt(0) ;
			if(Character.isLetter(ch))
			{
				context.write(key, outputVal);	
			}
			*/								
		}
	}
}