/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.example1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MaxInteger {
	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private Text integer = new Text(); // creating a text variable

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// created a iterator to iterate over the tokens which will be
			// extracted from the input files
			StringTokenizer itr = new StringTokenizer(value.toString());

			// filesplit to get the filenames from the input file names
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			// storing the extracted filename into the variable fileName
			String fileName = fileSplit.getPath().getName();
			// making the filename as the key
			Text tempKey = new Text(fileName);

			while (itr.hasMoreTokens()) {
				// using iterator to set each token to the integer object of the Text class
				integer.set(itr.nextToken()); 
				
				// storing the integers extracted in the value							
				IntWritable tempVal = new IntWritable(Integer.parseInt(integer
						.toString()));
				// writing the key value pair  with the filename and the value
				context.write(tempKey, tempVal); 										
													 
			}
		}
	}

	public static class MaxReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int max = 0;
			// finding the maximum value using the iterable values
			for (IntWritable val : values) {
				if (val.get() > max)
					max = val.get();
			}

			result.set(max); // setting the maximum integer value
			context.write(new Text("maximumInteger"), result);

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			// if the arguments are not passed, the above error line is shown on
			// the console
			System.err.println("Usage: maximumInteger <in> <out>");
			System.exit(2);
		}

		// creating the job object with the maximum integer configuration
		Job job = new Job(conf, "max integer");
		// setting the jar by the class of the current MaxInteger class
		job.setJarByClass(MaxInteger.class);
		// setting the Mapper Class to the TokenizerMapper
		job.setMapperClass(TokenizerMapper.class);
		// setting the combiner class to the MaxReducer
		job.setCombinerClass(MaxReducer.class);
		// after combining setting the reducer class to the MaxReducer
		job.setReducerClass(MaxReducer.class);
		// setting the number of reduce tasks
		job.setNumReduceTasks(1);
		// setting the output key class to the Text class
		job.setOutputKeyClass(Text.class);
		// setting the output value class to the IntWritable class
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}