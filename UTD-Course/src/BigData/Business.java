package BigData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Business {
	
	/*
	 * Mapper : Split the line and check for the 2nd item in the o/p array as that contains the value of location
	 * Once you find if the string contains Palo Alto, then split the 3rd item to get the category
	 */
	public static class CategoryMap extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text(); // type of output key
		List<String> Categories = new ArrayList<String>();
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// Split the line by the deliminator (i.e Scope resolution operator)
			String[] line = value.toString().split("::");
			if (line[1].contains("Palo Alto")) {
				String[] categories = line[2].substring(5, line[2].length()-1).split(",");
				for(int i=0;i<categories.length;i++)
				{
					if(categories[i].trim().startsWith(" "))
						word.set(categories[i].substring(1).trim());
					else
						word.set(categories[i].trim());
					
					context.write(word, one);
				}
				
			}
		}

	}

	//Writing Unique outputs onto console
	public static class CategoryReduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
				context.write(key, null);	
		}
	}

	// Driver program
	public static void main(String[] args) throws Exception {
		//Provides access to configuration parameters
		Configuration conf = new Configuration();
		//Utility to parse command line arguments  generic to the hadoop framework
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		// get all args
		if (otherArgs.length != 2) {
			System.err
					.println("Please enter the input file location and output file locations");
			System.exit(2);
		}
		// create a job with name "find Unique Category"
		Job job = new Job(conf, "findUniqueCategories");
		//Set the jar file class
		job.setJarByClass(Business.class);
		job.setMapperClass(CategoryMap.class);
		job.setReducerClass(CategoryReduce.class);
		// set output key type
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(IntWritable.class);
		// set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		// Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
