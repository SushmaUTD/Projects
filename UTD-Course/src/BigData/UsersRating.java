package BigData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class UsersRating {
	public static class Job1Mapper extends
			Mapper<LongWritable, Text, Text, FloatWritable> {
		private Text word = new Text(); // type of output key

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// Split the line by the deliminator (i.e Scope resolution operator)
			String[] line = value.toString().split("::");
			word.set(line[2]);
			context.write(word, new FloatWritable(Float.parseFloat(line[3])));
		}
	}


	public static class Job1Reducer extends
			Reducer<Text, FloatWritable, Text, FloatWritable> {
		HashMap<Float, List<Text>> business_ids = new HashMap<Float, List<Text>>();

		public void reduce(Text key, Iterable<FloatWritable> values,
				Context context) throws IOException, InterruptedException {
			float avg = (float) 0.0;
			int count = 0;
			for (FloatWritable value : values) {
				avg += value.get();
				count++; // to calculate the average
			}
			Float avg_val = avg / count;
			if (business_ids.containsKey(avg_val)) {
				business_ids.get(avg_val).add(new Text(key.toString()));
			} else {
				List<Text> bId_List = new ArrayList<Text>();
				bId_List.add(new Text(key.toString()));
				business_ids.put(avg_val, bId_List);
			}
		}

		@Override
		protected void cleanup(
				Reducer<Text, FloatWritable, Text, FloatWritable>.Context context)
				throws IOException, InterruptedException {
			List<Entry<Float, List<Text>>> valueList = new ArrayList<Entry<Float, List<Text>>>(
					business_ids.entrySet());
			Collections.sort(valueList,
					new Comparator<Map.Entry<Float, List<Text>>>() {
						@Override
						public int compare(Entry<Float, List<Text>> o1,
								Entry<Float, List<Text>> o2) {
							return o2.getKey().compareTo(o1.getKey());
						}
					});
			int count = 0;
			for (Entry<Float, List<Text>> entry : valueList) {
				Float val = entry.getKey();
				List<Text> businessidList = entry.getValue();
				for (Text t : businessidList) {
					if (count >= 10)
						break;
					context.write(new Text(t.toString()),
							new FloatWritable(val));
					count++;
				}
			}
		}
	}

public static class Job2Job1Output extends Mapper<LongWritable,Text,Text,Text>
	{
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString().trim();
			String[] lines = line.split("\t");
			String businessId = lines[0].trim();
			String rating = lines[1].trim();
			context.write(new Text(businessId), new Text( businessId + "::" + rating));

		}
	}


public static class Job2Mapper extends
			Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String businessInfo[] = value.toString().split("::");
			context.write(new Text(businessInfo[0].trim()), new Text(businessInfo[0].trim()+"::"+ businessInfo[1].trim()
					+ "::" + businessInfo[2].trim()));

		}
	}

	public static class Job2Reducer extends Reducer<Text, Text, Text, Text> {

		private ArrayList<String> topTenBusiness = new ArrayList<String>();
		private ArrayList<String> businessInfo = new ArrayList<String>();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			for (Text text : values) {
				String value = text.toString();
				if (value.split("::").length==2) {			
					topTenBusiness.add(value);
				} else {
					businessInfo.add(value);
				}
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			for (String topBusiness : topTenBusiness) {
				for (String detail : businessInfo) {
					String[] topSplit = topBusiness.split("::");
					String businessIds = topSplit[0].trim();
					String[] businessDetailsInfo = detail.split("::");
					String businessIds2 = businessDetailsInfo[0].trim();
					if (businessIds.equals(businessIds2)) {
						context.write(new Text(businessIds), new Text(
								businessDetailsInfo[1] + "\t" + businessDetailsInfo[2] + "\t"
										+ topSplit[1]));
						break;
					}
				}
			}
		}
	}

	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		// get all args
		if (otherArgs.length != 4) {
			System.err
					.println("Please enter the input file location and output file locations");
			System.exit(2);
		}
		
		Job job = new Job(conf, "findTopTenBusiness");
		job.setJarByClass(UsersRating.class);
		job.setMapperClass(Job1Mapper.class);
		job.setReducerClass(Job1Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		boolean isJobCompleted = job.waitForCompletion(true);
		if (isJobCompleted) {
			Configuration config2 = new Configuration();
			Job job2 = Job.getInstance(config2, "JOB2");
			job2.setJarByClass(UsersRating.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			job2.setInputFormatClass(TextInputFormat.class);
			job2.setOutputFormatClass(TextOutputFormat.class);

			
			MultipleInputs.addInputPath(job2, new Path(otherArgs[2]),
					TextInputFormat.class, Job2Job1Output.class);
			MultipleInputs.addInputPath(job2, new Path(otherArgs[1]),
					TextInputFormat.class, Job2Mapper.class);

			job2.setReducerClass(Job2Reducer.class);
			FileOutputFormat.setOutputPath(job2, new Path(otherArgs[3]));

			job2.waitForCompletion(true);
		}
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
