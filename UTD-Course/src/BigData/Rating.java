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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Rating {
	public static class CategoryMap extends
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

	public static class CategoryReduce extends
			Reducer<Text, FloatWritable, Text, FloatWritable> {
		HashMap<Float, List<Text>> business_ids = new HashMap<Float, List<Text>>();
		public void reduce(Text key, Iterable<FloatWritable> values,
				Context context) throws IOException, InterruptedException {
			float avg = (float) 0.0;
			int count = 0;
			for (FloatWritable value : values) {
				avg += value.get();
				count++;
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
					context.write(new Text(t.toString()), new FloatWritable(val));
					count++;
				}
			}
		}
	}

	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		
		if (otherArgs.length != 2) {
			System.err
					.println("Please enter the input file location and output file locations");
			System.exit(2);
		}
		
		Job job = new Job(conf, "findTopTenBusiness");
		job.setJarByClass(Rating.class);
		job.setMapperClass(CategoryMap.class);
		job.setReducerClass(CategoryReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
