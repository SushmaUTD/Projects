/**
 * 
 */
package BigData;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * @author Sushma
 *
 */
public class InMemoryJoin {
	public static class MemoryJoin extends
			Mapper<LongWritable, Text, Text, FloatWritable> {
		private Text word = new Text(); // type of output key
		private BufferedReader fileContent;
		List<String> inMemoryCache = new ArrayList<String>();

		@Override
		protected void setup(
				Mapper<LongWritable, Text, Text, FloatWritable>.Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			URI[] cacheFiles = context.getCacheFiles();
			Path getPath = new Path(cacheFiles[0].getPath());
			//FileSystem fs = FileSystem.getLocal(conf);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(getPath)));
			String words;
			while ((words = br.readLine()) != null) {
				if (words.contains("Stanford"))
				{
					inMemoryCache.add(words.split("::")[0]);
				}
			}
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] line = value.toString().split("::");
			if (inMemoryCache.contains(line[2])) {
				word.set(line[1]);
				context.write(word,
						new FloatWritable(Float.parseFloat(line[3])));
			}
		}
	}

	/**
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 * @throws URISyntaxException 
	 */
	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException, URISyntaxException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();

		GenericOptionsParser parser = new GenericOptionsParser(conf, args);
		args = parser.getRemainingArgs();
		Job job = new Job(conf, "inMemoryJoin");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		job.setJarByClass(InMemoryJoin.class);
		job.setMapperClass(MemoryJoin.class);
		job.setNumReduceTasks(0);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.addCacheFile(new Path(args[0]).toUri());
		FileInputFormat.setInputPaths(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
