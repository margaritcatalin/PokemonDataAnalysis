package hadoop.pokemon;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AverageSpeed {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, FloatWritable> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] values = value.toString().split(",");
			String pokemon = values[1];
			try {
				Float hp = Float.parseFloat(values[10]);
				context.write(new Text(pokemon), new FloatWritable(hp));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static class ReducerClass extends Reducer<Text, FloatWritable, Text, Text> {

		private Float totalSpeed = (float) 0;
		private int totalCount = 0;

		public void reduce(Text key, Iterable<FloatWritable> valueList, Context con)
				throws IOException, InterruptedException {
			try {
				for (FloatWritable var : valueList) {
					totalSpeed += var.get();
					System.out.println("reducer " + var.get());
					totalCount++;
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			String out =""+ totalSpeed / totalCount;
			context.write(new Text("Media totala: "), new Text(out));
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "AverageSpeed");
		job.setJarByClass(AverageSpeed.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(ReducerClass.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
