package hadoop.pokemon;

import java.io.IOException;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;

public class TopFastestPokemon {

	public static final int K = 10;

	public static class PokemonMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String lineValue = value.toString();
			if (null == lineValue) {
				return;
			}
			String[] strArr = lineValue.split(",");
			context.write(new Text(strArr[1]), new LongWritable(Long.valueOf(strArr[10])));
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			super.cleanup(context);
		}
	}

	public static class PokemonReducer extends Reducer<Text, LongWritable, TopPokemonWritable, NullWritable> {
		TreeSet<TopPokemonWritable> treeSet = new TreeSet<TopPokemonWritable>();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
		}

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			if (null == key) {
				return;
			}
			String strArr = key.toString();
			String pokemonName = strArr;
			Long totalPower = 0L;
			for (LongWritable value : values) {
				totalPower += value.get();
			}
			treeSet.add(new TopPokemonWritable(pokemonName, totalPower));
			if (treeSet.size() > K) {
				treeSet.remove(treeSet.last());
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for (TopPokemonWritable top : treeSet) {
				context.write(top, NullWritable.get());
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "TopFastestPokemon");
		job.setJarByClass(TopFastestPokemon.class);
		job.setMapperClass(PokemonMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setReducerClass(PokemonReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(TopFastestPokemon.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
