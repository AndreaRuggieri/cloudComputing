package it.unipi.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import java.io.DataOutput;
import java.io.IOException;
import java.io.DataInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import it.unipi.hadoop.KMeansUtils;

public class KMeansMapReduce {

	static int k;
	static int d;

	public static class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, PointWritable> {
		private PointWritable[] centroids;
		private final IntWritable reducerKey = new IntWritable();
		private final PointWritable reducerValue = new PointWritable();

		public KMeansMapper() {
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			// Genera centroidi naive method
			centroids = PointWritable.generateCentroids(k, d);
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// Convert the input text to a PointWritable
			PointWritable point = textToPoint(value, context);

			// Find the nearest centroid to the point
			IntWritable nearestCentroidId = point.getNearestCentroid(centroids).getID();

			// Write the centroid id and the point to the context
			context.write(nearestCentroidId, point);
		}

		private PointWritable textToPoint(Text text, Context context) throws IOException, InterruptedException {
			String line = text.toString();
			if (line == null || line.length() == 0)
				return null;

			String[] tokens = line.trim().split(",");

			if (tokens.length != d) {
				double[] val = new double[d];
				for (int i = 1; i <= d; i++) {
					double att = Double.parseDouble(tokens[i]);
					val[i] = att;
				}
				reducerKey.set(Integer.parseInt(tokens[0]));
				// reducerKey.set(tokens[0]);
				reducerValue.set(reducerValue, val);
				context.write(reducerKey, reducerValue);
			}

			return null;
		}

	}

	public static class KMeansCombiner extends Reducer<IntWritable, PointWritable, IntWritable, ClusterSumWritable> {
		public KMeansCombiner() {
		}

		@Override
		protected void reduce(IntWritable key, Iterable<PointWritable> values, Context context)
				throws IOException, InterruptedException {
			// Calculate the sum of points and the count
			ClusterSumWritable clusterSum = KMeansUtils.calculateClusterSum(values);

			// Write the cluster id and the cluster sum to the context
			context.write(key, clusterSum);
		}

	}

	public static class KMeansReducer extends Reducer<IntWritable, PointWritable, IntWritable, PointWritable> {

		public KMeansReducer() {
		}

		@Override
		protected void reduce(IntWritable key, Iterable<PointWritable> values, Context context)
				throws IOException, InterruptedException {
			// Calculate the sum of points and the count
			ClusterSumWritable clusterSum = KMeansUtils.calculateClusterSum(values);

			// Calculate the new centroid
			PointWritable newCentroid = calculateNewCentroid(clusterSum, key);

			// Write the cluster id and the new centroid to the context
			context.write(key, newCentroid);
		}

		private PointWritable calculateNewCentroid(ClusterSumWritable clusterSum, IntWritable id) {
			double[] sum = clusterSum.getSumCoordinates();
			int count = clusterSum.getCount();
			double[] centroid = new double[sum.length];

			for (int i = 0; i < sum.length; i++) {
				centroid[i] = sum[i] / count;
			}

			return new PointWritable(centroid, id);
		}

	}

	public static void main(final String[] args) throws Exception {
		final Configuration conf = new Configuration();
		final Job job = new Job(conf, "kmeans");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		job.setJarByClass(KMeansMapReduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(KMeansMapper.class);
		job.setReducerClass(KMeansReducer.class);

		k = Integer.parseInt(otherArgs[1]);
		d = Integer.parseInt(otherArgs[2]);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[3]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
