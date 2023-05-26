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
import java.util.Arrays;

public class KMeansMapReduce {

	public static class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, PointWritable> {
		private PointWritable[] centroids;
		private final IntWritable reducerKey = new IntWritable();
		private final PointWritable reducerValue = new PointWritable();

		private int k, d;

		public KMeansMapper() {
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();

			// Retrieve k and d from the configuration
			this.k = conf.getInt("k", -1);
			this.d = conf.getInt("d", -1);

			// Genera centroidi naive method
			centroids = PointWritable.generateCentroids(k, d);
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// Convert the input text to a PointWritable
			PointWritable point = textToPoint(value, context);

			// If the point is null, skip this record
			if (point == null) {
				return;
			}

			System.out.println("ID: " + point.getID() + " - COO: " + Arrays.toString(point.getCoordinates()));

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

			// System.out.println("DEBUG: " + Arrays.toString(tokens) + "Lunghezza: " +
			// tokens.length);
			System.out.println("DEBUG: d -> " + d);

			// if (tokens.length != d + 1) {
			// throw new IllegalArgumentException(
			// "Each line must have d + 1 tokens, where the first token is the ID and the
			// remaining d tokens are the coordinates.");
			// }

			// Parse the ID
			int id = Integer.parseInt(tokens[0]);

			// Parse the coordinates
			double[] coordinates = new double[d];
			for (int i = 0; i < d; i++) {
				coordinates[i] = Double.parseDouble(tokens[i + 1]);
				System.out.println("Ciclo: " + i + " token: " + tokens[i + 1]);
			}

			System.out.println("COORDINATE: " + Arrays.toString(coordinates) + " Lunghezza: " + coordinates.length);

			// Create and return the point
			return new PointWritable(coordinates, new IntWritable(id));
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

		int k = Integer.parseInt(otherArgs[1]);
		int d = Integer.parseInt(otherArgs[2]);

		// Add k and d to the Configuration
		job.getConfiguration().setInt("k", k);
		job.getConfiguration().setInt("d", d);

		System.out.println("MAIN: d -> " + d);

		job.setJarByClass(KMeansMapReduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(KMeansMapper.class);
		job.setReducerClass(KMeansReducer.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[3]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
