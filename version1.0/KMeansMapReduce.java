package it.unipi.hadoop;

import java.io.*;
import java.util.*;

import javax.naming.Context;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;

import it.unipi.hadoop.CentroidUtils;

// TODO: Spostare Mapper, Combiner e Reducer in file diversi

public class KMeansMapReduce {

	public static class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, PointWritable> {
		private PointWritable[] centroids;

		private int k, d;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();

			// Retrieve k and d from the configuration
			this.k = conf.getInt("k", -1);
			this.d = conf.getInt("d", -1);

			centroids = CentroidUtils.loadCentroids("f", "kmeans/oldCentroids.txt");

		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// Convert the input text to a PointWritable
			PointWritable point = textToPoint(value, context);

			// If the point is null, skip this record
			if (point == null) {
				return;
			}

			// Find the nearest centroid to the point
			IntWritable nearestCentroidId = point.getNearestCentroid(centroids);

			// Write the centroid id and the point to the context
			context.write(nearestCentroidId, point);
		}

		private PointWritable textToPoint(Text text, Context context) throws IOException, InterruptedException {
			String line = text.toString();
			if (line == null || line.length() == 0)
				return null;

			String[] tokens = line.trim().split(",");

			if (tokens.length != d) {
				throw new IllegalArgumentException(
						"Each line must have d tokens, where the first token is the ID and the remaining d tokens are the coordinates.");
			}

			// Parse the coordinates
			double[] coordinates = new double[d];
			for (int i = 0; i < d; i++) {
				coordinates[i] = Double.parseDouble(tokens[i]);
			}

			// Create and return the point
			return new PointWritable(coordinates, new IntWritable(0));
		}

	}

	public static class KMeansCombiner
			extends Reducer<IntWritable, PointWritable, IntWritable, PointWritable> {

		private int k, d;

		@Override
		protected void reduce(IntWritable key, Iterable<PointWritable> values, Context context)
				throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();
			// Retrieve k and d from the configuration
			this.k = conf.getInt("k", -1);
			this.d = conf.getInt("d", -1);

			// Initialize partial sum value to 0
			PointWritable partialSum = new PointWritable(d);

			for (PointWritable point : values) {
				partialSum.sumPoint(point);
			}

			// Write the cluster id and the cluster sum to the context
			context.write(key, partialSum);
		}

	}

	public static class KMeansReducer
			extends Reducer<IntWritable, PointWritable, IntWritable, PointWritable> {

		private int k, d;

		@Override
		protected void reduce(IntWritable key, Iterable<PointWritable> partialSums, Context context)
				throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();
			// Retrieve k and d from the configuration
			this.k = conf.getInt("k", -1);
			this.d = conf.getInt("d", -1);

			// Initialize cluster sum value to 0
			PointWritable clusterSum = new PointWritable(d);

			for (PointWritable partialSum : partialSums) {
				clusterSum.sumPoint(partialSum);
			}

			// Calculate the new centroid
			PointWritable newCentroid = calculateNewCentroid(clusterSum, key);

			// Write the cluster id and the new centroid to the context
			context.write(key, newCentroid);
		}

		private PointWritable calculateNewCentroid(PointWritable clusterSum, IntWritable id) {
			double[] sum = clusterSum.getCoordinates();
			int count = clusterSum.getClusterElementsNumber();
			double[] centroid = new double[sum.length];

			for (int i = 0; i < sum.length; i++) {
				centroid[i] = sum[i] / count;
			}

			return new PointWritable(centroid, id);
		}

	}

	public static void debugToFile(String text) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		Random random = new Random();

		int c = (int) (random.nextDouble() * 1000);
		Path outputPath = new Path("debug/debug" + c + ".txt");

		if (fs.exists(outputPath)) {
			// If file exists, remove it to start fresh
			fs.delete(outputPath, true);
		}
		FSDataOutputStream out = fs.create(outputPath);
		out.writeBytes(text);
		out.writeBytes("\n");
		out.close();
	}

	public static void main(final String[] args) throws Exception {
		final Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		int k = Integer.parseInt(otherArgs[1]);
		int d = Integer.parseInt(otherArgs[2]);
		int MaxIterations = Integer.parseInt(otherArgs[3]);

		double threshold = 0.00001; // Define a threshold for the centroid difference
		boolean converged = false;
		boolean maxIterationReached = false;
		int count = 0;

		System.out.println("THRESHOLD: " + threshold);

		PointWritable[] centroids = CentroidUtils.getStartingCentroids("input.txt", k);
		// Salviamo i centroidi appena generati
		CentroidUtils.saveCentroids(centroids, "kmeans/oldCentroids.txt");

		while (!converged && !maxIterationReached) {
			System.out.println("CICLO: n -> " + (count + 1));

			final Job job = new Job(conf, "kmeans");
			// Add k and d to the Configuration
			job.getConfiguration().setInt("k", k);
			job.getConfiguration().setInt("d", d);

			// Set one reducer per cluster
			job.setNumReduceTasks(k);

			job.setJarByClass(KMeansMapReduce.class);

			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(PointWritable.class);

			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(PointWritable.class);

			job.setMapperClass(KMeansMapper.class);
			job.setCombinerClass(KMeansCombiner.class);
			job.setReducerClass(KMeansReducer.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[4] + "/iteration" + count));

			job.waitForCompletion(true);

			// Load the old and new centroids from HDFS
			PointWritable[] oldCentroids = CentroidUtils.loadCentroids("f", "kmeans/oldCentroids.txt");
			PointWritable[] newCentroids = CentroidUtils.loadCentroids("d", args[4] + "/iteration" + count); // outputTestxx/part*
			count++;

			// Calculate the difference between the old and new centroids
			double difference = 0.0;
			for (int i = 0; i < k; i++) {
				double temp = CentroidUtils.calculateCentroidDifference(oldCentroids[i], newCentroids[i]);
				if (temp > difference) {
					difference = temp;
				}
			}

			System.out.println("DIFFERENZA: " + difference);

			// If the difference is less than the threshold, the algorithm has converged
			if (difference < threshold) {
				converged = true;
				System.out.println("END: threshold.");
			}

			// if the number of max iterations is reached, the algorithm stops
			if (count >= MaxIterations) {
				maxIterationReached = true;
				System.out.println("END: max iterations reached.");
			}

			CentroidUtils.saveCentroids(newCentroids, "kmeans/oldCentroids.txt");
		}
	}

}
