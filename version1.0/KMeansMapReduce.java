package it.unipi.hadoop;

import java.io.*;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import it.unipi.hadoop.KMeansUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;

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

			// Salviamo i centroidi appena generati
			saveCentroids(centroids, "kmeans/oldCentroids.txt");

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

			// this.saveCentroid(newCentroid, "kmeans/newCentroids.txt");
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

	public static PointWritable[] loadCentroids(String mod, String filename) throws IOException {
		List<PointWritable> centroids = new ArrayList<>();
		String line;

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		switch (mod) {
			case "f":
				Path filePath = new Path(filename);
				if (!fs.exists(filePath)) {
					throw new IOException("File does not exist: " + filename);
				}
				try (FSDataInputStream in = fs.open(filePath);
						BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
					while ((line = reader.readLine()) != null) {
						String[] parts = line.split("\\[");
						IntWritable id = new IntWritable(Integer.parseInt(parts[0].trim()));
						String[] coordStrings = parts[1].substring(0, parts[1].length() - 1).split(",\\s");
						double[] coordinates = Arrays.stream(coordStrings).mapToDouble(Double::parseDouble).toArray();
						centroids.add(new PointWritable(coordinates, id));
					}
				}
				System.out.println("CENTROIDS LENGHT: " + centroids.size());
				break;

			case "d":
				Path dirPath = new Path(filename);
				if (!fs.exists(dirPath) || !fs.isDirectory(dirPath)) {
					throw new IOException("Directory does not exist: " + filename);
				}
				FileStatus[] fileStatuses = fs.listStatus(dirPath, path -> path.getName().startsWith("part"));
				for (FileStatus fileStatus : fileStatuses) {
					try (FSDataInputStream in = fs.open(fileStatus.getPath());
							BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
						while ((line = reader.readLine()) != null) {
							String[] parts = line.split("\\[");
							IntWritable id = new IntWritable(Integer.parseInt(parts[0].trim()));
							String[] coordStrings = parts[1].substring(0, parts[1].length() - 1).split(",\\s");
							double[] coordinates = Arrays.stream(coordStrings).mapToDouble(Double::parseDouble)
									.toArray();
							centroids.add(new PointWritable(coordinates, id));
						}
					}
				}
				System.out.println("CENTROIDS LENGHT: " + centroids.size());
				break;

			default:
				break;
		}

		// Sort centroids by id
		centroids.sort(Comparator.comparingInt(PointWritable::get_int_ID));

		return centroids.toArray(new PointWritable[0]);
	}

	public static void saveCentroids(PointWritable[] centroids, String filename) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path outputPath = new Path(filename);

		if (fs.exists(outputPath)) {
			// If file exists, remove it to start fresh
			fs.delete(outputPath, true);
		}

		FSDataOutputStream out = fs.create(outputPath);

		for (PointWritable centroid : centroids) {
			out.writeBytes(centroid.get_int_ID() + " " + centroid.toString());
			out.writeBytes("\n");
		}

		out.close();

	}

	public static double calculateCentroidDifference(PointWritable centroid1, PointWritable centroid2) {
		double[] coordinates1 = centroid1.getCoordinates();
		double[] coordinates2 = centroid2.getCoordinates();

		if (coordinates1.length != coordinates2.length) {
			throw new IllegalArgumentException("Centroids must have the same dimension.");
		}

		double sum = 0.0;
		for (int i = 0; i < coordinates1.length; i++) {
			double diff = coordinates1[i] - coordinates2[i];
			sum += diff * diff;
		}

		return Math.sqrt(sum);
	}

	public static void main(final String[] args) throws Exception {
		final Configuration conf = new Configuration();
		final Job job = new Job(conf, "kmeans");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		int k = Integer.parseInt(otherArgs[1]);
		int d = Integer.parseInt(otherArgs[2]);
		int MaxIterations = Integer.parseInt(otherArgs[3]);

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
		job.setReducerClass(KMeansReducer.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[4]));

		double threshold = 0.00001; // Define a threshold for the centroid difference
		boolean converged = false;
		boolean maxIterationReached = false;
		int count = 0;

		System.out.println("THRESHOLD: " + threshold);

		while (!converged && !maxIterationReached) {
			count++;
			System.out.println("CICLO: n -> " + count);
			job.waitForCompletion(true);

			// Load the old and new centroids from HDFS
			PointWritable[] oldCentroids = loadCentroids("f", "kmeans/oldCentroids.txt");
			PointWritable[] newCentroids = loadCentroids("d", args[4]); // outputTestxx/part*
			//
			// Calculate the difference between the old and new centroids
			double difference = 0.0;
			for (int i = 0; i < k; i++) {
				double temp = calculateCentroidDifference(oldCentroids[i], newCentroids[i]);
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

			// DEBUG
			System.out.println("CENTROIDI VECCHI:");
			for (int i = 0; i < k; i++) {
				System.out.println(i + " " + oldCentroids[i].toString());
			}
			System.out.println("\nCENTROIDI NUOVI:");
			for (int i = 0; i < k; i++) {
				System.out.println(i + " " + newCentroids[i].toString());
			}
			System.out.println("\n");

			saveCentroids(newCentroids, "kmeans/oldCentroids.txt");
		}
	}

}
