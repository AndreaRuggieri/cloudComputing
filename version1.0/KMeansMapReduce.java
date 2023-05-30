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

public class KMeansMapReduce {

	public static class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, PointWritable> {
		private PointWritable[] centroids;

		// private final IntWritable reducerKey = new IntWritable();
		// private final PointWritable reducerValue = new PointWritable();

		private int k, d;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();

			// Retrieve k and d from the configuration
			this.k = conf.getInt("k", -1);
			this.d = conf.getInt("d", -1);

			centroids = loadCentroids("f", "kmeans/oldCentroids.txt");

			// Genera centroidi naive method
			// centroids = PointWritable.generateCentroids(k, d);

			// try {
			// debugToFile(centroids.toString());
			// } catch (IOException e) {
			// e.printStackTrace();
			// }

		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// Convert the input text to a PointWritable
			PointWritable point = textToPoint(value, context);

			// If the point is null, skip this record
			if (point == null) {
				return;
			}

			// debug
			// System.out.println("ID: " + point.getID() + " - COO: " +
			// Arrays.toString(point.getCoordinates()));

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

			// System.out.println("DEBUG: " + Arrays.toString(tokens) + "Lunghezza: " +
			// tokens.length);
			// System.out.println("DEBUG: d -> " + d);
			// debugToFile(line);

			// debug

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
				partialSum.sumPoint(new PointWritable(point.getCoordinates(), point.getID()));
			}

			// Write the cluster id and the cluster sum to the context
			context.write(key, new PointWritable(partialSum));
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
			int count = clusterSum.getNumeroPuntiCluster();
			double[] centroid = new double[sum.length];

			// System.out.println("======== CALCOLO NUOVO CENTROIDE ========");

			for (int i = 0; i < sum.length; i++) {
				// System.out.println("Centroide: " + clusterSum.toString() + " Conteggio: " +
				// count);
				// try {
				// debugToFile("SommaParziale " + id + ": " +
				// clusterSum.toString()
				// + " Conteggio: " + count);
				// } catch (IOException e) {
				// e.printStackTrace();
				// }
				centroid[i] = sum[i] / count;
			}

			return new PointWritable(centroid, id);
		}

	}

	public static PointWritable[] getRandomCentroids(String filename, int k) throws IOException {
		List<PointWritable> centroids = new ArrayList<>();
		String line;

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		Path filePath = new Path(filename);
		if (!fs.exists(filePath)) {
			throw new IOException("File does not exist: " + filename);
		}
		try (FSDataInputStream in = fs.open(filePath);
				BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
			int i = 0;
			while (i < k && (line = reader.readLine()) != null) {
				i++;
				String[] parts = line.split(",");
				IntWritable id = new IntWritable(i);
				double[] coordinates = new double[parts.length - 1];
				for (int j = 1; j < parts.length; j++) {
					coordinates[j - 1] = Double.parseDouble(parts[j]);
				}
				centroids.add(new PointWritable(coordinates, id));
			}
		}
		System.out.println("CENTROIDS LENGHT: " + centroids.size());

		// Sort centroids by id
		centroids.sort(Comparator.comparingInt(PointWritable::get_int_ID));

		return centroids.toArray(new PointWritable[0]);
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

		// System.out.println("THRESHOLD: " + threshold);

		PointWritable[] centroids = getRandomCentroids("input.txt", k);
		// Salviamo i centroidi appena generati
		saveCentroids(centroids, "kmeans/oldCentroids.txt");

		while (!converged && !maxIterationReached) {

			final Job job = new Job(conf, "kmeans");
			// Add k and d to the Configuration
			job.getConfiguration().setInt("k", k);
			job.getConfiguration().setInt("d", d);

			// Set one reducer per cluster
			// job.setNumReduceTasks(k);

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

			System.out.println("CICLO: n -> " + count);

			// Load the old and new centroids from HDFS
			PointWritable[] oldCentroids = loadCentroids("f", "kmeans/oldCentroids.txt");
			PointWritable[] newCentroids = loadCentroids("d", args[4] + "/iteration" + count); // outputTestxx/part*
			count++;

			// Calculate the difference between the old and new centroids
			double difference = 0.0;
			for (int i = 0; i < k; i++) {
				double temp = calculateCentroidDifference(oldCentroids[i], newCentroids[i]);
				if (temp > difference) {
					difference = temp;
				}
			}

			System.out.println("DIFFERENZA: " + difference);

			//// If the difference is less than the threshold, the algorithm has converged
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
			// System.out.println("CENTROIDI VECCHI:");
			// for (int i = 0; i < k; i++) {
			// System.out.println(i + " " + oldCentroids[i].toString());
			// }
			// System.out.println("\nCENTROIDI NUOVI:");
			// for (int i = 0; i < k; i++) {
			// System.out.println(i + " " + newCentroids[i].toString());
			// }
			// System.out.println("\n");

			saveCentroids(newCentroids, "kmeans/oldCentroids.txt");
		}
	}

}
