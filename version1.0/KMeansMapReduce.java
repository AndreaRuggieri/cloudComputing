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
import java.util.List;

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
			// for (PointWritable centroid : centroids) {
			// this.saveCentroid(centroid, "kmeans/oldCentroids.txt");
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

	public PointWritable[] loadCentroids(String mod, String filename) throws IOException {
		List<PointWritable> centroids = new ArrayList<>();

		String line;

		// TODO: aggiustare il modo in cui si va a fare il parsing dei centroidi dal
		// file di testo

		switch (mod) {
			case "f":
				BufferedReader reader = new BufferedReader(new FileReader(filename));

				while ((line = reader.readLine()) != null) {
					String[] parts = line.split("\\s+");
					IntWritable id = new IntWritable(Integer.parseInt(parts[0]));
					String[] coordStrings = parts[1].substring(1, parts[1].length() - 1).split(",");
					double[] coordinates = Arrays.stream(coordStrings).mapToDouble(Double::parseDouble).toArray();
					centroids.add(new PointWritable(coordinates, id));
				}
				break;

			case "d":
				File dir = new File(filename);

				// Check if the directory exists and it is indeed a directory
				if (dir.exists() && dir.isDirectory()) {
					// List all files matching the pattern "part*"
					File[] files = dir.listFiles(new FilenameFilter() {
						@Override
						public boolean accept(File dir, String name) {
							return name.startsWith("part");
						}
					});

					// Print the names of the matching files
					for (File file : files) {
						System.out.println(file.getName());
						while ((line = reader.readLine()) != null) {
							String[] parts = line.split("\\s+");
							IntWritable id = new IntWritable(Integer.parseInt(parts[0]));
							String[] coordStrings = parts[1].substring(1, parts[1].length() - 1).split(",");
							double[] coordinates = Arrays.stream(coordStrings).mapToDouble(Double::parseDouble)
									.toArray();
							centroids.add(new PointWritable(coordinates, id));
						}
					}
				} else {
					System.out.println("The specified path either does not exist or is not a directory.");
				}
				break;

			default:
				break;
		}

		BufferedReader reader = new BufferedReader(new FileReader(filename));

		reader.close();
		return centroids.toArray(new PointWritable[0]);
	}

	public void saveCentroids(PointWritable[] centroids, String filename) throws IOException {
		BufferedWriter writer = new BufferedWriter(new FileWriter(filename));
		// TODO: verificare che stampi consistentemente a come si vanno a leggere gli
		// oldCentroids
		for (PointWritable centroid : centroids) {
			writer.write(centroid.toString());
			writer.newLine();
		}

		writer.close();
	}

	public double calculateCentroidDifference(PointWritable centroid1,
			PointWritable centroid2) {
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

		double threshold = 0.01; // Define a threshold for the centroid difference
		boolean converged = false;
		boolean maxIterationReached = false;
		int count = 0;

		while (!converged && !maxIterationReached) {
			count++;
			// System.out.println("CICLO: n -> " + count);
			job.waitForCompletion(true);

			// TODO: sistemare verifica di convergenza con soglia

			// Load the old and new centroids from HDFS
			PointWritable[] oldCentroids = loadCentroidsFromHDFS("f", "kmeans/oldCentroids.txt");
			PointWritable[] newCentroids = loadCentroidsFromHDFS("d", args[4]); // outputTestxx/part*
			//
			// Calculate the difference between the old and new centroids
			// double difference = calculateCentroidDifference(oldCentroids, newCentroids);

			// If the difference is less than the threshold, the algorithm has converged
			// if (difference < threshold) {
			// converged = true;
			// }

			if (count >= MaxIterations) {
				maxIterationReached = true;
			}
		}
	}

}
