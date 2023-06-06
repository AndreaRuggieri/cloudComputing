package it.unipi.hadoop;

import java.io.*;
import java.util.*;

import javax.naming.Context;

import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;

import it.unipi.hadoop.CentroidUtils;
import it.unipi.hadoop.KMeansMapper;
import it.unipi.hadoop.KMeansCombiner;
import it.unipi.hadoop.KMeansReducer;

public class KMeansMapReduce {

	public static void debugToFile(String text) throws IOException {
		// function to write debug strings to a file in HDFS
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		Random random = new Random();

		int c = (int) (random.nextDouble() * 1000);

		// to avoid conflict between concurrent MapReduce tasks, each one of them
		// geneate a debug file with a random name
		Path outputPath = new Path("debug/debug" + c + ".txt");

		// If file exists already, remove it
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}

		// create and write the debugxxx.txt file
		FSDataOutputStream out = fs.create(outputPath);
		out.writeBytes(text);
		out.writeBytes("\n");
		out.close();
	}

	public static void saveOutputStats(String inputFile, String dir, Long time, int n_iter, int k, int d,
			String endReason) throws IOException {
		// write a final file with the overall execution stats
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		Path outputPath = new Path(dir + "/stat.txt");

		// If file exists already, remove it
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}

		// create and write the stat.txt file
		FSDataOutputStream out = fs.create(outputPath);
		out.writeBytes("Input file: " + inputFile);
		out.writeBytes("\n");
		out.writeBytes("Num Clusters: : " + k + " - Data Dimension: " + d);
		out.writeBytes("\n");
		out.writeBytes("EXECUTION TIME: " + time + " s");
		out.writeBytes("\n");
		out.writeBytes("N. ITERATIONS: " + n_iter);
		out.writeBytes("\n");
		out.writeBytes("END: " + endReason);
		out.writeBytes("\n");
		out.close();
	}

	public static void main(final String[] args) throws Exception {
		long startTime = 0;
		long endTime = 0;
		long startIC = 0;
		long endIC = 0;
		String endReason = "";

		// start the timer
		startTime = System.currentTimeMillis();

		final Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		// check the number of arguments passed
		if (otherArgs.length != 6) {
			System.err.println("Usage: <input> <k> <d> <max iterations> <threshold> <output>");
			System.exit(1);
		}

		// parameters parsing
		String input = otherArgs[0]; // input file
		int k = Integer.parseInt(otherArgs[1]); // number of clusters
		int d = Integer.parseInt(otherArgs[2]); // dimension of a datapoint
		int MaxIterations = Integer.parseInt(otherArgs[3]); // max number of iterations
		double threshold = Double.parseDouble(otherArgs[4]);
		String output = otherArgs[5]; // output directory

		// set utility variables
		boolean converged = false;
		boolean maxIterationReached = false;
		int count = 0;

		// print the given threshold
		System.out.println("Given Threshold: " + threshold);

		// get the starting centroids from the dataset
		PointWritable[] centroids = CentroidUtils.getStartingCentroids(input, k);
		// save the first centroids to HDFS
		CentroidUtils.saveCentroids(centroids, "kmeans/oldCentroids.txt");

		while (!converged && !maxIterationReached) {
			System.out.println("Cycle n.: -> " + (count + 1));

			final Job job = new Job(conf, "kmeans");
			// Add k and d in the Configuration
			job.getConfiguration().setInt("k", k);
			job.getConfiguration().setInt("d", d);

			// Set one reducer per cluster
			job.setNumReduceTasks(k);

			// setting of the classes
			job.setJarByClass(KMeansMapReduce.class);

			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(PointWritable.class);

			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(PointWritable.class);

			job.setMapperClass(KMeansMapper.class);
			job.setCombinerClass(KMeansCombiner.class);
			job.setReducerClass(KMeansReducer.class);

			// setting of input and output
			FileInputFormat.addInputPath(job, new Path(input));
			FileOutputFormat.setOutputPath(job, new Path(output + "/iteration" + count));

			// if a job fails, exit the program with code 1
			if (!job.waitForCompletion(true)) {
				System.err.println("Iteration " + count + " failed.");
				System.exit(1);
			}

			// Load the old and new centroids from HDFS
			PointWritable[] oldCentroids = CentroidUtils.loadCentroids("f", "kmeans/oldCentroids.txt");
			PointWritable[] newCentroids = CentroidUtils.loadCentroids("d", output + "/iteration" + count);

			// increment iterations counter
			count++;

			// Calculate the difference between the old and new centroids
			double difference = 0.0;
			for (int i = 0; i < newCentroids.length; i++) {
				// temp will contain the maximum euclidean distance between the old centroids
				// and the relative new ones
				double temp = CentroidUtils.calculateCentroidDifference(oldCentroids[i], newCentroids[i]);
				if (temp > difference) {
					difference = temp;
				}
			}

			// print the current maximum distance
			System.out.println("Difference: " + difference);

			// If the difference is less than the threshold, the algorithm has converged
			if (difference < threshold) {
				converged = true;
				endReason = "threshold";
				System.out.println("END: threshold.");
			}

			// if the number of max iterations is reached, the algorithm stops
			if (count >= MaxIterations) {
				maxIterationReached = true;
				endReason = "max iterations reached";
				System.out.println("END: max iterations reached.");
			}

			// save the new centroids in HDFS
			CentroidUtils.saveCentroids(newCentroids, "kmeans/oldCentroids.txt");
		}

		// stop the timer
		endTime = System.currentTimeMillis();
		// take the difference
		endTime -= startTime;
		// convert from milliseconds to seconds
		endTime /= 1000;

		try {
			// save the final stats in HDFS
			saveOutputStats(input, output, endTime, count, k, d, endReason);

		} catch (IOException e) {
			e.printStackTrace();
		}

		// print useful execution informations
		System.out.println("EXECUTION TIME: " + endTime + " s");
		System.out.println("N. ITERATIONS: " + count);

		System.exit(0);
	}

}
