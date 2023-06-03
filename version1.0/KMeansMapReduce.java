package it.unipi.hadoop;

import java.io.*;
import java.util.*;

import javax.naming.Context;

import org.apache.hadoop.io.*;
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
import it.unipi.hadoop.KMeansMapper;
import it.unipi.hadoop.KMeansCombiner;
import it.unipi.hadoop.KMeansReducer;

public class KMeansMapReduce {

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
		long start = 0;
		long end = 0;
		long startIC = 0;
		long endIC = 0;

		start = System.currentTimeMillis();

		final Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 6) {
			System.err.println("Usage: <input> <k> <d> <max iterations> <threshold> <output>");
			System.exit(1);
		}

		// parameters
		int k = Integer.parseInt(otherArgs[1]); // number of clusters
		int d = Integer.parseInt(otherArgs[2]); // dimension of a datapoint
		int MaxIterations = Integer.parseInt(otherArgs[3]); // max number of iterations
		double threshold = Double.parseDouble(otherArgs[4]);

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
			FileOutputFormat.setOutputPath(job, new Path(args[5] + "/iteration" + count));

			// if a job fails, exit the program with code 1
			if (!job.waitForCompletion(true)) {
				System.err.println("Iteration " + count + " failed.");
				System.exit(1);
			}

			// Load the old and new centroids from HDFS
			PointWritable[] oldCentroids = CentroidUtils.loadCentroids("f", "kmeans/oldCentroids.txt");
			PointWritable[] newCentroids = CentroidUtils.loadCentroids("d", args[5] + "/iteration" + count); // outputTestxx/part*
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

		end = System.currentTimeMillis();

		end -= start;

		end /= 1000;

		System.out.println("execution time: " + end + " s");
		System.out.println("n_iter: " + count);

		System.exit(0);
	}

}
