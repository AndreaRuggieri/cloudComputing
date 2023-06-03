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

import org.apache.hadoop.mapreduce.Mapper;

public class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, PointWritable> {
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