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

import org.apache.hadoop.mapreduce.Reducer;

public class KMeansReducer
        extends Reducer<IntWritable, PointWritable, IntWritable, PointWritable> {

    @Override
    protected void reduce(IntWritable key, Iterable<PointWritable> partialSums, Context context)
            throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();
        // Retrieve k and d from the configuration
        int k = conf.getInt("k", -1);
        int d = conf.getInt("d", -1);

        // Initialize cluster sum value to 0
        PointWritable clusterSum = new PointWritable(d);

        // for each partial sum (or datapoint in the case the combiner is not exploit)
        // passed to the reducer, sum it to the total sum of the current cluster
        // (given from the key)
        for (PointWritable partialSum : partialSums) {
            clusterSum.sumPoint(partialSum);
        }

        // Calculate the new centroid
        PointWritable newCentroid = calculateNewCentroid(clusterSum, key);

        // Write the cluster id and the new centroid to the context
        context.write(key, newCentroid);
    }

    private PointWritable calculateNewCentroid(PointWritable clusterSum, IntWritable id) {
        // get the components of the total sum of the cluster points
        double[] sum = clusterSum.getCoordinates();
        // get the number of the points belonging to the cluster
        int count = clusterSum.getClusterElementsNumber();
        // allocate an array for the coordinates of the new centroid
        double[] centroid = new double[sum.length];

        // calculate each coordinate of the new centroid performing the mean of each
        // feature of the cluster points
        for (int i = 0; i < sum.length; i++) {
            centroid[i] = sum[i] / count;
        }

        // return the new centroid
        return new PointWritable(centroid, id);
    }

}