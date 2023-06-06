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

public class KMeansCombiner
        extends Reducer<IntWritable, PointWritable, IntWritable, PointWritable> {

    @Override
    protected void reduce(IntWritable key, Iterable<PointWritable> values, Context context)
            throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();
        // Retrieve k and d from the configuration
        int k = conf.getInt("k", -1);
        int d = conf.getInt("d", -1);

        // Initialize partial sum value to 0
        PointWritable partialSum = new PointWritable(d);

        // for each datapoint passed to the combiner, sum it to the partial sum of
        // the current cluster (given from the key)
        for (PointWritable point : values) {
            partialSum.sumPoint(point);
        }

        // Write the cluster id and the cluster partial sum to the context
        context.write(key, partialSum);
    }

}