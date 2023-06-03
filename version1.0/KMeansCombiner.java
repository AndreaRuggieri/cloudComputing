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