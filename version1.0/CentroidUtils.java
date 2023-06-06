package it.unipi.hadoop;

import it.unipi.hadoop.PointWritable;

import java.io.*;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;

public class CentroidUtils {

    public static PointWritable[] getStartingCentroids(String filename, int k) throws IOException {
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
                double[] coordinates = new double[parts.length];
                for (int j = 0; j < parts.length; j++) {
                    coordinates[j] = Double.parseDouble(parts[j]);
                }
                centroids.add(new PointWritable(coordinates, id));
            }
        }

        System.out.println("CENTROIDS LENGHT: " + centroids.size());

        for (PointWritable centroid : centroids) {
            System.out.println(centroid.toString());
        }

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
}
