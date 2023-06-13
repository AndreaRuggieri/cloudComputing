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
        // parse the first k records of the dataset as starting centroids

        List<PointWritable> centroids = new ArrayList<>();
        String line;

        Configuration conf = new Configuration();  //istanza che contiene le configurazioni 
        FileSystem fs = FileSystem.get(conf); //prende un filesystem e ci inserisce i dati della configurazione

        Path filePath = new Path(filename);
        // check if the input file exists, throw exception if not
        if (!fs.exists(filePath)) {
            throw new IOException("File does not exist: " + filename);
        }

        try (FSDataInputStream in = fs.open(filePath); //utilizzo try.catch per ogni operazione di scrittura e lettura di java
                BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
            int i = 0;
            while (i < k && (line = reader.readLine()) != null) {
                i++;
                // split the coordinate that are separated by comma
                String[] parts = line.split(",");
                // assign the id
                IntWritable id = new IntWritable(i);
                // allocate an array for the coordinates
                double[] coordinates = new double[parts.length];
                // parse each coordinate
                for (int j = 0; j < parts.length; j++) {
                    coordinates[j] = Double.parseDouble(parts[j]);
                }
                // instatiate the new centroid and add it to the array
                centroids.add(new PointWritable(coordinates, id));
            }
        }

        // Convert the list of centroids to an array and return it
        return centroids.toArray(new PointWritable[0]);
    }

    public static PointWritable[] loadCentroids(String mod, String filename) throws IOException {
        // method to load centroids from a file or a directory

        // initialize a list to store the centroids
        List<PointWritable> centroids = new ArrayList<>();
        String line;

        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);

        // switch case based on the provided mode
        switch (mod) {
            // used when we need to read a single, direct file (i.e. oldCentroids.txt)
            case "f":
                // create a Path object with the filename
                Path filePath = new Path(filename);

                // check if the file exists, if not throw an exception
                if (!fs.exists(filePath)) {
                    throw new IOException("File does not exist: " + filename);
                }

                // open the file and read it line by line
                try (FSDataInputStream in = fs.open(filePath);
                        BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
                    while ((line = reader.readLine()) != null) {
                        // split the line on the "[" character
                        String[] parts = line.split("\\[");

                        // parse the ID from the first part
                        IntWritable id = new IntWritable(Integer.parseInt(parts[0].trim()));

                        // parse the coordinates from the second part
                        String[] coordStrings = parts[1].substring(0, parts[1].length() - 1).split(",\\s");
                        double[] coordinates = Arrays.stream(coordStrings).mapToDouble(Double::parseDouble).toArray();

                        // add the new centroid to the list
                        centroids.add(new PointWritable(coordinates, id));
                    }
                }
                break;

            // used when we need to read all the part* files of an iteration
            // in this case the argument is the iteration directory
            case "d":
                // create a Path object with the directory name
                Path dirPath = new Path(filename);

                // check if the directory exists and it is indeed a directory, if not throw an
                // exception
                if (!fs.exists(dirPath) || !fs.isDirectory(dirPath)) {
                    throw new IOException("Directory does not exist: " + filename);
                }

                // get the list of files in the directory that start with "part"
                FileStatus[] fileStatuses = fs.listStatus(dirPath, path -> path.getName().startsWith("part"));

                // loop over each file in the directory
                for (FileStatus fileStatus : fileStatuses) {
                    // open the file and read it line by line
                    try (FSDataInputStream in = fs.open(fileStatus.getPath());
                            BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
                        while ((line = reader.readLine()) != null) {
                            // split the line on the "[" character
                            String[] parts = line.split("\\[");

                            // parse the ID from the first part
                            IntWritable id = new IntWritable(Integer.parseInt(parts[0].trim()));

                            // parse the coordinates from the second part
                            String[] coordStrings = parts[1].substring(0, parts[1].length() - 1).split(",\\s");
                            double[] coordinates = Arrays.stream(coordStrings).mapToDouble(Double::parseDouble)
                                    .toArray();

                            // add the new centroid to the list
                            centroids.add(new PointWritable(coordinates, id));
                        }
                    }
                }
                break;

            default:
                // do nothing if the mode is not "f" or "d"
                break;
        }

        // sort the list of centroids by their ID
        centroids.sort(Comparator.comparingInt(PointWritable::get_int_ID));

        // convert the list of centroids to an array and return it
        return centroids.toArray(new PointWritable[0]);
    }

    public static void saveCentroids(PointWritable[] centroids, String filename) throws IOException {
        // method to save centroids to a file

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Path outputPath = new Path(filename);

        // if the file already exists, delete it to start fresh
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        // create the output file
        FSDataOutputStream out = fs.create(outputPath);

        // write each centroid to the output file
        for (PointWritable centroid : centroids) {
            out.writeBytes(centroid.get_int_ID() + " " + centroid.toString());
            out.writeBytes("\n");
        }

        // close the output stream
        out.close();
    }

    public static double calculateCentroidDifference(PointWritable centroid1, PointWritable centroid2) {
        // method to calculate the difference between two centroids

        // get the coordinates of the centroids
        double[] coordinates1 = centroid1.getCoordinates();
        double[] coordinates2 = centroid2.getCoordinates();

        // check if the centroids have the same dimension
        if (coordinates1.length != coordinates2.length) {
            throw new IllegalArgumentException("Centroids must have the same dimension.");
        }

        // initialize the sum
        double sum = 0.0;

        // calculate the sum of the squared differences
        for (int i = 0; i < coordinates1.length; i++) {
            double diff = coordinates1[i] - coordinates2[i];
            sum += diff * diff;
        }

        // return the square root of the sum (Euclidean distance)
        return Math.sqrt(sum);
    }

}
