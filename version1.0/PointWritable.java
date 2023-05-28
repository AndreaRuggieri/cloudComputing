package it.unipi.hadoop;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;

import java.io.DataOutput;
import java.io.IOException;
import java.io.DataInput;
import java.util.Random;

public class PointWritable implements Writable {
    private double[] coordinates;
    private IntWritable id; // ID field added

    public PointWritable() {
        this(new double[0], new IntWritable(-1)); // default id is -1, indicating no id assigned
    }

    public static PointWritable[] generateCentroids(int k, int d) {
        PointWritable[] centroidi = new PointWritable[k];
        for (int i = 0; i < k; i++) {
            PointWritable pw = new PointWritable(new double[d], new IntWritable(i)); // assign id during generation
            for (int j = 0; j < d; j++) {
                Random random = new Random();
                pw.set(pw, j, random.nextDouble() * 100);
            }
            centroidi[i] = pw; // assign the generated PointWritable to the array
        }
        return centroidi;
    }

    public void set(PointWritable pw, int index, double value) {
        pw.getCoordinates()[index] = value;
    }

    public PointWritable(double[] coordinates, IntWritable id) { // constructor now accepts an id
        this.coordinates = coordinates;
        this.id = id;
    }

    public IntWritable getID() { // getter for id
        return this.id;
    }

    public double[] getCoordinates() {
        return coordinates;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(coordinates.length);
        for (double coordinate : coordinates) {
            out.writeDouble(coordinate);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int length = in.readInt();
        coordinates = new double[length];
        for (int i = 0; i < length; i++) {
            coordinates[i] = in.readDouble();
        }
    }

    public void set(PointWritable pw, double[] val) {
        for (int i = 0; i < val.length; i++)
            pw.set(pw, i, val[i]);
    }

    public PointWritable getNearestCentroid(PointWritable[] centroids) {
        PointWritable nearestCentroid = null;
        double nearestDistance = Double.MAX_VALUE;

        for (PointWritable centroid : centroids) {

            double sum = 0.0;
            for (int i = 0; i < this.coordinates.length; i++) {
                double diff = this.coordinates[i] - centroid.coordinates[i];
                sum += diff * diff;
            }
            double distance = Math.sqrt(sum);

            if (distance < nearestDistance) {
                nearestDistance = distance;
                nearestCentroid = centroid;
            }
        }

        return nearestCentroid;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (double coord : coordinates) {
            sb.append(coord);
            sb.append(", ");
        }
        sb.delete(sb.length() - 2, sb.length());
        sb.append("]");
        return sb.toString().trim();
    }

}
