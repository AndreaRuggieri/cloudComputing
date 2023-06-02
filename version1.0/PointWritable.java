package it.unipi.hadoop;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;

import java.io.DataOutput;
import java.io.IOException;
import java.io.DataInput;
import java.util.Random;

public class PointWritable implements Writable {
    // Features added to implement an object can work both as Point and ClusterSum
    private double[] coordinates;
    private IntWritable id; // ID field added
    private int clusterElementsNumber;

    public PointWritable() {
    }

    // copy constructor
    public PointWritable(PointWritable point) {
        this.coordinates = new double[point.getCoordinates().length];
        for (int i = 0; i < coordinates.length; i++) {
            this.coordinates[i] = point.getCoordinates()[i];
        }
        this.id = new IntWritable(point.get_int_ID());
        this.clusterElementsNumber = point.getClusterElementsNumber();
    }

    public PointWritable(int d) { // constructor for a 0-initialized point of dimension d
        this.id = new IntWritable(0);
        this.coordinates = new double[d];
        for (int i = 0; i < d; i++) {
            this.coordinates[i] = 0;
        }
        // Anche in questo costruttore bisogna settare num a 0
        this.clusterElementsNumber = 0;
    }

    public PointWritable(double[] coordinates, IntWritable id) { // constructor now accepts an id
        this.coordinates = coordinates;
        this.id = id;
        // Anche in questo costruttore bisogna settare num a 0
        this.clusterElementsNumber = 0;
    }

    public PointWritable(double[] coordinates, IntWritable id, int numero) { // constructor now accepts an id
        this.coordinates = coordinates;
        this.id = id;
        // Anche in questo costruttore bisogna settare num a 0
        this.clusterElementsNumber = numero;
    }

    public void set(int index, double value) {
        this.getCoordinates()[index] = value;
    }

    public void set(double[] val) {
        for (int i = 0; i < val.length; i++)
            this.set(i, val[i]);
    }

    public void set(double[] val, int clusterElementsNumber) {
        for (int i = 0; i < val.length; i++)
            this.set(i, val[i]);
        this.setClusterElementsNumber(clusterElementsNumber);
    }

    public IntWritable getID() { // getter for id
        return this.id;
    }

    public int getClusterElementsNumber() {
        return this.clusterElementsNumber;
    }

    public void setClusterElementsNumber(int new_clusterElementsNumber) {
        this.clusterElementsNumber = new_clusterElementsNumber;
    }

    public int get_int_ID() { // getter for id
        return this.id.get();
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
        // Write id and clusterElementsNumber
        id.write(out);
        out.writeInt(clusterElementsNumber);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int length = in.readInt();
        coordinates = new double[length];
        for (int i = 0; i < length; i++) {
            coordinates[i] = in.readDouble();
        }
        // Read id and clusterElementsNumber
        id = new IntWritable();
        id.readFields(in);
        clusterElementsNumber = in.readInt();
    }

    public void sumPoint(PointWritable point) {
        for (int i = 0; i < this.coordinates.length; i++) {
            this.coordinates[i] += point.getCoordinates()[i];
        }
        if (point.getClusterElementsNumber() > 0) {
            this.clusterElementsNumber += point.getClusterElementsNumber();
        } else {
            this.clusterElementsNumber++;
        }
    }

    public IntWritable getNearestCentroid(PointWritable[] centroids) {
        IntWritable nearestCentroid = new IntWritable(0);
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
                nearestCentroid = centroid.getID();
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
        sb.append("]"); // , num punti appartenenti al cluster: " clusterElementsNumber );
        return sb.toString().trim();
    }

}
