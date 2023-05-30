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

    // Features added to implement an object can work both as Point and ClusterSum

    private int numero_punti_cluster;
    // sum_features non mi serve, come array uso coordinates

    // public PointWritable() {
    // // Il costruttore vuoto:
    // // - inizializza l'id a 0
    // // - crea un array di dimensione uno, tanto è possibile cambiarlo con la
    // // relativa set
    // // - inizializza il numero di punti presenti nel relativo cluster a 0
    // // - inizializza la somma (parziale) delle features presenti nel cluster a 0
    // this.coordinates = new double[1];
    // this.coordinates[0] = -0;
    // this.id = new IntWritable(-1);
    // this.numero_punti_cluster = 0; // all'inizio non ci sono punti nel cluster
    // // this(new double[0], new IntWritable(-1)); // default id is -1, indicating
    // no
    // // id assigned
    // }

    public PointWritable(int d) { // constructor for a 0-initialized point of dimension d
        this.id = 0;
        this.coordinates = new double[d];
        for (int i = 0; i < d; i++) {
            this.coordinates[i] = 0;
        }
        // Anche in questo costruttore bisogna settare num a 0
        this.numero_punti_cluster = 0;
    }

    public PointWritable(double[] coordinates, IntWritable id) { // constructor now accepts an id
        this.coordinates = coordinates;
        this.id = id;
        // Anche in questo costruttore bisogna settare num a 0
        this.numero_punti_cluster = 0;
    }

    public PointWritable(double[] coordinates, IntWritable id, int numero) { // constructor now accepts an id
        this.coordinates = coordinates;
        this.id = id;
        // Anche in questo costruttore bisogna settare num a 0
        this.numero_punti_cluster = numero;
    }

    public static PointWritable[] generateCentroids(int k, int d) {
        // Creo un array di punti, inizialmente vuoto
        PointWritable[] centroidi = new PointWritable[k];
        for (int i = 0; i < k; i++) {
            // Per ogni punto che devo metterci dentro inserisco un double casuale
            PointWritable pw = new PointWritable(new double[d], new IntWritable(i)); // assign id during generation
            for (int j = 0; j < d; j++) {
                // Estraggo il numero random, i-esima feature (coordinata) del mio centroide
                Random random = new Random();
                // Setto la feature nell'i-esima posizione del punto in questione
                pw.set(pw, j, random.nextDouble() * 1000);
            }
            centroidi[i] = pw; // assign the generated PointWritable to the array
        }
        return centroidi;
    }

    public void set(int index, double value) {
        this.getCoordinates()[index] = value;
    }

    public void set(double[] val) {
        for (int i = 0; i < val.length; i++)
            this.set(i, val[i]);
    }

    public void set(double[] val, int numero_punti_cluster) {
        for (int i = 0; i < val.length; i++)
            this.set(i, val[i]);
        this.setNumeroPuntiCluster(numero_punti_cluster);
    }

    public IntWritable getID() { // getter for id
        return this.id;
    }

    public int getNumeroPuntiCluster() {
        return this.numero_punti_cluster;
    }

    public void setNumeroPuntiCluster(int new_numero_punti_cluster) {
        this.numero_punti_cluster = new_numero_punti_cluster;
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
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int length = in.readInt();
        coordinates = new double[length];
        for (int i = 0; i < length; i++) {
            coordinates[i] = in.readDouble();
        }
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
        sb.append("]"); // , num punti appartenenti al cluster: " numero_punti_cluster );
        return sb.toString().trim();
    }

    public PointWritable sumPoint(PointWritable point) {
        for (int i = 0; i < this.coordinates.length; i++) {
            this.coordinates[i] += point.getCoordinates()[i];
            this.numero_punti_cluster++;
        }
    }

}
