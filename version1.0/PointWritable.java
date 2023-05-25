package tmp;

import org.apache.hadoop.io.Writable;

public class PointWritable implements Writable {
    private double[] coordinates;

    public PointWritable() {
        this(new double[0]);
    }

    public static PointWritable[] generateCentroids(int k, int d) {
        PointWritable[] centroidi = new PointWritable[k];
        for (int i = 0; i < k; i++) {
            PointWritable pw = new PointWritable();
            for (int j = 0; j < d; j++) {
                Random random = new Random();
                pw.set(pw, j, random.nextDouble() * 100);
            }
        }
        return centroidi;
    }

    public void set(PointWritable pw, int index, double value) {
        pw.getCoordinates()[index] = value;
    }

    public PointWritable(double[] coordinates) {
        this.coordinates = coordinates;
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

    private PointWritable getNearestCentroid(PointWritable point, List<PointWritable> centroids) {
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

}
