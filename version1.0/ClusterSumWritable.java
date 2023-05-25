package tmp;

import org.apache.hadoop.io.Writable;

public class ClusterSumWritable implements Writable {
    private double[] sumCoordinates;
    private int count;

    public ClusterSumWritable() {
        this(new double[0], 0);
    }

    public ClusterSumWritable(double[] sumCoordinates, int count) {
        this.sumCoordinates = sumCoordinates;
        this.count = count;
    }

    public double[] getSumCoordinates() {
        return sumCoordinates;
    }

    public int getCount() {
        return count;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(sumCoordinates.length);
        for (double sumCoordinate : sumCoordinates) {
            out.writeDouble(sumCoordinate);
        }
        out.writeInt(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int length = in.readInt();
        sumCoordinates = new double[length];
        for (int i = 0; i < length; i++) {
            sumCoordinates[i] = in.readDouble();
        }
        count = in.readInt();
    }

    private ClusterSumWritable calculateClusterSum(Iterable<PointWritable> values) {
        int count = 0;
        double[] sum = null;

        for (PointWritable point : values) {
            if (sum == null) {
                sum = new double[point.getCoordinates().length];
            }
            for (int i = 0; i < sum.length; i++) {
                sum[i] += point.getCoordinates()[i];
            }
            count++;
        }

        return new ClusterSumWritable(sum, count);
    }
}
