package tmp;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import java.io.DataOutput;
import java.io.IOException;
import java.io.DataInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KMeansUtils {
    public static ClusterSumWritable calculateClusterSum(Iterable<PointWritable> values) {
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
