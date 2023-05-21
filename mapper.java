import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, PointWritable> {

    private List<Point> means;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Initialize the means from the distributed cache or any other source
        // Here, we assume the means are already set and available in the `means` list
        means = new ArrayList<>(); // Initialize and populate the means list
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Convert the input value to a Point object
        Point point = Point.fromText(value.toString());

        // Find the closest mean for the point
        int closestMeanIndex = 0;
        double closestMeanDistance = Double.MAX_VALUE;

        for (int i = 0; i < means.size(); i++) {
            double distance = point.calculateDistance(means.get(i));
            if (distance < closestMeanDistance) {
                closestMeanDistance = distance;
                closestMeanIndex = i;
            }
        }

        // Emit the closest mean index as the key and the point as the value
        context.write(new IntWritable(closestMeanIndex), new PointWritable(point));
    }
}
