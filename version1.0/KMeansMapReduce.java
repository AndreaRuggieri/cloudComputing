package tmp;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;

public class KMeansMapReduce {

	static int k;
	static int d;

	public class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, PointWritable> {
		private PointWritable[] centroids;
		private final Text reducerKey = new Text();
		private final PointWritable reducerValue = new PointWritable();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			// Genera centroidi naive method
			centroids = PointWritable.generateCentroids(k, d);
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// Convert the input text to a PointWritable
			PointWritable point = textToPoint(value);

			// Find the nearest centroid to the point
			int nearestCentroidId = getNearestCentroid(point);

			// Write the centroid id and the point to the context
			context.write(new IntWritable(nearestCentroidId), point);
		}

		private PointWritable textToPoint(Text text) {
			String line = text.toString();
			if (line == null || line.length() == 0)
				return null;

			String[] tokens = line.trim().split(",");

			if (tokens.length != d) {
				double[] val = new double[d];
				for (int i = 1; i <= d; i++) {
					double att = Double.parseDouble(tokens[i]);
					val[i] = att;
				}
				// reducerKey.set(Integer.parseInt(tokens[0]);
				reducerKey.set(tokens[0]);
				reducerValue.set(reducerValue, val);
				context.write(reducerKey, reducerValue);
			}

			return null;
		}

	}

	public class KMeansCombiner extends Reducer<IntWritable, PointWritable, IntWritable, ClusterSumWritable> {
		@Override
		protected void reduce(IntWritable key, Iterable<PointWritable> values, Context context)
				throws IOException, InterruptedException {
			// Calculate the sum of points and the count
			ClusterSumWritable clusterSum = calculateClusterSum(values);

			// Write the cluster id and the cluster sum to the context
			context.write(key, clusterSum);
		}

	}

	public class KMeansReducer extends Reducer<IntWritable, PointWritable, IntWritable, PointWritable> {
		@Override
		protected void reduce(IntWritable key, Iterable<PointWritable> values, Context context)
				throws IOException, InterruptedException {
			// Calculate the sum of points and the count
			ClusterSumWritable clusterSum = calculateClusterSum(values);

			// Calculate the new centroid
			PointWritable newCentroid = calculateNewCentroid(clusterSum);

			// Write the cluster id and the new centroid to the context
			context.write(key, newCentroid);
		}

		private PointWritable calculateNewCentroid(ClusterSumWritable clusterSum) {
			double[] sum = clusterSum.getSumCoordinates();
			int count = clusterSum.getCount();
			double[] centroid = new double[sum.length];

			for (int i = 0; i < sum.length; i++) {
				centroid[i] = sum[i] / count;
			}

			return new PointWritable(centroid);
		}

	}

	public static void main(final String[] args) throws Exception {
		final Configuration conf = new Configuration();
		final Job job = new Job(conf, "kmeans");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		job.setJarByClass(KMeansMapReduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(KMeansMapper.class);
		job.setReducerClass(KMeansReducer.class);

		k = Integer.parseInt(otherArgs[1]);
		d = Integer.parseInt(otherArgs[2]);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[3]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
