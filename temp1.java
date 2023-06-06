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
            for (int j = 1; j < parts.length; j++) {
                coordinates[j - 1] = Double.parseDouble(parts[j]);
            }
            centroids.add(new PointWritable(coordinates, id));
        }
    }

    return centroids.toArray(new PointWritable[0]);
}