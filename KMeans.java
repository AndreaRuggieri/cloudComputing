import java.util.ArrayList;
import java.util.List;

public class KMeans {

    private List<Point> centroids;

    public static void main(String[] args) {
        // Generate random points
        List<Point> points = generateRandomPoints(100);

        // Set the number of clusters
        int k = 3;

        // Create an instance of KMeans
        KMeans kMeans = new KMeans();

        // Initialize centroids randomly
        kMeans.initializeCentroids(k);

        // Run K-means algorithm
        kMeans.runKMeans(points);

        // Print the final centroids and their respective clusters
        kMeans.printCentroids();
        kMeans.printClusters();
    }

    public void initializeCentroids(int k) {
        centroids = generateRandomPoints(k);
    }

    public void runKMeans(List<Point> points) {
        boolean centroidsChanged;

        do {
            centroidsChanged = false;

            // Clear clusters
            for (Point centroid : centroids) {
                centroid.getCluster().clear();
            }

            // Assign points to the nearest centroid
            for (Point point : points) {
                Point nearestCentroid = findNearestCentroid(point);
                nearestCentroid.getCluster().add(point);

                // Check if the point changed its cluster
                if (!nearestCentroid.equals(point.getCentroid())) {
                    point.setCentroid(nearestCentroid);
                    centroidsChanged = true;
                }
            }

            // Recalculate centroids
            for (Point centroid : centroids) {
                centroid.updateCentroid();
            }

        } while (centroidsChanged);
    }

    public Point findNearestCentroid(Point point) {
        Point nearestCentroid = null;
        double minDistance = Double.MAX_VALUE;

        for (Point centroid : centroids) {
            double distance = calculateDistance(point, centroid);
            if (distance < minDistance) {
                minDistance = distance;
                nearestCentroid = centroid;
            }
        }

        return nearestCentroid;
    }

    public double calculateDistance(Point point1, Point point2) {
        double xDiff = point1.getX() - point2.getX();
        double yDiff = point1.getY() - point2.getY();
        return Math.sqrt(xDiff * xDiff + yDiff * yDiff);
    }

    public void printCentroids() {
        System.out.println("Final Centroids:");
        for (Point centroid : centroids) {
            System.out.println(centroid);
        }
        System.out.println();
    }

    public void printClusters() {
        System.out.println("Final Clusters:");
        for (Point centroid : centroids) {
            System.out.println("Centroid: " + centroid);
            for (Point point : centroid.getCluster()) {
                System.out.println(point);
            }
            System.out.println();
        }
    }

    public static List<Point> generateRandomPoints(int numPoints) {
        List<Point> points = new ArrayList<>();

        for (int i = 1; i <= numPoints; i++) {
            double x = Math.random() * 100; // Adjust the range as needed
            double y = Math.random() * 100; // Adjust the range as needed
            points.add(new Point(i, x, y));
        }

        return points;
    }

    public static class Point {
        private int id;
        private double x;
        private double y;
        private Point centroid;
        private List<Point> cluster;

        public Point(int id, double x, double y) {
            this.id = id;
            this.x = x;
            this.y = y;
            this.cluster = new ArrayList<>();
        }

        public int getId() {
            return id;
        }

        public double getX() {
            return x;
        }

        public double getY() {
            return y;
        }

        public Point getCentroid() {
            return centroid;
        }

        public void setCentroid(Point centroid) {
            this.centroid = centroid;
        }

        public List<Point> getCluster() {
            return cluster;
        }

        public void updateCentroid() {
            double sumX = 0;
            double sumY = 0;

            for (Point point : cluster) {
                sumX += point.getX();
                sumY += point.getY();
            }

            int clusterSize = cluster.size();

            if (clusterSize > 0) {
                x = sumX / clusterSize;
                y = sumY / clusterSize;
            }
        }

        @Override
        public String toString() {
            return id + ", " + x + ", " + y;
        }
    }
}
