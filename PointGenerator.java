import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class PointGenerator {
    private int dimensions;
    private int numPoints;

    public PointGenerator(int dimensions, int numPoints) {
        this.dimensions = dimensions;
        this.numPoints = numPoints;
    }

    public void generatePointsToFile(String fileName) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
            for (int i = 1; i <= numPoints; i++) {
                StringBuilder rowBuilder = new StringBuilder();
                rowBuilder.append(i).append(", ");

                for (int j = 0; j < dimensions; j++) {
                    double coordinate = getRandomCoordinate();
                    rowBuilder.append(coordinate);

                    if (j < dimensions - 1) {
                        rowBuilder.append(", ");
                    }
                }

                writer.write(rowBuilder.toString());
                writer.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private double getRandomCoordinate() {
        return Math.random() * 100; 
    }

    public static void main(String[] args) {
        int dimensions = 7; // Number of dimensions for each object
        int numPoints = 1000; // Number of points to create
        String fileName = "Points.txt"; // Output file name

        PointGenerator pointGenerator = new PointGenerator(dimensions, numPoints);
        pointGenerator.generatePointsToFile(fileName);
    }
}

