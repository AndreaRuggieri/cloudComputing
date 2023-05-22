import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PointGenerator {
public static void main(String[] args) {
    List<Point> points = generatePoints();

    // Print points to a file
    printToFile(points, "points.txt");
}

public static List<Point> generatePoints() {
    List<Point> points = new ArrayList<>();

    for (int i = 1; i <= 100; i++) {
        double x = getRandomCoordinate();
        double y = getRandomCoordinate();
        points.add(new Point(i, x, y));
    }

    return points;
}

public static double getRandomCoordinate() {
    return Math.random() * 100; // Adjust the range as needed
}

/**
 * @param points
 * @param fileName
 */
public static void printToFile(List<Point> points, String fileName) {
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
        for (Point point : points) {
            writer.write(point.toString());
            writer.newLine();
        }
    } catch (IOException e) {
        e.printStackTrace();
    }
}

public static class Point {
    private int id;
    private double x;
    private double y;

    public Point(int id, double x, double y) {
        this.id = id;
        this.x = x;
        this.y = y;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public double getX() {
        return x;
    }

    public void setX(double x) {
        this.x = x;
    }

    public double getY() {
        return y;
    }

    public void setY(double y) {
        this.y = y;
    }

    @Override
    public String toString() {
        return id + ", " + x + ", " + y;
    }
}
}
