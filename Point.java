package tmp;

public class Point {
    private int id;
    private double[] coordinates;

    public Point(int id, double[] coordinates) {
        this.id = id;
        this.coordinates = coordinates;
    }

    public int getId() {
        return id;
    }

    public double[] getCoordinates() {
        return coordinates;
    }

    public void setCoordinates(double[] coordinates) {
        this.coordinates = coordinates;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(id).append(", ");

        for (int i = 0; i < coordinates.length; i++) {
            sb.append(coordinates[i]);

            if (i < coordinates.length - 1) {
                sb.append(", ");
            }
        }

        return sb.toString();
    }
}
