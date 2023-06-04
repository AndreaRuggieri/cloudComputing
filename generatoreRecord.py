import random
import argparse
from sklearn.datasets import make_blobs

def generate_file(n, d):
    centers = 5

    points, y = make_blobs(n_samples=n, centers=centers, n_features=d, shuffle=True, center_box=(0, 1000), cluster_std=30)

    filename = f"input_{n}_{d}.txt"

    with open(filename, "w") as file:
        i = 0
        for point in points:
            for value in range(d):
                if value == (d - 1):
                    file.write(str(point[value]))
                else:
                    file.write(str(point[value]) + ",")
            #file.write(' - ' + str(y[i]))
            i+=1
            file.write("\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-n", "--num_rows", type=int,
                        default=1000, help="Number of rows")
    parser.add_argument("-d", "--num_columns", type=int,
                        default=2, help="Number of columns")
    parser.add_argument("-c", "--num_centers", type=int,
                        default=3, help="Number of centers")
    args = parser.parse_args()

    generate_file(args.num_rows, args.num_columns)
