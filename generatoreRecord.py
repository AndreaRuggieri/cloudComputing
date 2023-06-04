import random
import argparse
from sklearn.datasets import make_blobs

def generate_file(n, d):
    filename = f"input_{n}_{d}.txt"

    with open(filename, "w") as file:
        for i in range(1, n + 1):
            file.write(str(random.random() * 1000))
            for _ in range(d - 1):
                file.write("," + str(random.random() * 1000))
            file.write("\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-n", "--num_rows", type=int,
                        default=1000, help="Number of rows")
    parser.add_argument("-d", "--num_columns", type=int,
                        default=2, help="Number of columns")
    args = parser.parse_args()

    generate_file(args.num_rows, args.num_columns)
