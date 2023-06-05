import numpy as np
from sklearn.cluster import KMeans

# Read input from file
data = []
with open("./input_10000_2_100.txt", "r") as file:
    for line in file:
        line = line.strip().split(",")
        point = [float(x) for x in line[0:]]
        data.append(point)

# Convert data to numpy array
X = np.array(data)

# Extract number of points and features
n = X.shape[0]
d = X.shape[1]

# Prompt user to enter the number of clusters
k = int(input("Enter the number of clusters: "))

# Check if k is less than or equal to the number of data points
if k > n:
    print("Error: k cannot be greater than the number of data points.")
    exit(1)

# Create a KMeans instance with the first k data points as initial centroids
init_centroids = X[:k, :]
kmeans = KMeans(n_clusters=k, init="random", n_init='auto', random_state=1)

# Fit the data to the KMeans model
kmeans.fit(X)

# Get the cluster labels and centroids
labels = kmeans.labels_
centroids = kmeans.cluster_centers_
num_iter = kmeans.n_iter_

# Print the centroids
print("Centroids:")
for i, centroid in enumerate(centroids):
    print("Cluster {}: {}".format(i+1, centroid))

print('\n n° iter: ', num_iter)
