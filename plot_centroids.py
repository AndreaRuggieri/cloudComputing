import os
import glob
import matplotlib.pyplot as plt
import re
import json
import numpy as np
import matplotlib.cm as cm

# Root directory
root_dir = './'
# Input directory
input_dir = './../oldInputs'

# Regex to extract the centroid id and its coordinates
pattern = r'(\d+)\s+\[([\d.,\s]+)\]'

# Function to read points from file
def read_points_from_file(file_name, num_points=None):
    points = []
    with open(file_name, 'r') as file:
        for i, line in enumerate(file):
            if num_points is not None and i >= num_points:
                break
            values = line.strip().split(',')
            x = float(values[0])
            y = float(values[1])
            points.append((x, y))
    return points

# Get the directories
test_dirs = [d for d in os.listdir(root_dir) if os.path.isdir(os.path.join(root_dir, d)) and '_2_' in d]

# Prepare a dictionary to store the data
data = {}

for dir in test_dirs:
    dir_path = os.path.join(root_dir, dir)
    stat_file_path = os.path.join(dir_path, 'stat.txt')
    
    # Parse stat.txt file
    with open(stat_file_path, 'r') as f:
        content = f.read()
        num_clusters = int(re.search(r'Num Clusters:\s*:\s*(\d+)', content).group(1))
        data_dim = int(re.search(r'Data Dimension:\s*(\d+)', content).group(1))

    # Detect the number of iterations and sort them correctly
    iteration_dirs = sorted(glob.glob(os.path.join(dir_path, 'iteration*')), key=lambda x: int(re.search(r'iteration(\d+)', x).group(1)))
    num_iterations = len(iteration_dirs)
    
    # Prepare a dictionary to store the centroid data
    data[dir] = {'num_clusters': num_clusters, 'num_iterations': num_iterations, 'centroids': {i: {j: [0]*data_dim for j in range(num_clusters)} for i in range(num_iterations)}}

    # Get initial centroid positions from the first k records of the input file
    input_file_path = os.path.join(input_dir, f'input_{dir[5:]}.txt')
    initial_centroids = read_points_from_file(input_file_path, num_clusters)
    for id, centroid in enumerate(initial_centroids):
        data[dir]['centroids'][0][id] = list(centroid)

    # Parse iteration directories
    for iteration_dir in iteration_dirs:
        iteration = int(re.search(r'iteration(\d+)', iteration_dir).group(1))
        
        # Parse part files
        part_files = glob.glob(os.path.join(iteration_dir, 'part-r-*'))
        for part_file in part_files:
            with open(part_file, 'r') as f:
                for line in f:
                    match = re.match(pattern, line)
                    if match:
                        id = int(match.group(1))
                        coords = json.loads('[' + match.group(2) + ']')
                        data[dir]['centroids'][iteration][id] = coords

    # If there's no id from an iteration and the previous, assume that the new position at i iterations for this centroid is not changed and is equal to the previous.
    for i in range(1, num_iterations):
        for id in range(num_clusters):
            if data[dir]['centroids'][i][id] == [0]*data_dim:
                data[dir]['centroids'][i][id] = data[dir]['centroids'][i-1][id]

# Plotting
for dir, dir_data in data.items():
    num_clusters = dir_data['num_clusters']
    num_iterations = dir_data['num_iterations']
    plt.figure(figsize=(10, 6))
    plt.title(f'Centroid Positions Over Iterations for {dir}')
    # Plot dataset points
    input_file_path = os.path.join(input_dir, f'input_{dir[5:]}.txt')
    points = read_points_from_file(input_file_path)
    x_data = [point[0] for point in points]
    y_data = [point[1] for point in points]
    plt.scatter(x_data, y_data, s=5, alpha=0.1)  # Adjust the 's' parameter to change the size of the points
    # Plot centroid positions
    colors = cm.rainbow(np.linspace(0, 1, num_clusters))  # Color map
    for id, color in zip(range(num_clusters), colors):
        x = [dir_data['centroids'][iteration][id][0] for iteration in range(num_iterations)]
        y = [dir_data['centroids'][iteration][id][1] for iteration in range(num_iterations)]
        plt.plot(x, y, '-o', label=f'Centroid {id}') #, color=color)
    plt.xlabel('X Position')
    plt.ylabel('Y Position')
    plt.legend()
    plt.show()
