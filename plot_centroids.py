import os
import glob
import matplotlib.pyplot as plt
import re
import json
import numpy as np

# Root directory
root_dir = './'

# Regex to extract the centroid id and its coordinates
pattern = r'(\d+)\s+\[([\d.,\s]+)\]'

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

# Plot the centroid positions over iterations
for dir, dir_data in data.items():
    num_clusters = dir_data['num_clusters']
    num_iterations = dir_data['num_iterations']
    plt.figure(figsize=(10, 6))
    plt.title(f'Centroid Positions Over Iterations for {dir}')
    for id in range(num_clusters):
        x = np.arange(num_iterations)
        y = [dir_data['centroids'][iteration][id][0] for iteration in range(num_iterations)]  # Assumes 2D data, change this for higher dimensions
        plt.plot(x, y, label=f'Centroid {id}')
    plt.xlabel('Iteration')
    plt.ylabel('Centroid Position')
    plt.legend()
    plt.show()
