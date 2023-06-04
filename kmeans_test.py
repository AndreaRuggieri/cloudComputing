import subprocess

def run_command(command):
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
    output, error = process.communicate()

    if error:
        print(f'Error: {error}')
    else:
        print(f'Output: {output}')

# Parametri per la generazione dei file di input
n_values = [10000, 50000, 100000]
d_values = [2, 4, 7]
c_values = [5, 10, 20, 50, 100]

# Itera su tutte le combinazioni dei parametri
for n in n_values:
    for d in d_values:
        for c in c_values:
            # Controlla che c non sia troppo grande rispetto a n
            if c <= n / 100:
                # Genera il file di input
                input_file_name = f'input_{n}_{d}_{c}.txt'
                gen_command = f'python3 generatoreRecord.py -n {n} -d {d} -c {c}'
                print(f"Generating input file: {input_file_name}")
                run_command(gen_command)

                # Sposta il file su HDFS
                hdfs_command = f'hadoop fs -put {input_file_name} {input_file_name}'
                print(f"Moving file to HDFS: {input_file_name}")
                run_command(hdfs_command)

                # Esegui il job MapReduce
                mapreduce_command = f'hadoop --loglevel FATAL jar kmeans-1.0-SNAPSHOT.jar it.unipi.hadoop.KMeansMapReduce {input_file_name} {c} {d} 50 0.001 test_{n}_{d}_{c}'
                print(f"Running MapReduce job on: {input_file_name}")
                run_command(mapreduce_command)
