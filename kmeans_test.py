from discord_webhook import DiscordWebhook, DiscordEmbed
import subprocess
import os

def notify_discord(file_path, webhook_url):
    with open(file_path, 'r') as file:
        content = file.read()

    webhook = DiscordWebhook(url=webhook_url, content=content)
    webhook.execute()


def run_command(command):
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, error = process.communicate()

    if error:
        print(f'Error: {error}')
        return output.decode()
    else:
        print(f'Output: {output.decode()}')
        return output.decode()

# Parametri per la generazione dei file di input
n_values = [5000, 25000, 100000]
d_values = [2, 4, 7]
c_values = [5, 10, 25]

# Discord webhook URL
webhook_url = 'https://discord.com/api/webhooks/1115003067188908093/-Eyki_ZFM4Y6mL1Oaqqcc4G3xbBe5lMvXKNZHCkqDkbwmusL-eqYX-XYTx_knOVj3yoj'

# Itera su tutte le combinazioni dei parametri
for n in n_values:
    for d in d_values:
        for c in c_values:
            # Controlla che c non sia troppo grande rispetto a n
            if c <= n / 100:
                # Genera il file di input
                input_file_name = f'input_{n}_{d}_{c}.txt'
                output_file_path = f'test_{n}_{d}_{c}'

                # Check if output directory already exists
                ls_command = f"hadoop fs -ls"
                ls_output = run_command(ls_command)
                if output_file_path in ls_output:
                    print(f"Skipping as output directory {output_file_path} already exists")
                    continue
                
                gen_command = f'python3 generatoreRecord.py -n {n} -d {d} -c {c}'
                print(f"Generating input file: {input_file_name}")
                run_command(gen_command)

                # Sposta il file su HDFS
                hdfs_command = f'hadoop fs -put {input_file_name} {input_file_name}'
                print(f"Moving file to HDFS: {input_file_name}")
                run_command(hdfs_command)

                # Esegui il job MapReduce
                mapreduce_command = f'hadoop --loglevel FATAL jar kmeans-1.0-SNAPSHOT.jar it.unipi.hadoop.KMeansMapReduce {input_file_name} {c} {d} 200 0.001 {output_file_path}'
                print(f"Running MapReduce job on: {input_file_name}")
                run_command(mapreduce_command)

                # Read stat.txt content
                stat_command = f"hadoop fs -cat {output_file_path}/stat.txt"
                stat_output = run_command(stat_command)

                # Send notification to Discord
                webhook = DiscordWebhook(url=webhook_url)
                # Send notification to Discord
                webhook = DiscordWebhook(url=webhook_url)
                embed = DiscordEmbed(title='JOB ESEGUITO', description=stat_output, color='03b2f8')
                webhook.add_embed(embed)
                webhook.execute()

# Send notification to Discord
webhook = DiscordWebhook(url=webhook_url)

# Send notification to Discord
webhook = DiscordWebhook(url=webhook_url)
embed = DiscordEmbed(title='CICLO COMPLETATO', description="SIUUUUUUUM", color='ff0000')
webhook.add_embed(embed)
webhook.execute()
