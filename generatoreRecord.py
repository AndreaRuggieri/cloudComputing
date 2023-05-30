import random

def generate_file(n, d, filename="input.txt"):
    with open(filename, "w") as file:
        for i in range(1, n + 1):
            file.write(str(0))
            for _ in range(d):
                file.write("," + str(random.random()*100))
            file.write("\n")

generate_file(300, 3)