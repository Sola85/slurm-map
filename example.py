import slurm_map
import time
import subprocess

data = [1, 2, 3, 4, 5, 6, 7]

def do_something(x):
    print("Working on", x)
    time.sleep(10)
    hostname = subprocess.run(['hostname'], stdout=subprocess.PIPE).stdout
    print("Done with", x)

    return x**2, hostname

if __name__ == '__main__':
    #results = [do_something(x) for x in data]
    #print(results)

    results = slurm_map.map(do_something, data, slurm_args="--mem=8G --partition=itp --time=3600", extra_commands=["hostname"])
    print(results)
