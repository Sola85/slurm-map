import json
import os
import subprocess
import time
from typing import Callable, List

import dill as pickle

from utils import *

def startJobs(folder: str, function: Callable, data: List[Any], slurm_args: str, extra_commands: List[str]) -> None:
    with namedTemporaryFile(folder, "function.dill") as function_file:
        pickle.dump(function, function_file, recurse=True)

    job_ids = []

    for i, x in enumerate(data):
        with namedTemporaryFile(folder, f"arg_{i}.dill") as arg_file:
            pickle.dump(x, arg_file)

        with namedTemporaryFile(folder, f"run_{i}.slurm", mode="w") as slurm_file:
            slurm_file_contents = [
                "#!/bin/sh",
                f"#SBATCH --output {folder}job_{i}.log",
                f"#SBATCH --error {folder}job_{i}.log"
            ]
            lines = slurm_file_contents + extra_commands
            slurm_file.writelines([command + "\n" for command in lines])

            # Start *this* file as a slurm job, passing it the pickled function, args and results file
            python_command = f"python -u {os.path.abspath(__file__)} {function_file.name} {arg_file.name} {os.path.join(folder, f'res_{i}.dill')}\n"
            slurm_file.writelines([python_command])

        
        p = subprocess.Popen(f"sbatch {slurm_args} " + slurm_file.name, bufsize=0, shell=True, stderr=subprocess.STDOUT, stdout=subprocess.PIPE)
        out = p.stdout.readlines()
        for line in out:
            line_str = line.decode("utf-8").rstrip("\n")
            print(line_str)
            if "Submitted batch job" in line_str:
                job_ids.append(int(line_str.split(" ")[-1])) #Extract job ids from output of sbatch command

    with namedTemporaryFile(folder, "job_ids.json", mode="w") as f:
        json.dump(job_ids, f)


def map(function: Callable, data: List[Any], slurm_args: str = None, extra_commands: List[str] = None, cleanup=True) -> List[Any]:
    """ 
        Maps function over data, where every element of data is executed as a separate slurm batch job.
        
        slurm_args: additional arguments to pass to slurm when submitting the job
        extra_commands: additional commands to run in the job before starting the evaluation of function. (Eg. 'module load python3')
        cleanup: If true, delete temporary files created after this function finishes. (Temporary files are pickled versions of the function, data and results, as well as the slurm bash scripts and log files.) 

    """

    if slurm_args is None:
        slurm_args = ""
    if extra_commands is None:
        extra_commands = []

    folder = f"./.slurmpy/{function.__name__}/"

    if os.path.isfile(os.path.join(folder, "job_ids.json")):
        print("Jobs were already started previously. Reusing those results.")
    else: startJobs(folder, function, data, slurm_args, extra_commands)

    with open(os.path.join(folder, "job_ids.json")) as f:
        job_ids = json.load(f)
    
    print("Waiting for results from jobs", job_ids)
    
    running = jobs_running(job_ids)

    # Start file watchers for unfinished jobs in order to get stdout.
    log_watchers = [watchFileAsync(os.path.join(folder, f"job_{i}.log")) if r else None for i, r in enumerate(running)]

    # Wait until all jobs are finished
    while any(running):
        time.sleep(1)
        running = jobs_running(job_ids)
    
    results = [None]*len(data)
      
    # collect results
    for i in range(len(data)):
        results[i] = unpickleWithTimeout(os.path.join(folder, f"res_{i}.dill"))

    if cleanup:
        for f in os.listdir(folder):
            os.remove(os.path.join(folder, f))
    
    return results


def compute(function_file: str, arg_file: str, res_file: str) -> None:
    function = unpickleWithTimeout(function_file)
    args = unpickleWithTimeout(arg_file)

    try:
        os.remove(arg_file)
    except FileNotFoundError:
        pass

    res = function(args)

    with open(res_file, "wb") as f:
        pickle.dump(res, f)

    print("slurm_map saved data!")

if __name__ == "__main__":
    # Entrypoint when this file is started as a slurm job

    import sys
    sys.path.insert(0,'.')  # "." is the cwd where slurm_map was originally called from. If we dont add it, 
                            # unpickling of the mapped 'function' will fail if it uses code from modules in cwd.
    compute(sys.argv[1], sys.argv[2], sys.argv[3])