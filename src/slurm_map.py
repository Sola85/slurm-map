import json
import os
import subprocess
import time
from typing import Callable, List, Any

import dill as pickle

from utils import robust_rmtree, unpickleWithTimeout, jobs_running, watchFileAsync, namedTemporaryFile

SLURM_MAP_DIR = ".slurm_map"

stopListeners = False

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
            python_command = f"python -u {os.path.abspath(__file__)} execute {function_file.name} {arg_file.name} {os.path.join(folder, f'res_{i}.dill')}\n"
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

    folder = f"./{SLURM_MAP_DIR}/{function.__name__}/"

    if os.path.isfile(os.path.join(folder, "job_ids.json")):
        print("Jobs were already started previously. Reusing those results.")
    else: startJobs(folder, function, data, slurm_args, extra_commands)

    with open(os.path.join(folder, "job_ids.json")) as f:
        job_ids = json.load(f)
    
    print("Waiting for results from jobs", job_ids)
    
    running = jobs_running(job_ids)

    global stopListeners
    stopListeners = False

    def stopListenerCallback():
        return stopListeners

    # Start file watchers for unfinished jobs in order to get stdout.
    log_watchers = [watchFileAsync(os.path.join(folder, f"job_{i}.log"), stopListenerCallback) if r else None for i, r in enumerate(running)]

    # Wait until all jobs are finished
    try: 
        while any(running):
            time.sleep(1)
            running = jobs_running(job_ids)
    except KeyboardInterrupt:
        stopListeners = True
        print(f"Stopping to wait for results. Jobs will continue to run. Use 'python -m slurm_map cancel {function.__name__}' to cancel the jobs.")
        exit()
    
    print("Jobs done, waiting for results...")

    # collect results
    results = [None]*len(data)
    succeeded = [False]*len(data)
    iteration = 0
    max_iterations = 10

    while (not all(succeeded)) and (iteration < max_iterations):
        for i in [j for j in range(len(data)) if not succeeded[j]]:
            try:
                with open(os.path.join(folder, f"res_{i}.dill"), "rb") as f:
                    results[i] = pickle.load(f)
                succeeded[i] = True
            except Exception:
                pass
        time.sleep(0.5)
        iteration += 1

    all_succeeded = all(succeeded)
    if not all_succeeded:
        print(f"Warning: not all jobs returned a result. Not cleaning up. Clean up manually using 'python -m slurm_map cleanup {function.__name__}'")

    if cleanup and all_succeeded:
        robust_rmtree(folder)
    
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

    import argparse

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='action', required=True)

    parser_cleanup = subparsers.add_parser('cleanup')
    parser_cleanup.add_argument(
        'function_name', help='The name of the function whose data slurm-map data should be cleaned up.')

    parser_cancel = subparsers.add_parser('cancel')
    parser_cancel.add_argument(
        'function_name', help='The name of the function whose slurm-jobs should be cancelled.')

    parser_execute = subparsers.add_parser('execute')
    #function_file: str, arg_file: str, res_file: str
    parser_execute.add_argument(
        'function_file', help='Path of the .dill file encoding the function to be executed.')
    parser_execute.add_argument(
        'arg_file', help='Path of the .dill file encoding the arguments of the function to be executed.')
    parser_execute.add_argument(
        'res_file', help='Path of the .dill file where the results of the function shall be stored.')

    args = parser.parse_args()
    if args.action == 'execute':
        compute(args.function_file, args.arg_file, args.res_file)
    elif args.action == 'cleanup':
        path = os.path.join(".", SLURM_MAP_DIR, args.function_name)
        if os.path.isdir(path):
            robust_rmtree(path)
            print("Cleaned up", path)
        else:
            print(f"Nothing to do. {path} does not exist.")
    elif args.action == 'cancel':
        path = os.path.join(".", SLURM_MAP_DIR, args.function_name, "job_ids.json")
        if os.path.isfile(path):
            with open(path) as f:
                job_ids = json.load(f)
            print("Cancelling jobs", job_ids)
            for job_id in job_ids:
                out = subprocess.Popen(["scancel", str(job_id)], bufsize=0, stderr=subprocess.STDOUT, stdout=subprocess.PIPE).stdout.readlines()
                for l in out:
                    print(l.decode('utf-8').rstrip("\n"))
        else:
            print(f"Nothing to do. {path} does not exist.")