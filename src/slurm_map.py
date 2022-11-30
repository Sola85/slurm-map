import json
import os, sys
import subprocess
import time
import inspect
import hashlib
from typing import Callable, List, Any
from pathlib import Path

import dill as pickle
import rich

from utils import robust_rmtree, unpickleWithTimeout, jobs_running, watchFileAsync, namedTemporaryFile

SLURM_MAP_DIR = ".slurm_map"

config = {"print": "rich"}

def print(*args, **kwargs):
    if config["print"] == "rich":
        rich.print("[grey50](slurm-map):", *args, **kwargs)
    elif config["print"] == "plain":
        print(*args, **kwargs)
    else:
        pass

stopListeners = False

def startJobs(folder: str, function: Callable, data: List[Any], slurm_args: str, extra_commands: List[str], kwargs) -> None:
    with namedTemporaryFile(folder, "function.dill") as function_file:
        pickle.dump(function, function_file, recurse=True)

    with namedTemporaryFile(folder, "kwargs.json", "w") as global_args_file:
        json.dump(kwargs, global_args_file)

    job_ids = []


    Path(os.path.join(folder, "slurm")).mkdir(parents=True, exist_ok=True)
    Path(os.path.join(folder, "results")).mkdir(parents=True, exist_ok=True)

    # Workaround: periodically call ls in order to force any underlying filesystem to update. 
    subprocess.Popen(['ls', os.path.join(folder, "results")], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    subprocess.Popen(['ls', os.path.join(folder, "args")], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    len_new_data = 0
    for x in data:
        data_hash = hashlib.md5(pickle.dumps(x)).hexdigest()

        results_exist = os.path.isfile(os.path.join(folder, "results", f"res_{data_hash}.dill"))
        args_exist = os.path.isfile(os.path.join(folder, "args", f"arg_{data_hash}.dill"))
        if args_exist or results_exist: #Already have results for this data element
            continue 
    
        len_new_data += 1

        with namedTemporaryFile(os.path.join(folder, "args"), f"arg_{data_hash}.dill") as arg_file:
            pickle.dump(x, arg_file)

        with namedTemporaryFile(folder, f"run_{data_hash}.slurm", mode="w") as slurm_file:
            slurm_file_contents = [
                "#!/bin/sh",
                f"#SBATCH --output " + os.path.join(folder, "slurm", f"job_%j.log"),
                f"#SBATCH --error " + os.path.join(folder, "slurm", f"job_%j.log")
            ]
            lines = slurm_file_contents + extra_commands
            slurm_file.writelines([command + "\n" for command in lines])

            python_executable = sys.executable
            # Start *this* file as a slurm job, passing it the pickled function, args and results file
            python_command = f"{python_executable} -u {os.path.abspath(__file__)} execute {function_file.name} {arg_file.name} {global_args_file.name} {os.path.join(folder, 'results', f'res_{data_hash}.dill')}\n"
            slurm_file.writelines([python_command])

        
        p = subprocess.Popen(f"sbatch {slurm_args} " + slurm_file.name, bufsize=0, shell=True, stderr=subprocess.STDOUT, stdout=subprocess.PIPE)
        out = p.stdout.readlines()
        for line in out:
            line_str = line.decode("utf-8").rstrip("\n")
            if "Submitted batch job" in line_str:
                job_ids.append(int(line_str.split(" ")[-1])) #Extract job ids from output of sbatch command
            else:
                print(line_str)

    if len_new_data < len(data):
        print(f"Re-using {len(data) - len_new_data} results -> only need {len_new_data} new jobs")
    if len(job_ids) == len_new_data:
        print(f"Submitted {len(job_ids)} jobs.")
    else:
        print(f"Warning! There was a problem with job submission. Only submitted {len(job_ids)} out of {len_new_data} jobs!")    

    previous_job_ids = []
    try:
        with namedTemporaryFile(folder, "job_ids.json", mode="r") as f:
            previous_job_ids = json.load(f)
    except Exception:
        pass
    with namedTemporaryFile(folder, "job_ids.json", mode="w") as f:
        json.dump(previous_job_ids + job_ids, f)


def map(function: Callable, data: List[Any], slurm_args: str = None, extra_commands: List[str] = None, cleanup=True, kwargs=None) -> List[Any]:
    """ 
        Maps function over data, where every element of data is executed as a separate slurm batch job.

        The result is equivalent to 

        [function(x, **kwargs) for x in data]
        
        slurm_args: additional arguments to pass to slurm when submitting the job
        extra_commands: additional commands to run in the job before starting the evaluation of function. (Eg. 'module load python3')
        cleanup: If true, delete temporary files created after this function finishes. (Temporary files are pickled versions of the function, data and results, as well as the slurm bash scripts and log files.) 
        args: Additional keyword arguments for 'function'

    """

    if slurm_args is None:
        slurm_args = ""
    if extra_commands is None:
        extra_commands = []

    callee_filename = os.path.basename(inspect.stack()[1].filename).rstrip(".py")

    args_str = json.dumps(kwargs)
    args_hash = hashlib.md5(args_str.encode('utf-8')).hexdigest()

    folder = os.path.join(".", SLURM_MAP_DIR, callee_filename, function.__name__, args_hash)

    startJobs(folder, function, data, slurm_args, extra_commands, kwargs)

    with open(os.path.join(folder, "job_ids.json")) as f:
        job_ids = json.load(f)
    
    running = jobs_running(job_ids)

    print(f"Waiting for {sum(running)} jobs to finish...")

    global stopListeners
    stopListeners = False

    def stopListenerCallback():
        return stopListeners

    # Start file watchers for unfinished jobs in order to get stdout.
    log_watchers = [watchFileAsync(os.path.join(folder, "slurm", f"job_{id}.log"), stopListenerCallback) if r else None for id, r in zip(job_ids, running)]

    # Wait until all jobs are finished
    try: 
        while any(running):
            time.sleep(1)
            running = jobs_running(job_ids)
    except KeyboardInterrupt:
        stopListeners = True
        print(f"\nStopping to wait for results. Jobs will continue to run. Use 'python -m slurm_map cancel {folder}' to cancel the jobs.")
        exit()
    
    print("Jobs done, waiting for results...")

    # collect results
    results = [None]*len(data)
    succeeded = [False]*len(data)
    iteration = 0
    max_iterations = 20

    while (not all(succeeded)) and (iteration < max_iterations):
        for i in [j for j in range(len(data)) if not succeeded[j]]:
            try:
                data_hash = hashlib.md5(pickle.dumps(data[i])).hexdigest()
                with open(os.path.join(folder, "results", f"res_{data_hash}.dill"), "rb") as f:
                    results[i] = pickle.load(f)
                succeeded[i] = True
            except Exception as e:
                pass
        # Workaround: periodically call ls in order to force any underlying filesystem to update. 
        subprocess.Popen(['ls', os.path.join(folder, "results")], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        time.sleep(0.5)
        iteration += 1

    all_succeeded = all(succeeded)
    if all_succeeded:
        print("Done.")
    else:
        print(f"Warning: not all jobs returned a result. Not cleaning up. Clean up manually using 'python -m slurm_map cleanup {folder}'")

    if cleanup and all_succeeded:
        robust_rmtree(folder)
    
    return results


def compute(function_file: str, arg_file: str, kwarg_file:str, res_file: str) -> None:
    function = unpickleWithTimeout(function_file)
    args = unpickleWithTimeout(arg_file)

    with open(kwarg_file) as f:
        kwargs = json.load(f)

    if kwargs is None:
        kwargs = {}

    try:
        res = function(args, **kwargs)

        with open(res_file, "wb") as f:
            pickle.dump(res, f)
    finally:
        try: # Remove arg_file even when an error occurs. That way, a second slum_map call will retry this call.
            os.remove(arg_file)
        except FileNotFoundError:
            pass

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
        'kwarg_file', help='Path of the .dill file encoding the keyword arguments of the function to be executed.')
    parser_execute.add_argument(
        'res_file', help='Path of the .dill file where the results of the function shall be stored.')

    args = parser.parse_args()
    if args.action == 'execute':
        compute(args.function_file, args.arg_file, args.kwarg_file, args.res_file)
    elif args.action == 'cleanup':
        path = os.path.join(args.function_name)
        if os.path.isdir(path):
            robust_rmtree(path)
            print("Cleaned up", path)
        else:
            print(f"Nothing to do. {path} does not exist.")
    elif args.action == 'cancel':
        path = os.path.join(args.function_name, "job_ids.json")
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