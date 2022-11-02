from pathlib import Path
import string, random, os, subprocess, time, threading
from typing import List, Any

import dill as pickle

def namedTemporaryFile(folder: str, name: str, mode="wb"):
    Path(folder).mkdir(parents=True, exist_ok=True) 
    characters = string.ascii_letters + string.digits
    random_chars = ''.join(random.choice(characters) for i in range(20))
    name = name.replace("#", random_chars)
    return open(os.path.join(folder, name), mode)

def watchFileAsync(filepath: str) -> None:
    """Prints new data in file 'filepath' to console."""

    def watcher():
        while not os.path.isfile(filepath):
            # Workaround: periodically call ls in order to force any underlying filesystem to update. 
            subprocess.Popen(['ls', os.path.dirname(filepath)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            time.sleep(1)

        f = subprocess.Popen(['tail', '-F', filepath], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        while os.path.isfile(filepath):
            line = f.stdout.readline().decode("utf-8").rstrip("\n")
            if "slurm_map saved data!" in line: break # Hack: stop when hearing a special string.
            print(line)
    threading.Thread(target=watcher, daemon=True).start()

def jobs_running(job_ids: List[str]) -> List[bool]:
    """Queries squeue to check whether a list of jobs is finished or not."""

    if len(job_ids) == 0: return False

    squeue = subprocess.Popen(["squeue -u $(whoami)"], shell=True, stderr=subprocess.STDOUT, stdout=subprocess.PIPE).stdout.readlines()
    squeue = "".join([s.decode("utf-8") for s in squeue])

    running = [str(job_id) in squeue for job_id in job_ids]
    return running

def unpickleWithTimeout(filename: str, num_tries=10) -> Any:
    """Tries multiple times to unpickle a particular file."""

    for _ in range(num_tries):
        try:
            with open(filename, "rb") as f:
                return pickle.load(f)
        except (FileNotFoundError, EOFError):
            print("Waiting for file", filename)
            time.sleep(1)
    print("ERROR: Stopping to wait for file", filename)
    return None #raise Exception(f"File {filename} failed to unpickle or does not exist...")