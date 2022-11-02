# slrum-map
Very simple parallel for-loops based on slurm!

# Usage
Usage is very simple, similar to the [multiprocessing](https://docs.python.org/3/library/multiprocessing.html) library and pythons built-in [map](https://docs.python.org/3/library/functions.html#map) function.

This package provides a `map` function that maps a function over an iterable. However, unlike the built-in map or multiprocessings map, every element of the iterable is handled by a separate slurm job. This allows for easy parallelization of a for-loop over an entire compute cluster!

Example:
```python

import slurm_map

def do_something(x):
    return x**2

if __name__ == '__main__':
    #results = [do_something(x) for x in data]
    #print(results)

    results = slurm_map.map(do_something, [1, 2, 3, 4, 5, 6, 7])
    print(results)

```

Slurm arguments and pre-run commands can be passed as a parameter:
```python
results = slurm_map.map(do_something, data, 
                        slurm_args="--mem=8G --time=3600", 
                        extra_commands=["hostname", "module load python3"])
    
```

Features:
- Forwarding of `stdout` and `stderr`, in other words `print` statements work
- Interruptable! Meaning after submission, the above python script can be interrupted and the jobs will continue running. Restarting the same python script will not submit new jobs but will continue to wait for the old ones to finish! Eg:
  ```
  > python submit.py
  Submitted batch job 67196
  Submitted batch job 67197
  Submitted batch job 67198
  Waiting for results from jobs [67097, 67098, 67198]
  ^C Keyboard interrupt
  ...
  
  > python submit.py
  Jobs were already started previously. Reusing those results.
  Waiting for results from jobs [67097, 67098, 67198]
  ```
  This module is therefore compatible with compute clusters that dont allow executing long tasks on the login nodes.

  # Installation

  Clone the repository, `cd` into it and run `pip install .`
