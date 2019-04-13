import inspect
import json
import logging
import os
import sys
import traceback
from copy import deepcopy
from multiprocessing import Pool, cpu_count
from multiprocessing.pool import ThreadPool

from enum import Enum

logger = logging.getLogger()

class SuccessPolicy(Enum):
    expect_all = 0  # most restrictive, success if all runs are successful
    expect_any = 1  # success if one or more runs are successful
    super_lax = 2  # most permissive, success even if everything failed


class MappedException(Exception):
    def __init__(self, orig_ex_descriptors):
        """
        :param orig_ex_descriptors: list of error descriptors coming from all the
         processes that threw an exception
        """
        super(MappedException, self).__init__(
            json.dumps(orig_ex_descriptors, indent=4) +
            '\n%d processes threw exception(s)' % len(orig_ex_descriptors))

def _per_process_args(func, wrapped_args, wrapped_kwargs,
                      num_processes, mapped_arg, arg_value_divider):
    func_arg_spec = inspect.getargspec(func)
    def build_function_args(divided_arg_subset, orig_args, orig_kwargs):
        """
        build replacement *args, **kwargs -
            use a subset in place of original divisible arg
        """
        new_args = [orig_args[i] if func_arg_spec.args[i] !=
                                    mapped_arg else divided_arg_subset
                    for i in xrange(len(func_arg_spec.args))]
        new_args.extend(orig_args[len(func_arg_spec.args):])
        new_kwargs = deepcopy(orig_kwargs)
        if mapped_arg in new_kwargs:
            new_kwargs[mapped_arg] = divided_arg_subset
        return new_args, new_kwargs

    max_num_processes = num_processes if num_processes is not None else cpu_count()
    premapped_arg_value = None
    if mapped_arg in func_arg_spec.args:
        premapped_arg_value = wrapped_args[func_arg_spec.args.index(mapped_arg)]
    elif mapped_arg in wrapped_kwargs:
        premapped_arg_value = wrapped_kwargs[mapped_arg]
    arg_value_parts = premapped_arg_value
    if arg_value_divider:
        arg_value_parts = arg_value_divider(premapped_arg_value, max_num_processes)
    if not arg_value_parts:
        mapped_arg_values = [[wrapped_args, wrapped_kwargs]]
    else:
        mapped_arg_values = [
            build_function_args(arg_value_part, wrapped_args, wrapped_kwargs)
            for arg_value_part in arg_value_parts]

    num_processes = max(min(max_num_processes, len(mapped_arg_values)), 1)
    return num_processes, mapped_arg_values

def run_distributively(mapped_arg=None, arg_value_divider=None,
                       result_reducer=None, num_processes=None,
                       success_policy=SuccessPolicy.expect_all,
                       in_process=False):
    """
    Decorator to run function in multiple threads or processes in parallel
    If in_process is False (i.e. run in multiple processes) then
    function arguments and the return value must be *picklable*.
    Signature should contain mapped_arg either explicitely or in kwargs

    :mapped_arg: argument which can be divided on which, once divided, mapping
        occurs (list, tokenizable string, etc)
    :arg_value_divider: function which takes value at mapped_arg and returns a
        list of values of the same type as mapped_arg. Elements of this list are
        then used to call the function in a process, one per element, in parallel.
        Expected signature: (value, num_parts) value - value to divide, max_parts -
        how many parts, max. May be less if value doesn't warrant division up to max.
        Note: arg_value_divider may be None, if so - the value at mapped_arg must
        be an iterable. workload_distributor then just picks an element one by one
        from the iterable to map them to workers.
    :result_reducer: build end result from  results of processified functions.
        In: iterable Out: object of a type consistent with elements of iterable
    :num_processes: number of processes in the Pool. Uses cpu_count if None
    :success_policy: what to do - return or raise exception -
                    if any runs raise exception?
    :in_process: [True | False]

    This decorator:
    - Builds a complete and non-overlapping list of subsets of the value at
        mapped_arg. Uses mapped_arg to do that.
    - Maps the subsets to the decorated function running in a process. For each
        mapping, modifies original function args to use the subset instead of
        original value at mapped_arg.
    - Reads the results.
    - Joins the processes.
    - Builds the end result. Uses result_reducer to do that.
    - Supports semantics for "what to do" if one or more of the results fails:
        Raise MappedException if one or more of the results raises an exception
        and "success_policy" instructs to fail.


    Note: if mapped_arg is None (or any value that's not found),
        then this just runs decorated func in a standalone process.
        This is a valid use case for this util.
    """
    def wrapper(func):
        def process_at_worker(arg):
            try:
                result = func(*arg[0], **arg[1])
            except Exception:
                ex_type, ex_value, tb = sys.exc_info()
                ex = dict(type=str(ex_type),
                          value=str(ex_value),
                          tb=traceback.format_tb(tb),
                          function=func.__name__,
                          function_inputs=dict(args=arg[0], kwargs=arg[1]),
                          pid=os.getpid())
                result = None
            else:
                ex = None
            return dict(result=result, ex=ex)

        # make process_at_worker picklable
        process_at_worker.__name__ = func.__name__ + "_at_worker"
        setattr(
            sys.modules[__name__], process_at_worker.__name__, process_at_worker)

        def wrapped(*args, **kwargs):
            actual_num_processes, per_process_args = _per_process_args(
                func, args, kwargs, num_processes, mapped_arg, arg_value_divider)
            logger.info("Actual number of processes in the pool: %d" %
                        actual_num_processes)
            if in_process:
                pool = ThreadPool(actual_num_processes)
            else:
                pool = Pool(actual_num_processes)

            results = pool.map(
                process_at_worker,
                per_process_args)
            pool.close()
            pool.join()
            if results is None:  # legitimate scenario
                return
            result_exeptions = [
                result['ex'] for result in results if result['ex']]
            result_data = None if result_reducer is None else result_reducer(
                [result['result'] for result in results if result['result']])

            if success_policy == SuccessPolicy.super_lax:
                return result_data
            if success_policy == SuccessPolicy.expect_any and (result_data or
                                                        not result_exeptions):
                return result_data
            if success_policy == SuccessPolicy.expect_all and not result_exeptions:
                return result_data
            raise MappedException(result_exeptions)

        return wrapped

    return wrapper

