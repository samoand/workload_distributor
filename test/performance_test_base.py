import logging
import time
from collections import defaultdict, namedtuple
from optparse import OptionParser

import unittest2 as unittest

from workload_distributor import run_distributively


Data = namedtuple('Data', ['valid_keys', 'words'])
def prepare_data():
    """
    prepare test data: a list of strings
    'aaaa', 'aaab', ..., 'aaaz', ...., 'zzzz'
    """
    chars = [chr(c) for c in range(ord('a'), ord('z')+1)]
    # return reduce(lambda acc, el: acc+chars, chars, [])
    result = reduce(lambda acc, el: acc+[el+c for el in el for c in reduce(
        lambda acc, el: acc+[el+c for el in el for c in reduce(
            lambda acc, el: acc+[el+c for el in el for c in chars], chars, [])],
        chars, [])], chars, [])
    return Data(valid_keys=set(result[:len(result)/2]), words=result)


def list_arg_divider(words, suggested_num_chunks):
    return [words[i:i+suggested_num_chunks] for i
            in range(0, len(words), suggested_num_chunks)]

def occur_reducer(partials):
    result = {}
    for partial in partials:
        result.update(partial)
    return result

def calculate(valid_keys, words, simulate_activity_coef):
    def simulate_activity_on_word(word):
        for i in range(simulate_activity_coef):
            word = word[::-1]
        return word
    
    result = defaultdict(lambda: 0)
    for word in words:
        if simulate_activity_on_word(word) in valid_keys:
            result[word] += 1
    return result[1] # extract regular dict from defaultdict

@run_distributively('words', list_arg_divider, occur_reducer)
def distribute_processes_w_arg_divide(valid_keys, words, simulate_activity_coef):
    return calculate(valid_keys, words, simulate_activity_coef)

@run_distributively('words', list_arg_divider, occur_reducer, in_process=True)
def distribute_threads_w_arg_divide(valid_keys, words, simulate_activity_coef):
    return calculate(valid_keys, words, simulate_activity_coef)

@run_distributively('words', None, occur_reducer)
def distribute_processes_wo_arg_divide(valid_keys, words, simulate_activity_coef):
    return calculate(valid_keys, words, simulate_activity_coef)

@run_distributively('words', None, occur_reducer, in_process=True)
def distribute_threads_wo_arg_divide(valid_keys, words, simulate_activity_coef):
    return calculate(valid_keys, words, simulate_activity_coef)

def distribute_undecorated(valid_keys, words, simulate_activity_coef):
    return calculate(valid_keys, words, simulate_activity_coef)

def main(repeat_times, simulate_activity_coef):
    logger = logging.getLogger()
    logging.basicConfig(level=logging.DEBUG)

    data = prepare_data()
    valid_keys = data.valid_keys
    words = data.words * repeat_times
    starting_time = time.time()
    distribute_processes_w_arg_divide(valid_keys, words, simulate_activity_coef)
    logger.info('Time to run distribute_processes_w_arg_divide: ' +
                str(time.time()-starting_time))

    starting_time = time.time()
    distribute_threads_w_arg_divide(valid_keys, words, simulate_activity_coef)
    logger.info('Time to run distribute_threads_w_arg_divide: ' +
                str(time.time()- starting_time))

    starting_time = time.time()
    distribute_processes_wo_arg_divide(valid_keys, words, simulate_activity_coef)
    logger.info('Time to run distribute_processes_wo_arg_divide: ' +
                str(time.time()- starting_time))

    starting_time = time.time()
    distribute_threads_wo_arg_divide(valid_keys, words, simulate_activity_coef)
    logger.info('Time to run distribute_threads_wo_arg_divide: ' +
                str(time.time()- starting_time))

    starting_time = time.time()
    distribute_undecorated(valid_keys, words, simulate_activity_coef)
    logger.info('Time to run distribute_undecorated: ' +
                str(time.time()- starting_time))

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option(
        '-r', '--repeat-times',
        dest='repeat_times',
        default=1,
        help='Number of times to produce list of char combos')
    parser.add_option(
        '-s', '--simulate-activity-coef',
        dest='simulate_activity_coef',
        default=1000,
        help='Number of times to swap every word, to simulate cpu-intensive op')
    
    (options, args) = parser.parse_args()
    main(int(options.repeat_times), int(options.simulate_activity_coef))
