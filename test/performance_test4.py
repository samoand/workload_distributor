import performance_test_base
import logging
import time
from optparse import OptionParser

def main(repeat_times, simulate_activity_coef):
    logger = logging.getLogger()
    logging.basicConfig(level=logging.DEBUG)

    data = performance_test_base.prepare_data()
    valid_keys = data.valid_keys
    words = data.words * repeat_times

    starting_time = time.time()
    performance_test_base.distribute_threads_wo_arg_divide(
        valid_keys, words, simulate_activity_coef)
    logger.info('Time to run distribute_threads_wo_arg_divide: ' +
                str(time.time() - starting_time))


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
