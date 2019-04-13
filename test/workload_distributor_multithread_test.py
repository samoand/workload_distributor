import logging
from workload_distributor_test import (UnitTestCase,
    text_mapper, num_mapper, total_reducer, count_words, count_substrings)
import unittest2 as unittest

from workload_distributor import MappedException, run_distributively

logger = logging.getLogger()

@run_distributively('text', text_mapper, total_reducer, in_process=True)
def count_num_words(text):
    return count_words(text)

@run_distributively('text', None, total_reducer, in_process=True)
def count_num_words_no_mapper(text):
    return count_words(text)


@run_distributively('text', text_mapper, total_reducer, in_process=True)
def count_num_substr_occurrences(text, substr):
    return count_substrings(text, substr)

@run_distributively('sum_me_up', num_mapper, total_reducer, in_process=True)
def distributed_sum(sum_me_up):
    return sum(sum_me_up)

class UtilTestCase(UnitTestCase):

    def test_count_num_words_long(self):
        self.assertEqual(count_num_words(self.long_text), 24)

    def test_count_num_words_long_no_mapper(self):
        self.assertEqual(count_num_words_no_mapper(self.long_text_as_list), 24)

    def test_count_num_words_short(self):
        self.assertEqual(count_num_words(self.short_text), 1)

    def test_count_num_words_empty(self):
        self.assertEqual(count_num_words(self.no_text), 0)

    def test_count_num_words_none(self):
        with self.assertRaises(MappedException) as context:
            count_num_words(None)
        logger.info(context.exception)

    def test_count_num_substr_occurrences_long(self):
        self.assertEqual(count_num_substr_occurrences(self.long_text, 'a'), 6)

    def test_count_num_substr_occurrences_short(self):
        self.assertEqual(count_num_substr_occurrences(self.short_text, 'a'), 1)

    def test_count_num_substr_occurrences_empty(self):
        self.assertEqual(count_num_substr_occurrences(self.no_text, 'a'), 0)

    def test_count_num_substr_occurrences_none(self):
        with self.assertRaises(MappedException) as context:
            count_num_substr_occurrences(None, 'a')
        logger.info(context.exception)

    def test_count_sum(self):
        # verify that sum(self.nums) is equal to
        # sum of sums of subdivided components calculated distributively
        self.assertEqual(sum(self.nums), distributed_sum(self.nums))


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    unittest.main()
