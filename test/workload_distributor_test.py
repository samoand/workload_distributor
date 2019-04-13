import logging
import os
import unittest2 as unittest
import re

from workload_distributor import MappedException, run_distributively

logger = logging.getLogger()

def text_mapper(text, suggested_num_chunks):
    """
    text: to be divided
    suggested_num_chunks: for compliance with mapper signature,
                            not used
    """
    return text.split('\n') if text else None

def num_mapper(nums, suggested_num_chunks):
    """
    text: to be divided
    suggested_num_chunks: suggested number of chunks
                            typically equal to num of cores
    """
    chunk_size = max(len(nums) // suggested_num_chunks, 1)
    return [nums[i:i+chunk_size] for i in range(0, len(nums), chunk_size)]

def total_reducer(partials):
    return sum(partials)

def count_words(text):
    count = len(re.findall(r'\w+', text))
    logger.info('Returning %d from process %s. This process works with text "%s"\n'
                % (count, os.getpid(), text))
    return count

def count_substrings(text, substr):
    count = text.count(substr)
    logger.info('Returning %d from process %s. This process works with text "%s"\n'
                % (count, os.getpid(), text))
    return count

class UnitTestCase(unittest.TestCase):
    def setUp(self):
        self.long_text = """this is what could
be a very
long text.
It contains
multiple lines
this
is by design
just because
it looks cool
and awesome"""
        self.long_text_as_list = ['this is what could',
                                  'be a very',
                                  'long text.',
                                  'It contains',
                                  'multiple lines',
                                  'this',
                                  'is by design',
                                  'just because',
                                  'it looks cool',
                                  'and awesome']
        self.short_text = 'Plain'
        self.no_text = ''

        self.nums = range(10000)