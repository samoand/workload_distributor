Utility to distributively run a function with signature that contains a
large iterable argument either in args or kwargs.

Typically used as a decorator.

run_distributively
- divides the large argument.
- runs decorated function distributively (either in threads or in processes)
- builds end result from partial results.

Motivation:

- to provide a standard way to split up a dividable argument and build
  end result from partials.

- it may be desirable to "map" args to function in a more
  economical way than provided by vanilla Pool/map approach. For
  example, if in addition to a dividable argument one or more of
  function arguments is of large size, it's desirable to minimize
  serialization and string passing overhead when function is mapped to
  multiple processes. This is demonstrated in test/performance_test*

How-To:

Define arg_divider: it should take 2 arguments:

       - value that is to be divided into parts
       - desirable number of chunks to divide

       Implementation should return the parts. Their structure is to
       be consistent with the signature of the decorated method
       arg_divider may be None. In this case the result would be
       similar to vanilla Pool/map approach

Define result_reducer:

       it's argument is a list of partial result. It should use that
       list to build final result.

See tests for examples on how to use run_distributively as a decorator.

Supported semantics:

  - arg divider
  - result reducer
  - policy for exception handling

