"""
Module that implements a retry decorator.

You can, for example, do this:

    @retry(5)
    def my_function():
        ...

And 'my_function', upon an exception, will be retried 4 more times until
a final exception is raised. 'retry' will wait a little bit longer after each
failure before retrying.

Very useful for, for example, retrying a download if timeouts occur frequently.
Customization of exceptions and exception handlers is possible.
"""

from time import sleep
from functools import wraps


def _warning_printer(func, exception, tries_remaining):
    """Simple exception handler that prints a warning.

    :param exception: The exception instance which was raised
    :param int tries_remaining: The number of tries remaining
    """
    print("Caught '{0}' in {1}, {2} tries remaining.".format(
        exception, func.__name__, tries_remaining))


def _error_printer(func, exception, tries):
    """Exception handler that prints an error.

    :param exception: The exception instance which was raised
    :param int tries: Total number of tries
    """

    try:
        print("{} failed (reason: {}), giving up after {} tries.".format(
            func.__name__, exception.reason, int(tries)))
    except AttributeError:
        print("{} failed, giving up after {} tries.".format(
            func.__name__, int(tries)))


def retry(max_tries, delay=1, backoff=2, exceptions=(Exception,),
          on_retry=_warning_printer, on_fail=_error_printer):
    """Function decorator implementing retry logic.

    The decorator will call the function up to max_tries times if it raises
    an exception.

    By default it catches instances of the Exception class and subclasses.
    This will recover after all but the most fatal errors. You may specify a
    custom tuple of exception classes with the 'exceptions' argument; the
    function will only be retried if it raises one of the specified
    exceptions.

    Additionally you may specify a on_retry function which will be
    called prior to retrying with the number of remaining tries and the
    exception instance. This is primarily intended to give the opportunity to
    log the failure. on_fail is another function called after failure if no
    retries remain.

    :param int max_tries: Maximum number of retries
    :param int or float delay: Sleep this many seconds * backoff *
        try number after failure
    :param int or float backoff: Multiply delay by this after each failure
    :param tuple exceptions: A tuple of exception classes; default (Exception,)
    :param func on_retry: An on-retry exception handler function
        (args should be: function, exception, tries_remaining)
    :param func on_fail: A final exception handler function
        (args should be: function, exception, tries_remaining)
    """

    assert max_tries > 0

    def dec(func):
        # 'wraps' updates a wrapper function to look like the wrapped function
        @wraps(func)
        def f2(*args, **kwargs):
            mydelay = delay
            tries = reversed(range(max_tries))
            for tries_remaining in tries:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if tries_remaining > 0:
                        # call on_retry exception handler after an exception
                        if on_retry is not None:
                            on_retry(func, e, tries_remaining)
                        sleep(mydelay)
                        mydelay *= backoff
                    else:
                        # no more retries, call the on_fail exception handler
                        if on_fail is not None:
                            on_fail(func, e, max_tries)
                        else:
                            raise e
        return f2
    return dec
