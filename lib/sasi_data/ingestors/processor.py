class Processor(object):
    """ A Processor is one item in a possible chain of processors.
    Each processor receives data as input, and returns a result
    that is passed to the next processor.
    In the future, processors could be keyed with ids to allow for
    more complex handling, but that seems too complicated for right
    now.
    """
    def process(data=None, counter=None, total=None, **kwargs):
        pass
