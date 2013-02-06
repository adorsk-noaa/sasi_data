class FishingResult(object):

    def __init__(self, id=None, t=None, cell_id=None, gear_id=None,
                 a=None, hours_fished=None, hours_fished_net=None, 
                 value=None, value_net=None):
        self.id = id
        self.t = t
        self.cell_id = cell_id
        self.gear_id = gear_id
        self.a = a
        self.hours_fished = hours_fished
        self.hours_fished_net = hours_fished_net
        self.value = value
        self.value_net = value_net

    # Convenience methods to use results like dicts.
    def __getitem__(self, key):
        return self.__dict__.__getitem__(key)
    def __setitem__(self, key, value):
        return self.__dict__.__setitem__(key, value)
    def __delitem__(self, key):
        return self.__dict__.__delitem__(key)
    def setdefault(self, *args, **kwargs):
        return self.__dict__.setdefault(*args, **kwargs)
