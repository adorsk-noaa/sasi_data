class Result(object):

    def __init__(self, id=None, t=None, cell_id=None, gear_id=None,
                 substrate_id=None, energy_id=None, feature_id=None,
                 feature_category_id=None, a=None, x=None, y=None, z=None, 
                 znet=None, hours_fished=None, value=None):
        self.id = id
        self.t = t
        self.cell_id = cell_id
        self.gear_id = gear_id
        self.substrate_id = substrate_id
        self.energy_id = energy_id
        self.feature_id = feature_id
        self.feature_category_id = feature_category_id
        self.a = a
        self.x = x
        self.y = y
        self.z = z
        self.znet = znet
        self.hours_fished = hours_fished
        self.value = value

    # Convenience methods to use results like dicts.
    def __getitem__(self, key):
        return self.__dict__.__getitem__(key)
    def __setitem__(self, key, value):
        return self.__dict__.__setitem__(key, value)
    def __delitem__(self, key):
        return self.__dict__.__delitem__(key)
    def setdefault(self, *args, **kwargs):
        return self.__dict__.setdefault(*args, **kwargs)
