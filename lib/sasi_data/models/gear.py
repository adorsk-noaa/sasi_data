class Gear(object):
    def __init__(self, id=None, generic_id=None, label=None, description=None,
                 is_generic=None, min_depth=None, max_depth=None):
        self.id = id
        self.generic_id = generic_id
        self.is_generic = is_generic
        self.label = label
        self.description = description
        self.min_depth = min_depth
        self.max_depth = max_depth
