class Gear(object):
    def __init__(self, id=None, label=None, description=None,
                 min_depth=None, max_depth=None):
        self.id = id
        self.label = label
        self.description = description
        self.min_depth = min_depth
        self.max_depth = max_depth
