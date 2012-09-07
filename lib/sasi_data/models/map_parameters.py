class MapParameters(object):
    def __init__(self, max_extent=None, graticule_intervals=None,
                 resolutions=[]):
        self.max_extent = max_extent
        self.graticule_intervals = graticule_intervals
        self.resolutions = resolutions
