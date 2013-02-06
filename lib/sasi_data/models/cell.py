class Cell(object):
    def __init__(self, id=None, geom=None, area=None,
                 habitat_composition=None, depth=None, geom_wkt=None):
        self.id = id
        self.geom = geom
        self.area = area
        self.habitat_composition = habitat_composition
        self.depth = depth
        self.geom_wkt = geom_wkt
