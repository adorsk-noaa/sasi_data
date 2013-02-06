class Habitat(object):

    def __init__(self, id=None, substrate_id=None, energy_id=None, depth=None, area=None, geom=None, features=None):
        self.id = id
        self.substrate_id = substrate_id
        self.energy_id = energy_id
        self.depth = depth
        self.area = area
        self.geom = geom
        self.features = features
