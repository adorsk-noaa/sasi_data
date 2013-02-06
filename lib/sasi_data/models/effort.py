class Effort(object):
    def __init__(self, id=None, generic_id=None, is_generic=None, cell_id=None,
                 time=None, gear_id=None, a=None, hours_fished=None, 
                 value=None):
        self.id = id
        self.generic_id = generic_id
        self.is_generic = is_generic
        self.cell_id = cell_id
        self.time = time
        self.gear_id = gear_id
        self.a = a
        self.hours_fished = hours_fished
        self.value = value
