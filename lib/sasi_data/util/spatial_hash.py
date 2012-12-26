from math import floor


class SpatialHash(object):
    def __init__(self, cell_size=1):
        self.cell_size = float(cell_size)
        self.d = {}

    def _add(self, cell_coord, o):
        """Add the object o to the cell at cell_coord."""
        try:
            self.d.setdefault(cell_coord, set()).add(o)
        except KeyError:
            self.d[cell_coord] = set((o,))

    def _cell_for_point(self, p):
        cx = floor(p[0]/self.cell_size)
        cy = floor(p[1]/self.cell_size)
        return (int(cx), int(cy))

    def _cells_for_rect(self, r):
        cells = set()
        cy = floor(r[1] / self.cell_size)
        while (cy * self.cell_size) <= r[3]:
            cx = floor(r[0] / self.cell_size)
            while (cx * self.cell_size) <= r[2]:
                cells.add((int(cx), int(cy)))
                cx += 1.0
            cy += 1.0
        return cells

    def add_rect(self, r, obj):
        cells = self._cells_for_rect(r)
        for c in cells:
            self._add(c, obj)

    def items_for_point(self, p):
        cell = self._cell_for_point(p)
        return self.d.get(cell, set())

    def items_for_rect(self, r):
        cells = self._cells_for_rect(r)
        items = set()
        for c in cells:
            items.update(self.d.get(c, set()))
        return items
