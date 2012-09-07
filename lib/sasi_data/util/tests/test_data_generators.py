import unittest
import sasi_data.util.data_generators as data_generators


class DataGeneratorsTest(unittest.TestCase):

    def test_generate_cell_grid(self):
        grid = data_generators.generate_cell_grid()

    def test_generate_features(self):
        features = data_generators.generate_features()

    def test_generate_substrates(self):
        substrates = data_generators.generate_substrates()

    def test_generate_gears(self):
        gears = data_generators.generate_gears()

    def test_generate_energies(self):
        energies = data_generators.generate_energies()

    def test_generate_results(self):
        results = data_generators.generate_results()

if __name__ == '__main__':
    unittest.main()
