import sasi_data.models as models
import sasi_data.util.gis as gis_util
import sasi_data.util.shapefile as shapefile_util
import os



class FakeGeom(object):
    def __init__(self, wkb):
        self.geom_wkb = wkb

def generate_cell_grid(x0=0, xf=3, y0=0, yf=3, dx=1, dy=1, cell_type="100km"):
    grid = []
    cell_counter = 1
    for x in range(x0, xf, dx):
        for y in range(y0, yf, dy):
            coords = [[x, y], [x, y+dy], [x+dx, y+dy], [x+dx, y], [x, y]]
            geojson = {
                "type": "MultiPolygon",
                "coordinates": [[coords]]
            }
            geom = FakeGeom(gis_util.geojson_to_wkb(geojson))
            grid.append(models.Cell(
                id=cell_counter,
                type=cell_type,
                type_id=cell_counter,
                area=float(cell_counter),
                geom=geom
            ))
            cell_counter += 1
    return grid

def generate_features(n=2, categories=['bio', 'geo']):
    features = []
    feature_counter = 1
    for category in categories:
        for i in range(n):
            features.append(models.Feature(
                id="F%s" % feature_counter,
                label="Feature %s" % feature_counter,
                description="Description for feature %s" % feature_counter,
                category=category,
            ))
            feature_counter += 1
    return features

def generate_substrates(n=2):
    substrates = []
    counter = 1
    for i in range(n):
        substrates.append(models.Substrate(
            id="S%s" % counter,
            label="Substrate %s" % counter,
            description="Description for substrate %s" % counter,
        ))
        counter += 1
    return substrates

def generate_gears(n=2):
    gears = []
    counter = 1
    for i in range(n):
        gears.append(models.Gear(
            id="G%s" % counter,
            label="Gear %s" % counter,
            description="Description for gear %s" % counter,
        ))
        counter += 1
    return gears

def generate_energies(n=2):
    energies = []
    counter = 1
    for i in range(n):
        energies.append(models.Energy(
            id="E%s" % counter,
            label="Energy %s" % counter,
            description="Description for energy %s" % counter,
        ))
        counter += 1
    return energies

def generate_results(times=range(3), cells=None, energies=None, features=None,
                     substrates=None, gears=None):
    if not cells:
        cells = generate_cell_grid()
    if not energies:
        energies = generate_energies()
    if not features:
        features = generate_features()
    if not substrates:
        substrates = generate_substrates()
    if not gears:
        gears = generate_gears()

    results = []
    counter = 1
    for t in times:
        for c in cells:
            for e in energies:
                for f in features:
                    for s in substrates:
                        for g in gears:
                            results.append(models.Result(
                                id=counter,
                                t=t,
                                cell_id=c.id,
                                gear_id=g.id,
                                substrate_id=s.id,
                                energy_id=e.id,
                                feature_id=f.id,
                                a=counter,
                                x=counter,
                                y=counter,
                                z=counter,
                                znet=counter
                            ))
    return results

def generate_map_layer(layer_id=None, layer_dir=None):
    shpfile = os.path.join(layer_dir, "%s.shp" % layer_id)
    writer = shapefile_util.get_shapefile_writer(
        shapefile=shpfile, driver='ESRI Shapefile', crs={
            'no_defs': True, 'ellps': 'WGS84', 'datum': 'WGS84', 
            'proj': 'longlat' }, schema={
                'geometry': 'MultiPolygon',
                'properties': {
                    'INT_ATTR': 'int',
                    'STR_ATTR': 'str',
                }
            },
    )
    for j in range(3):
        coords = [[j, j], [j,j+1], [j+1, j+1], [j+1,j], [j,j]]
        record = {
            'id': j,
            'geometry': {
                'type': 'MultiPolygon',
                'coordinates': [[coords]]
            },
            'properties': {
                'INT_ATTR': j,
                'STR_ATTR': "str_%s" % layer_id
            }
        }
        writer.write(record)
    writer.close()

    # Write SLD.
    sld_file = os.path.join(layer_dir, "%s.sld" % layer_id)
    open(sld_file, "w").write(get_sld(layer_id))

def get_sld(layer_id):
    return """
<?xml version="1.0" encoding="ISO-8859-1"?>
<StyledLayerDescriptor version="1.0.0" 
    xsi:schemaLocation="http://www.opengis.net/sld StyledLayerDescriptor.xsd" 
    xmlns="http://www.opengis.net/sld" 
    xmlns:ogc="http://www.opengis.net/ogc" 
    xmlns:xlink="http://www.w3.org/1999/xlink" 
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <NamedLayer>
    <Name>%s</Name>
    <UserStyle>
      <Title>Simple polygon</Title>
      <FeatureTypeStyle>
        <Rule>
          <PolygonSymbolizer>
            <Fill>
              <CssParameter name="fill">#800080</CssParameter>
            </Fill>
          </PolygonSymbolizer>
        </Rule>
      </FeatureTypeStyle>
    </UserStyle>
  </NamedLayer>
</StyledLayerDescriptor>
""" % layer_id
