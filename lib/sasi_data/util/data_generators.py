import sasi_data.models as models
import sasi_data.util.gis as gis_util
import sasi_data.util.shapefile as shapefile_util
import tempfile
import csv
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

def generate_data_dir(data_dir="", time_start=0, time_end=10, time_step=1):
    if not data_dir:
        data_dir = tempfile.mkdtemp(prefix="tst.")

    sections = {}
    sections['substrates'] = {
        'id': 'substrates',
        'type': 'csv',
        'fields': ['id'],
        'data': [{'id': "S%s" % i} for i in range(5)]
    }

    sections['energies'] = {
        'id': 'energies',
        'type': 'csv',
        'fields': ['id'],
        'data': [{'id': "High"}, {'id': 'Low'}]
    }
        
    sections['features'] = {
        'id': 'features',
        'type': 'csv',
        'fields': ['id', 'category'],
        'data': [{'id': "F%s" % i, 'category': ('bio' if i % 2 else 'geo')}
                 for i in range(10) ]
    }

    sections['gears'] = {
        'id': 'gears',
        'type': 'csv',
        'fields': ['id'],
        'data': [{'id': "G%s" % i} for i in range(5)]
    }

    sections['model_parameters'] = {
        'id': 'model_parameters',
        'type': 'csv',
        'fields': ['time_start', 'time_end', 'time_step', 
                   't_0', 't_1', 't_2', 't_3', 
                   'w_0', 'w_1', 'w_2', 'w_3', 
                   'projection'],
        'data': [{'time_start': time_start, 'time_end': time_end, 'time_step':
                  time_step, 
                  't_0': 0, 't_1': 1, 't_2': 2, 't_3': 3, 
                  'w_0': 0, 'w_1': .1, 'w_2': .2, 'w_3':.3, 
                  'projection': None}]
    }


    va_data = []
    i = 0
    for s in sections['substrates']['data']:
        for e in sections['energies']['data']:
            for f in sections['features']['data']:
                for g in sections['gears']['data']:
                    va_data.append({
                        'Gear ID': g['id'],
                        'Feature ID': f['id'],
                        'Substrate ID': s['id'],
                        'Energy': e['id'],
                        'S': (i % 3) + 1,
                        'R': (i % 3) + 1,
                    })
                    i += 1
    sections['va'] = {
        'id': 'va',
        'type': 'csv',
        'fields': ['Gear ID', 'Feature ID', 'Substrate ID', 'Energy', 
                   'S', 'R'],
        'data': va_data
    }

    grid_size = 3

    hab_records = []
    i = 0
    for j in range(-2, grid_size + 2):
        for k in range(-2, grid_size + 2):
            x = j * 2
            y = k * 2
            substrate_data = sections['substrates']['data']
            substrate = substrate_data[i % len(substrate_data)]['id']
            energy_data = sections['energies']['data']
            energy = energy_data[i % len(energy_data)]['id']
            z = -1.0 * i
            coords = [[x,y], [x, y+2], [x+2, y+2], [x+2, y], [x,y]]
            hab_records.append({
                'id': i,
                'geometry': {
                    'type': 'MultiPolygon',
                    'coordinates': [[coords]]
                },
                'properties': {
                    'SUBSTRATE': substrate,
                    'ENERGY': energy,
                    'Z': z
                }
            })
            i += 1
    sections['habitats'] = {
        'id': 'habitats',
        'type': 'shp',
        'schema': {
            'geometry': 'MultiPolygon',
            'properties': {
                'SUBSTRATE': 'str',
                'ENERGY': 'str',
                'Z': 'float'
            }
        },
        'records': hab_records
    }

    cell_records = []
    i = 0
    for j in range(grid_size):
        for k in range(grid_size):
            x = (j * 2) + 1
            y = (k * 2) + 1
            coords = [[x,y], [x, y+2], [x+2, y+2], [x+2, y], [x,y]]
            cell_records.append({
                'id': i,
                'geometry': {
                    'type': 'MultiPolygon',
                    'coordinates': [[coords]]
                },
                'properties': {
                    'TYPE_ID': "%s" % i,
                    'TYPE': "km100"
                }
            })
            i += 1
    sections['grid'] = {
        'id': 'grid',
        'type': 'shp',
        'schema': {
            'geometry': 'MultiPolygon',
            'properties': {
                'TYPE': 'str',
                'TYPE_ID': 'str',
            }
        },
        'records': cell_records
    }

    fishing_efforts_data = []
    num_gears = len(sections['gears']['data'])
    for cell_record in sections['grid']['records']:
        cell_geom = gis_util.geojson_to_wkb(cell_record['geometry'])
        cell_area = gis_util.get_area(cell_geom)
        for t in range(time_start, time_end, time_step):
            for g in sections['gears']['data']:
                fishing_efforts_data.append({
                    'cell_id': cell_record['id'],
                    'time': t,
                    'swept_area': cell_area/num_gears,
                    'gear_id': g['id']
                })

    sections['fishing_efforts'] = {
        'id': 'fishing_efforts',
        'type': 'fishing_efforts',
        'model_type': 'realized',
        'fields': ['cell_id', 'time', 'swept_area', 'gear_id'],
        'data': fishing_efforts_data
    }

    sections['map_parameters'] = {
        'id': 'map_parameters',
        'type': 'csv',
        'fields': ['max_extent', 'graticule_intervals', 'resolutions'], 
        'data': [{
            'max_extent': '[-70, 40, -60, 50]',
            'graticule_intervals': '[2]',
            'resolutions': '[0.025, 0.0125, 0.00625, 0.003125, 0.0015625, 0.00078125]'
        }]
    }

    map_layers_data = []
    for i in range(4):
        if (i % 2) == 0:
            layer_category = 'base'
        else:
            layer_category = 'overlay'

        map_layers_data.append({
            'id': "layer%s" % i,
            'label': "Layer %s" % i,
            'description': "layer%s description" % i,
            'layer_category': layer_category,
            'source': 'local_wms',
            'layer_type': 'WMS',
        })
    sections['map_layers'] = {
        'id': 'map_layers',
        'type': 'map_layers',
        'fields': [
            'id', 
            'label', 
            'description',
            'layer_category',
            'source',
            'layer_type'
        ],
        'data': map_layers_data
    }

    for s in sections.values():
        if s['type'] == 'csv':
            generate_csv_section(data_dir, s)
        elif s['type'] == 'shp':
            generate_shp_section(data_dir, s)
        elif s['type'] == 'fishing_efforts':
            generate_fishing_efforts_section(data_dir, s)
        elif s['type'] == 'map_layers':
            generate_map_layers_section(data_dir, s)

    return data_dir

def setup_section_dirs(data_dir, section):
    section_dir = os.path.join(data_dir, section['id'])
    os.mkdir(section_dir)
    section_data_dir = os.path.join(section_dir, 'data')
    os.mkdir(section_data_dir)
    return section_data_dir

def generate_csv_section(data_dir, section):
    section_data_dir = setup_section_dirs(data_dir, section)
    w = csv.writer(
        open(os.path.join(section_data_dir, "%s.csv" % section['id']), "w"))
    w.writerow(section['fields'])
    for row in section['data']:
        w.writerow([row.get(field, '') for field in section['fields']])

def generate_shp_section(data_dir, section):
    section_data_dir = setup_section_dirs(data_dir, section)
    shpfile = os.path.join(section_data_dir, "%s.shp" % section['id'])
    w = shapefile_util.get_shapefile_writer(
        shapefile=shpfile, 
        crs={
            'no_defs': True, 'ellps': 'WGS84', 'datum': 'WGS84', 
            'proj': 'longlat'
        },
        schema=section['schema']
    )
    for record in section['records']:
        w.write(record)
    w.close()

def generate_fishing_efforts_section(data_dir, section):
    generate_csv_section(data_dir, section)
    section_dir = os.path.join(data_dir, section['id'])
    w = csv.writer(open(os.path.join(section_dir, 'model.csv'),'w'))
    w.writerow(['model_type'])
    w.writerow([section['model_type']])

def generate_map_layers_section(data_dir, section):
    generate_csv_section(data_dir, section)
    section_data_dir = os.path.join(data_dir, section['id'], "data")
    map_layers_dir = os.path.join(section_data_dir, "shapefiles")
    os.mkdir(map_layers_dir)
    for layer in section['data']:
        layer_dir = os.path.join(map_layers_dir, layer['id'])
        os.mkdir(layer_dir)
        generate_map_layer(layer_id=layer['id'], layer_dir=layer_dir)
