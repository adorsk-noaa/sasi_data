import sasi_data.models as models
import sasi_data.util.gis as gis_util
import sasi_data.util.shapefile as shapefile_util
import tempfile
import csv
import os
import zipfile
import json


def frange(start, end=None, inc=None):
    "A range function, that does accept float increments..."
    if end == None:
        end = start + 0.0
        start = 0.0
    if inc == None:
        inc = 1.0
    L = []
    while 1:
        next = start + len(L) * inc
        if inc > 0 and next >= end:
            break
        elif inc < 0 and next <= end:
            break
        L.append(next)
    return L

class FakeGeom(object):
    def __init__(self, wkb):
        self.geom_wkb = wkb

def generate_cell(id=0, x0=0, x1=1, y0=0, y1=1, geom_type='MultiPolygon',
                  **kwargs):
    geojson = generate_polygon_geojson(id=id, x0=x0, y0=y0, x1=x1, y1=y1,
                                       geom_type=geom_type)
    return models.Cell(
        id=id,
        geom=FakeGeom(gis_util.geojson_to_wkb(geojson)),
        **kwargs
    )

def generate_habitat(id=0, x0=0, x1=1, y0=0, y1=1, geom_type='MultiPolygon',
                     **kwargs):
    geojson = generate_polygon_geojson(id=id, x0=x0, y0=y0, x1=x1, y1=y1, 
                                       geom_type=geom_type)
    return models.Habitat(
        id=id,
        geom=FakeGeom(gis_util.geojson_to_wkb(geojson)),
        **kwargs
    )

def generate_polygon_geojson(id=None, x0=0, x1=1, y0=0, y1=1,
                             geom_type='MultiPolygon'):
    coords = [generate_polygon_coords(x0=x0, y0=y0, x1=x1, y1=y1)]
    if geom_type == 'MultiPolygon':
        coords = [coords]
    return {
        'type': geom_type,
        'coordinates': coords
    }

def generate_cell_grid(x0=0, xf=3, y0=0, yf=3, dx=1, dy=1,
                       geom_type='MultiPolygon'):
    grid = []
    cell_counter = 1
    for x in frange(x0, xf, dx):
        for y in frange(y0, yf, dy):
            cell = generate_cell(
                id=cell_counter,
                x0=x,
                x1=x+dx,
                y0=y,
                y1=y+dy,
                geom_type=geom_type
            )
            grid.append(cell)
            cell_counter += 1
    return grid

def generate_habitat_grid(x0=0, xf=3, y0=0, yf=3, dx=1, dy=1,
                       geom_type='MultiPolygon', substrates=None, energys=None):
    grid = []
    i = 1
    for x in frange(x0, xf, dx):
        for y in frange(y0, yf, dy):
            substrate_id = None
            if substrates:
                substrate_id = substrates[i % len(substrates)].id
            energy_id = None
            if energys:
                energy_id = energys[i % len(energys)].id
            hab = generate_habitat(
                id=i,
                x0=x,
                x1=x+dx,
                y0=y,
                y1=y+dy,
                geom_type=geom_type,
                substrate_id=substrate_id,
                energy_id=energy_id,
                z=i * 10.0,
            )
            grid.append(hab)
            i += 1
    return grid

def generate_feature_categories(n=2):
    feature_categories = []
    for i in range(n):
        feature_categories.append(models.FeatureCategory(
            id="FC%s" % i,
            label="FeatureCategory %s" % i,
            description="Description for feature category %s" % i,
        ))
    return feature_categories

def generate_features(n=2, feature_categories=None):
    if not feature_categories:
        feature_categories = generate_feature_categories()
    features = []
    feature_counter = 1
    for category in feature_categories:
        for i in range(n):
            features.append(models.Feature(
                id="F%s" % feature_counter,
                label="Feature %s" % feature_counter,
                description="Description for feature %s" % feature_counter,
                category=category.id,
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
            id="GC%s" % counter,
            label="Gear %s" % counter,
            description="Description for gear %s" % counter,
        ))
        counter += 1
    return gears

def generate_energys(n=2):
    energys = []
    counter = 1
    for i in range(n):
        energys.append(models.Energy(
            id="E%s" % counter,
            label="Energy %s" % counter,
            description="Description for energy %s" % counter,
        ))
        counter += 1
    return energys

def generate_efforts(cells=[], gears=[], t0=0, tf=1, dt=1):
    efforts = []
    for cell in cells:
        cell_area = gis_util.get_shape_area(
            gis_util.wkb_to_shape(cell.geom.geom_wkb))
        for t in range(t0, tf, dt):
            for g in gears:
                efforts.append(models.Effort(
                    cell_id=cell.id,
                    gear_id=g.id,
                    time=t,
                    a=cell_area/len(gears),
                ))
    return efforts

def generate_results(times=range(3), cells=None, energys=None, features=None,
                     substrates=None, gears=None):
    if not cells:
        cells = generate_cell_grid()
    if not energys:
        energys = generate_energys()
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
            for e in energys:
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
                            counter += 1
    return results

def generate_map_layer(layer_id="layer", layer_dir=None):
    if not layer_dir:
        layer_dir = tempfile.mkdtemp(prefix="layer.")
    shpfile = os.path.join(layer_dir, "%s.shp" % layer_id)
    writer = shapefile_util.get_shapefile_writer(
        shapefile=shpfile, driver='ESRI Shapefile',
        crs='EPSG:4326',
        schema={
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

    # Write Mapfile.
    mapfile = os.path.join(layer_dir, "%s.map" % layer_id)
    open(mapfile, "w").write(get_mapfile(layer_id))

    # Write config file.
    config_file= os.path.join(layer_dir, "config.json")
    config = {"mapfile": "%s.map" % layer_id}
    open(config_file, 'w').write(json.dumps(config))

    return layer_dir

def get_mapfile(layer_id):
    return """
MAP
  EXTENT -180 -90 180 90
  IMAGETYPE "image/gif"
  NAME "%s"
  SIZE 640 640
  STATUS ON
  UNITS DD
  PROJECTION
    "init=epsg:4326"
  END
  WEB
    METADATA
      "ows_enable_request"	"*"
    END
  END
  LAYER
    STATUS ON
    NAME "%s"
    DATA "%s"
    TYPE POLYGON
    CLASS
      STYLE
        COLOR 0 0 0
      END # STYLE
    END # CLASS
  END # LAYER
END # MAP
""" % (layer_id, layer_id, layer_id)

def generate_data_dir(data_dir="", data={}, time_start=0, time_end=10, 
                      time_step=1, effort_model='realized', 
                      default_grid_size=2, to_zipfile=False):
    if not data_dir:
        data_dir = tempfile.mkdtemp(prefix="tst.")

    # Generate data and sections for generic sections.
    data.setdefault('substrates', generate_substrates())
    data.setdefault('energys', generate_energys())
    data.setdefault('feature_categories', generate_feature_categories())
    data.setdefault('features', generate_features(
        feature_categories=data['feature_categories']))
    data.setdefault('gears', generate_gears())

    # Generate VA data.
    if not data.get('va'):
        va_data = []
        i = 0
        for s in data['substrates']:
            for e in data['energys']:
                for f in data['features']:
                    for g in data['gears']:
                        va_data.append({
                            'Gear ID': g.id,
                            'Feature ID': f.id,
                            'Substrate ID': s.id,
                            'Energy': e.id,
                            'S': (i % 3) + 1,
                            'R': (i % 3) + 1,
                        })
                        i += 1
        data['va'] = va_data

    # Generate grid data.
    data.setdefault('grid', generate_cell_grid(
        xf=default_grid_size, yf=default_grid_size))

    # Generate habitat data.
    data.setdefault('habitats', generate_habitat_grid(
        x0=-.5, xf=default_grid_size +.5, y0=-.5, yf=default_grid_size + .5,
        substrates=data['substrates'],
        energys=data['energys'],
    ))

    sections = {}
    sections['substrates'] = {
        'id': 'substrates',
        'type': 'csv',
        'fields': ['id', 'label', 'description'],
    }

    sections['energys'] = {
        'id': 'energies',
        'type': 'csv',
        'fields': ['id', 'label', 'description'],
    }

    sections['feature_categories'] = {
        'id': 'feature_categories',
        'type': 'csv',
        'fields': ['id', 'label', 'description'],
    }
        
    sections['features'] = {
        'id': 'features',
        'type': 'csv',
        'fields': ['id', 'category', 'label', 'description'],
    }

    sections['gears'] = {
        'id': 'gears',
        'type': 'csv',
        'fields': ['id', 'label', 'description'],
    }

    for section_name in ['substrates', 'energys', 'feature_categories',
                         'features', 'gears']:
        section_data = data[section_name]
        section = sections[section_name]
        section['data'] = []
        for obj in section_data:
            section['data'].append(dict(
                [(attr, getattr(obj, attr, None)) for attr in section['fields']]
            ))

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

    sections['va'] = {
        'id': 'va',
        'type': 'csv',
        'fields': ['Gear ID', 'Feature ID', 'Substrate ID', 'Energy', 
                   'S', 'R'],
        'data': data['va'],
    }

    # Shape sections.
    hab_records = []
    hab_counter = 0
    for hab in data['habitats']:
        hab_records.append({
            'id': hab_counter,
            'geometry': json.loads(
                gis_util.wkb_to_geojson(hab.geom.geom_wkb)),
            'properties': {
                'SUBSTRATE': getattr(hab, 'substrate_id', ''),
                'ENERGY': getattr(hab, 'energy_id', ''),
                'Z': getattr(hab, 'z', 0.0),
            }
        })
        hab_counter += 1
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
    cell_counter = 0
    for cell in data['grid']:
        cell_records.append({
            'id': cell_counter,
            'geometry': json.loads(
                gis_util.wkb_to_geojson(cell.geom.geom_wkb)),
            'properties': {
                'ID': str(cell_counter),
            }
        })
        cell_counter += 1
    sections['grid'] = {
        'id': 'grid',
        'type': 'shp',
        'schema': {
            'geometry': 'MultiPolygon',
            'properties': {
                'ID': 'str',
            }
        },
        'records': cell_records
    }

    if effort_model == 'realized':
        data.setdefault('fishing_efforts', generate_efforts(
            cells=data['grid'], gears=data['gears'], t0=time_start, tf=time_end,
            dt=time_step))

        fields = ['cell_id', 'time', 'a', 'gear_id', 'value', 'hours_fished']
        sections['fishing_efforts'] = {
            'id': 'fishing_efforts',
            'type': 'fishing_efforts',
            'model_type': 'realized',
            'fields': fields,
            'data': [dict(zip(fields, [getattr(e, f, None) for f in fields]))
                     for e in data['fishing_efforts']]
        }
    elif effort_model == 'nominal':
        sections['fishing_efforts'] = {
            'id': 'fishing_efforts',
            'type': 'fishing_efforts',
            'model_type': 'nominal',
        }

    sections['georefine'] = {
        'id': 'georefine',
        'type': 'georefine',
    }

    for s in sections.values():
        if s['type'] == 'csv':
            generate_csv_section(data_dir, s)
        elif s['type'] == 'shp':
            generate_shp_section(data_dir, s)
        elif s['type'] == 'fishing_efforts':
            generate_fishing_efforts_section(data_dir, s)
        elif s['type'] == 'georefine':
            generate_georefine_sections(data_dir, s)

    if to_zipfile:
        def zipdir(basedir, archivename, basename=None):
            z = zipfile.ZipFile(archivename, "w", zipfile.ZIP_DEFLATED)
            for root, dirs, files in os.walk(basedir):
                for fn in files:
                    absfn = os.path.join(root, fn)
                    zf_path_parts = [absfn[len(basedir)+len(os.sep):]]
                    if basename:
                        zf_path_parts.insert(0, basename)
                    zfn = os.path.join(*zf_path_parts)
                    z.write(absfn, zfn)
        zipdir(data_dir, to_zipfile)
        return to_zipfile
    else:
        return data_dir

def setup_section_dirs(data_dir, section):
    section_dir = os.path.join(data_dir, section['id'])
    if not os.path.exists(section_dir):
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
        crs='EPSG:4326',
        schema=section['schema'],
    )
    for record in section['records']:
        w.write(record)
    w.close()

def generate_fishing_efforts_section(data_dir, section):
    section_dir = os.path.join(data_dir, section['id'])
    os.makedirs(section_dir)
    w = csv.writer(open(os.path.join(section_dir, 'model.csv'),'w'))
    w.writerow(['model_type'])
    w.writerow([section['model_type']])
    if section['model_type'] == 'realized':
        generate_csv_section(data_dir, section)

def generate_georefine_sections(data_dir, section):

    map_layers_data = []
    for i in range(4):
        if (i % 2) == 0:
            layer_category = 'base'
            transparent = None
        else:
            layer_category = 'overlay'
            transparent = True

        map_layers_data.append({
            'id': "layer%s" % i,
            'label': "Layer %s" % i,
            'description': "layer%s description" % i,
            'layer_category': layer_category,
            'source': 'georefine_wms_layer',
            'layer_type': 'WMS',
            'transparent': transparent
        })
    map_layers_section = {
        'id': 'map_layers',
        'type': 'map_layers',
        'fields': [
            'id', 
            'label', 
            'description',
            'layer_category',
            'source',
            'layer_type',
            'transparent'
        ],
        'data': map_layers_data
    }
    generate_map_layers_section(data_dir, map_layers_section)

    map_parameters_section = {
        'id': 'map_parameters',
        'type': 'csv',
        'fields': ['max_extent', 'graticule_intervals', 'resolutions'], 
        'data': [{
            'max_extent': '[0, 0, 5, 5]',
            'graticule_intervals': '[2]',
            'resolutions': '[0.025, 0.0125, 0.00625, 0.003125, 0.0015625, 0.00078125]'
        }]
    }
    generate_csv_section(data_dir, map_parameters_section)

def generate_map_layers_section(data_dir, section):
    generate_csv_section(data_dir, section)
    section_data_dir = os.path.join(data_dir, section['id'], "data")
    map_layers_dir = os.path.join(section_data_dir)
    for layer in section['data']:
        layer_dir = os.path.join(map_layers_dir, layer['id'])
        os.mkdir(layer_dir)
        generate_map_layer(layer_id=layer['id'], layer_dir=layer_dir)

def generate_polygon_coords(x0=0, y0=0, x1=1, y1=1):
    coords = [[x0, y0], [x0, y1], [x1, y1], [x1, y0], [x0, y0]]
    return coords

def generate_multipolygon_wkt(**kwargs):
    coords = generate_polygon_coords(**kwargs)
    wkt = "MULTIPOLYGON(((%s)))" % (','.join(["%s %s" % (c[0], c[1]) for c in coords]))
    return wkt
