from org.geotools.data import DataStoreFinder
from java.lang import String, Integer, Double


if __name__ == '__main__':
    
    shapefile = '/home/adorsk/Desktop/habs_4326.shp'
    ds = DataStoreFinder.getDataStore({'url': 'file://%s' % shapefile})
    tnames = ds.getTypeNames()
    fs = ds.getFeatureSource(tnames[0])

    def get_records():
        iterator = fs.getFeatures().features()
        while (iterator.hasNext()):
            feature = iterator.next()
            yield feature

    s = fs.getSchema()
    gd = s.getGeometryDescriptor()
    gd_type = gd.getType()
    print "tname: ", gd_type.name
    attrs = s.getAttributeDescriptors()
    for a in attrs:
        if a is gd:
            print "gd!"
        else:
            name = a.getLocalName()
            type_ = a.getType().binding
            if type_ is String:
                print "str"
            elif type_ is Integer:
                print "int"
            elif type_ is Double:
                print "float"
            else:
                print "unknown, ", type_
    limit = 5
    counter = 0
    for r in get_records():
        counter += 1
        for a in attrs:
            name = a.getLocalName()
            value = r.getProperty(name).getValue()
            print "name: ", value, "(%s)" % type(value)
        if counter == limit:
            break

