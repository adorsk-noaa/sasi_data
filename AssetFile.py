config = {
    'CACHE_DIR': 'cache',
    'TARGET_DIR': 'assets'
}

assets = {
    'sa_dao' : {
        'type': 'git',
        'source': 'https://github.com/adorsk-noaa/sqlalchemy_dao.git',
        'path': 'lib/sa_dao',
    },
    'sqlalchemy' : {
        'type': 'hg',
        'source': 'https://adorsk@bitbucket.org/adorsk/sqlalchemy',
        'path': 'lib/sqlalchemy',
    },
    'geoalchemy': {
        'type': 'git',
        'source': 'https://github.com/adorsk/geoalchemy.git',
        'path': 'geoalchemy',
    },
}
