"""Load/dump static data between configuration files and database.

Some tables, see SUPPORTED_DAOS, require static data unique to any
install.  This module contains entry points for populating static
database tables from configuration files and dumping database values
to configuration files for later import.

"""
import argparse
import yaml

import pheme.longitudinal.tables as tables
from pheme.longitudinal.tables import AdmissionSource, Disposition
from pheme.longitudinal.tables import Facility, ReportableRegion
from pheme.util.pg_access import db_connection


# The list of DAO object types available for export and import.
SUPPORTED_DAOS = ['AdmissionSource', 'Disposition', 'Facility',
                  'ReportableRegion']


def obj_repr(dumper, obj):
    """Generates YAML compliant representation of a DAO object"""
    return dumper.represent_scalar(u'!DAO', obj.__repr__())


def obj_loader(loader, obj):
    """Constructs the DAO object from the YAML representation"""
    # Attempt to safeguard a bit from the danger of eval.  We
    # should only be constructing an instance of the supported
    supported = [getattr(tables, cls) for cls in SUPPORTED_DAOS]
    result = eval(loader.construct_scalar(obj))
    if not isinstance(result, tuple(supported)):  # pragma: no cover
        raise RuntimeError("Unsupported import - is eval being abused???")
    return result


def dump():
    """Entry point to dump static data to a config file

    View usage by calling with -h | --help.  See project's setup.py
    entry_points for full name.

    """
    parser = argparse.ArgumentParser()
    parser.add_argument("DEST", help='file to write to',
                        type=argparse.FileType('w'))
    args = parser.parse_args()

    connection = db_connection('longitudinal')
    objects = []
    for type in SUPPORTED_DAOS:
        yaml.add_representer(getattr(tables, type), obj_repr)
        objects.extend(connection.session.query(
            getattr(tables, type)).all())
    args.DEST.write(yaml.dump(objects, default_flow_style=False))
    connection.disconnect()


def load_file(data_file):
    yaml.add_constructor(u'!DAO', obj_loader)
    objects = yaml.load(data_file.read())
    # Foreign key constraints require we commit the Facilities
    # before the ReportableRegions
    facilities = [obj for obj in objects if isinstance(obj, Facility)]
    the_rest = [obj for obj in objects if not isinstance(obj, Facility)]

    connection = db_connection('longitudinal')
    connection.session.add_all(facilities)
    connection.session.commit()
    connection.session.add_all(the_rest)
    connection.session.commit()
    connection.disconnect()


def load():
    """Entry point to load static data from config file

    View usage by calling with -h | --help.  See project's setup.py
    entry_points for full name.

    """
    parser = argparse.ArgumentParser()
    parser.add_argument("SOURCE", help='source file to import',
                        type=argparse.FileType('r'))
    args = parser.parse_args()
    return load_file(args.SOURCE)
