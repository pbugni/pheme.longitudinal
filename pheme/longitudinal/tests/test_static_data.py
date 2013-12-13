from tempfile import NamedTemporaryFile
import os
import sys
import unittest

import pheme.longitudinal.tables as tables
from pheme.longitudinal.tables import AdmissionSource, Disposition
from pheme.longitudinal.tables import Facility, ReportableRegion
from pheme.longitudinal.tables import create_tables
from pheme.longitudinal.static_data import SUPPORTED_DAOS
from pheme.longitudinal.static_data import dump, load
from pheme.util.config import Config, configure_logging
from pheme.util.pg_access import db_connection, db_params

CONFIG_SECTION = 'longitudinal'


def setup_module():
    """Create a fresh db (once) for all tests in this module"""
    configure_logging(verbosity=2, logfile='unittest.log')
    c = Config()
    if c.get('general', 'in_production'):  # pragma: no cover
        raise RuntimeError("DO NOT run destructive test on production system")

    create_tables(enable_delete=True, **db_params(CONFIG_SECTION))


class TestStaticData(unittest.TestCase):
    def setUp(self):
        self.conn = db_connection('longitudinal')
        self.argv_restore = sys.argv
        self.file_to_purge = None

    def tearDown(self):
        """Clean up, by removing all supported objs tests introduced"""
        for type in SUPPORTED_DAOS:
            self.conn.session.query(getattr(tables, type)).delete()
            self.conn.session.commit()
        self.conn.disconnect()
        sys.argv = self.argv_restore
        if self.file_to_purge:
            os.remove(self.file_to_purge)

    def test_export(self):
        # Add one of each type to be exported
        f1 = Facility(county='FLEE', npi=10987, zip='12345',
                      organization_name='Reason Medical Center',
                      local_code='RMC')
        self.conn.session.add(f1)
        self.conn.session.commit()

        as1 = AdmissionSource(pk='2', description='Clinic referral')
        d1 = Disposition(code=1, description='Left',
                         gipse_mapping='Discharge',
                         odin_mapping='Not Admitted')
        rr1 = ReportableRegion(region_name='one',
                               dim_facility_pk=10987)
        self.conn.session.add_all((as1, d1, rr1))
        self.conn.session.commit()

        with NamedTemporaryFile(delete=False) as tmp:
            self.file_to_purge = tmp.name
        sys.argv = ['/typically/path/to/script', self.file_to_purge]
        dump()

        with open(self.file_to_purge, 'r') as result:
            data = result.read()

        assert data.count('!DAO') == 4
        for type in SUPPORTED_DAOS:
            assert data.count(type) == 1

    def test_import(self):
        input = """- !DAO 'AdmissionSource(pk=''2'', description=''Clinic referral'')'
- !DAO 'Disposition(code=1, description=''Left'', gipse_mapping=''Discharge'', odin_mapping=''Not
  Admitted'')'
- !DAO 'Facility(county=''FLEE'', npi=10987, zip=''12345'', organization_name=''Reason
  Medical Center'', local_code=''RMC'')'
- !DAO 'ReportableRegion(region_name=''one'', dim_facility_pk=10987)'"""
        with NamedTemporaryFile(delete=False) as tmp:
            self.file_to_purge = tmp.name
            tmp.write(input)

        sys.argv = ['/typically/path/to/script', self.file_to_purge]
        load()

        # Database should now have those objects
        as2 = self.conn.session.query(AdmissionSource).one()
        assert as2.description == 'Clinic referral'
        d2 = self.conn.session.query(Disposition).one()
        assert d2.description == 'Left'
        f2 = self.conn.session.query(Facility).one()
        assert f2.organization_name == 'Reason Medical Center'
        rr2 = self.conn.session.query(ReportableRegion).one()
        assert rr2.region_name == 'one'
