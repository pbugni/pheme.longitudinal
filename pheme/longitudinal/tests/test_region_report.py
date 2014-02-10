import os
import unittest

from pheme.util.config import Config, configure_logging
from pheme.util.pg_access import db_connection, db_params
from pheme.longitudinal.generate_daily_essence_report import GenerateReport
from pheme.longitudinal.generate_daily_essence_report import ReportCriteria
from pheme.longitudinal.static_data import load_file
from pheme.longitudinal.tables import Visit
from pheme.longitudinal.tables import create_tables


CONFIG_SECTION = 'longitudinal'


def setup_module():
    """Create a fresh db (once) for all tests in this module"""
    configure_logging(verbosity=2, logfile='unittest.log')
    c = Config()
    if c.get('general', 'in_production'):  # pragma: no cover
        raise RuntimeError("DO NOT run destructive test on production system")
    create_tables(enable_delete=True, **db_params(CONFIG_SECTION))

    # Load in all the static data in anonymized form
    static_data_file = open(os.path.join(os.path.dirname(
        os.path.abspath(__file__)), 'anon_static_db_data.yaml'), 'r')
    load_file(static_data_file)


class GenerateNoRegionReportTest(unittest.TestCase):
    """No region now excludes config IGNORE_SITE

    """
    def setUp(self):
        self.connection = db_connection(CONFIG_SECTION)
        self.session = self.connection.session
        params = db_params(CONFIG_SECTION)
        self.report_criteria = ReportCriteria()
        self.report_criteria.database = params['database']
        self.report_criteria.start_date =\
            self.report_criteria.end_date =\
                                           "2009-01-01"
        self.report_criteria.include_updates = True
        self.report = GenerateReport(user=params['user'],
                                     password=params['password'],
                                     report_criteria=self.report_criteria)
        self.report.IGNORE_SITE = 'Ekek'
        self.output = self.report.output_filename

    def tearDown(self):
        self.connection.disconnect()

        self.report.tearDown()
        os.remove(self.output)

    def testRegionX(self):
        # Add two visits to the database, one from region X
        # should then only see one in the report
        facility_in_region = 1297242868
        facility_not_in_region = 1873417805  # region X

        visit_in = Visit(pk=1,
                         visit_id=u'45',
                         patient_id=u'patient id',
                         dim_facility_pk=facility_in_region,
                         zip=u'zip',
                         admit_datetime=self.report_criteria.start_date,
                         gender=u'F',
                         dob=u'200101',
                         chief_complaint=u'Loves Testing',
                         patient_class=u'1',
                         disposition='01',
                         first_message='2010-10-10',
                         last_message='2010-10-10',)

        visit_out = Visit(pk=2,
                          visit_id=u'46',
                          patient_id=u'bad patient id',
                          dim_facility_pk=facility_not_in_region,
                          zip=u'222',
                          admit_datetime=self.report_criteria.start_date,
                          gender=u'F',
                          dob=u'199010',
                          chief_complaint=u'Hates Testing',
                          patient_class=u'1',
                          disposition='01',
                          first_message='2010-10-10',
                          last_message='2010-10-10',)

        self.session.add_all((visit_in, visit_out))
        self.session.commit()

        self.report.execute()
        results = open(self.output, 'r')
        lines = results.readlines()
        self.assertEqual(len(lines), 2)  # header + one match
        self.assertTrue('45' in lines[1])


if '__main__' == __name__:
    unittest.main()
