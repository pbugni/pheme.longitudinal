#!/usr/bin/env python
""" Test region reports w/o loading any static data in db.
"""

import os
import unittest

from pheme.util.config import Config, configure_logging
from pheme.util.pg_access import db_connection, db_params
from pheme.longitudinal.generate_daily_essence_report import GenerateReport
from pheme.longitudinal.generate_daily_essence_report import ReportCriteria
from pheme.longitudinal.tables import Facility, ReportableRegion, Visit
from pheme.longitudinal.tables import create_tables

CONFIG_SECTION = 'longitudinal'


def setup_module():
    """Create a fresh db (once) for all tests in this module"""
    configure_logging(verbosity=2, logfile='unittest.log')
    c = Config()
    if c.get('general', 'in_production'):  # pragma: no cover
        raise RuntimeError("DO NOT run destructive test on production system")
    create_tables(enable_delete=True, **db_params(CONFIG_SECTION))

    # create a "test_region" and a couple bogus facilities
    f1 = Facility(county='KING', npi=10987, zip='12345',
                  organization_name='Reason Medical Center',
                  local_code='RMC')
    f2 = Facility(county='POND', npi=65432, zip='67890',
                  organization_name='No-Reason Medical Center',
                  local_code='NMC')
    conn = db_connection(CONFIG_SECTION)
    conn.session.add(f1)
    conn.session.add(f2)
    conn.session.commit()
    rr1 = ReportableRegion(region_name='test_region',
                           dim_facility_pk=10987)
    conn.session.add(rr1)
    conn.session.commit()
    conn.disconnect()


class GenerateRegionReportTest(unittest.TestCase):
    """One of the reporting options is to limit the sites included to
    values matching in the reportable_region table.

    """
    def setUp(self):
        self.conn = db_connection(CONFIG_SECTION)
        params = db_params(CONFIG_SECTION)
        self.report_criteria = ReportCriteria()
        self.report_criteria.database = params['database']
        self.report_criteria.start_date =\
            self.report_criteria.end_date = "2009-01-01"
        # setting reportable_region requires db validation
        self.report_criteria.credentials(user=params['user'],
                                         password=params['password'])
        self.report_criteria.reportable_region = 'test_region'

        self.report = GenerateReport(user=params['user'],
                                     password=params['password'],
                                     report_criteria=self.report_criteria)
        self.output = self.report.output_filename

    def tearDown(self):
        self.conn.disconnect()
        self.report.tearDown()
        os.remove(self.report.output_filename)

    def testInvalidRegion(self):
        def assign_bad_region(value):
            self.report_criteria.reportable_region = value
        self.assertRaises(AttributeError, assign_bad_region,
                          'bogus')

    def testReportName(self):
        self.assertTrue('test_region' in self.output)

    def testEmptyReport(self):
        # without adding any visits to the database, we should
        # still be able to run, and simply get the header back
        self.report.execute()
        results = open(self.output, 'r')
        lines = results.readlines()
        self.assertEqual(len(lines), 1)
        self.assertEqual(self.report._header(), lines[0].rstrip())

    def testRegionOnly(self):
        # Add two visits to the database, only one from the facilities
        # in the 'test_region' region - should then only see one in the
        # report
        facility_in_region = 10987
        facility_not_in_region = 65432

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
                          patient_id=u'outta region id',
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

        self.conn.session.add_all((visit_in, visit_out))
        self.conn.session.commit()

        self.report.execute()
        results = open(self.output, 'r')
        lines = results.readlines()
        self.assertEqual(len(lines), 2)
        self.assertTrue('45' in lines[1])
