import datetime
import unittest
from decimal import Decimal

from pheme.longitudinal.tables import create_tables
from pheme.longitudinal.tables import AdmissionSource, AssignedLocation
from pheme.longitudinal.tables import AdmissionTemp, AdmissionO2sat
from pheme.longitudinal.tables import ChiefComplaint, FluVaccine, H1N1Vaccine
from pheme.longitudinal.tables import Disposition, Diagnosis, Location
from pheme.longitudinal.tables import Note, PerformingLab, SpecimenSource
from pheme.longitudinal.tables import Facility, Pregnancy, Race, ServiceArea
from pheme.longitudinal.tables import LabResult, Visit
from pheme.util.config import Config, configure_logging
from pheme.util.pg_access import AlchemyAccess, db_params

CONFIG_SECTION = 'longitudinal'


def setup_module():
    """Create a fresh db (once) for all tests in this module"""
    configure_logging(verbosity=2, logfile='unittest.log')
    c = Config()
    if c.get('general', 'in_production'):  # pragma: no cover
        raise RuntimeError("DO NOT run destructive test on production system")

    create_tables(enable_delete=True, **db_params(CONFIG_SECTION))


class TestLongitudinalAccess(unittest.TestCase):
    """Series of tests on longitudinal ORM classes. """
    def setUp(self):
        c = Config()
        cfg_value = lambda v: c.get('longitudinal', v)
        self.alchemy = AlchemyAccess(database=cfg_value('database'),
                                     host='localhost',
                                     user=cfg_value('database_user'),
                                     password=cfg_value('database_password'))
        self.session = self.alchemy.session
        self.remove_after_test = []

    def tearDown(self):
        map(self.session.delete, self.remove_after_test)
        self.session.commit()
        self.alchemy.disconnect()

    def commit_test_obj(self, obj):
        """Commit to db and bookkeep for safe removal on teardown"""
        self.session.add(obj)
        self.remove_after_test.append(obj)
        self.session.commit()

    def testAdmissionSource(self):
        self.commit_test_obj(AdmissionSource(pk='7',
                                             description='Emergency room'))
        query = self.session.query(AdmissionSource).\
            filter_by(description='Emergency room')
        self.assertEquals(1, query.count())
        self.assertEquals(query.first().pk, '7')

    def testAdmissionTemp(self):
        self.commit_test_obj(AdmissionTemp(degree_fahrenheit=98.5))
        query = self.session.query(AdmissionTemp)
        self.assertEquals(1, query.count())
        self.assertEquals(query.first().degree_fahrenheit,
                          Decimal('98.5'))

    def testAdmissionO2sat(self):
        self.commit_test_obj(AdmissionO2sat(o2sat_percentage=98))
        query = self.session.query(AdmissionO2sat)
        self.assertEquals(1, query.count())
        self.assertEquals(query.first().o2sat_percentage, 98)

    def testAssignedLocation(self):
        self.commit_test_obj(AssignedLocation(location='PMCLAB'))
        query = self.session.query(AssignedLocation)
        self.assertEquals(1, query.count())
        self.assertEquals(query.first().location, 'PMCLAB')

    def testChiefComplaint(self):
        self.commit_test_obj(ChiefComplaint(chief_complaint='ABDOMINAL PAIN'))
        query = self.session.query(ChiefComplaint)
        self.assertEquals(1, query.count())
        self.assertEquals(query.first().chief_complaint,
                          'ABDOMINAL PAIN')

    def testLabResult(self):
        loinc_text = 'Bacteria identified:Prid:Pt:Sputum:Nom:Aerobic culture'
        loinc_code = '622-1'
        coding = 'LN'
        result = """Few Neutrophils   Few Squamous Epithelial Cells   Mixed Flora   Squamous cells in the specimen   indicate the presence of   superficial material that may   contain contaminating or   colonizing bacteria unrelated to   infection. Collection of another   specimen is suggested, avoiding   superficial sources of   contamination.   *****CULTURE RESULTS*****"""

        self.commit_test_obj(LabResult(test_code=loinc_code,
                                       test_text=loinc_text,
                                       coding=coding,
                                       result=result))
        query = self.session.query(LabResult)
        self.assertEquals(1, query.count())
        self.assertEquals(query.first().test_code, loinc_code)
        self.assertEquals(query.first().test_text, loinc_text)
        self.assertEquals(query.first().result, result)

    def testLocationCountry(self):
        self.commit_test_obj(Location(country='CAN'))
        query = self.session.query(Location)
        self.assertEquals(1, query.count())
        self.assertEquals(query.first().country, 'CAN')
        self.assertTrue(datetime.datetime.now() >= query.first().last_updated)

    def testLocationCounty(self):
        self.commit_test_obj(Location(county='SPO-WA'))
        query = self.session.query(Location)
        self.assertEquals(1, query.count())
        self.assertEquals(query.first().county, 'SPO-WA')
        self.assertTrue(datetime.datetime.now() >= query.first().last_updated)

    def testLocationZip(self):
        self.commit_test_obj(Location(zip="98101"))
        query = self.session.query(Location)
        self.assertEquals(1, query.count())
        self.assertEquals(query.first().zip, '98101')

    def testLocation(self):
        self.commit_test_obj(Location(county='SPO-WA', state='WA',
                                      country='USA', zip='95432'))
        query = self.session.query(Location)
        self.assertEquals(1, query.count())
        self.assertEquals(query.first().state, 'WA')
        self.assertEquals(query.first().county, 'SPO-WA')
        self.assertEquals(query.first().country, 'USA')
        self.assertEquals(query.first().zip, '95432')

    def testNote(self):
        self.commit_test_obj(Note(note="IS PT ALLERGIC TO PENICILLIN? N"))
        query = self.session.query(Note)
        self.assertEquals(1, query.count())
        self.assertEquals(query.first().note,
                          "IS PT ALLERGIC TO PENICILLIN? N")

    def testLongNote(self):
        too_long_note = """ REFERENCE INTERVAL: INFLUENZA B VIRUS Ab, IgG 0.89 IV or less:    Negative - No significant level of influenza B virus IgG antibody detected. 0.90 - 1.10 IV:     Equivocal - Questionable presence of influenza B virus IgG antibody detected. Repeat testing in 10-14 days may be helpful. 1.11 IV or greater: Positive - IgG antibodies to influenza B virus detected, which may suggest current or past infection. Test performed at ARUP Laboratories, 500 Chipeta Way, Salt Lake City, Utah 84108 Performed at ARUP, 500 Chipeta Way, Salt Lake City, UT 84108"""
        self.commit_test_obj(Note(note=too_long_note))
        query = self.session.query(Note)
        self.assertEquals(1, query.count())
        self.assertTrue(query.first().note.startswith(too_long_note[:100]))

    def testDisposition(self):
        self.commit_test_obj(Disposition(code=20, description='Expired',
                                         gipse_mapping='Expired',
                                         odin_mapping='Died'))
        disposition = self.session.query(Disposition).\
            filter(Disposition.description == 'Expired').one()
        self.assertTrue(disposition)
        self.assertEquals(disposition.code, 20)
        self.assertEquals(disposition.odin_mapping, 'Died')
        self.assertEquals(disposition.gipse_mapping, 'Expired')
        self.assertTrue(datetime.datetime.now() > disposition.last_updated)

    def testDx(self):
        self.commit_test_obj(Diagnosis(status='W', icd9='569.3',
                                       description='HYPERTENSION NOS'))
        query = self.session.query(Diagnosis)
        self.assertEquals(1, query.count())
        self.assertEquals(query.first().description, 'HYPERTENSION NOS')

    def testFacility(self):
        self.commit_test_obj(Facility(county='NEAR', npi=123454321,
                                      zip='99999',
                                      organization_name='Nearby Medical '
                                      'Center', local_code='NMC'))
        sh = self.session.query(Facility).\
            filter_by(npi=123454321).one()
        self.assertEquals(sh.organization_name,
                          'Nearby Medical Center')
        self.assertEquals(sh.zip, '99999')
        self.assertEquals(sh.county, 'NEAR')

    def testFacilityUpdates(self):
        "Facilities are pre-loaded.  Use to test update timestamps"
        self.commit_test_obj(Facility(county='NEAR', npi=123454321,
                                      zip='99999',
                                      organization_name='Nearby Medical '
                                      'Center', local_code='NMC'))
        facility = self.session.query(Facility).\
            filter_by(npi=123454321).one()
        b4 = facility.last_updated
        self.assertTrue(b4)
        facility.local_code = 'FOO'
        self.session.commit()
        facility = self.session.query(Facility).\
            filter_by(npi=123454321).one()
        after = facility.last_updated
        self.assertTrue(after > b4)

    def testPerformingLab(self):
        self.commit_test_obj(PerformingLab(local_code='HFH'))
        query = self.session.query(PerformingLab)
        self.assertEquals(1, query.count())
        self.assertEquals(query.first().local_code,
                          'HFH')

    def testPrego(self):
        self.commit_test_obj(Pregnancy(result='Patient Currently Pregnant'))
        query = self.session.query(Pregnancy)
        self.assertEquals(1, query.count())
        self.assertEquals(query.first().result,
                          'Patient Currently Pregnant')

    def testRace(self):
        self.commit_test_obj(Race(race='Native Hawaiian or Other '
                                  'Pacific Islander'))
        query = self.session.query(Race)
        self.assertEquals(1, query.count())
        self.assertEquals(query.first().race,
                          'Native Hawaiian or Other Pacific Islander')

    def testServiceArea(self):
        self.commit_test_obj(ServiceArea(area='obstetrics'))
        query = self.session.query(ServiceArea)
        self.assertEquals(1, query.count())
        self.assertEquals(query.first().area,
                          'obstetrics')

    def testSpecimenSource(self):
        self.commit_test_obj(SpecimenSource(source='PLEFLD'))
        query = self.session.query(SpecimenSource)
        self.assertEquals(1, query.count())
        self.assertEquals(query.first().source, 'PLEFLD')

    def testFluVaccine(self):
        self.commit_test_obj(FluVaccine(status='Not Specified'))
        query = self.session.query(FluVaccine)
        self.assertEquals(1, query.count())
        self.assertEquals(query.first().status, 'Not Specified')

    def testH1N1Vaccine(self):
        self.commit_test_obj(H1N1Vaccine(status='Not Applicable (Age&lt;18)'))
        query = self.session.query(H1N1Vaccine)
        self.assertEquals(1, query.count())
        self.assertEquals(query.first().status, 'Not Applicable (Age&lt;18)')

    def testVisit(self):
        "Test with minimal required fields set"
        self.commit_test_obj(Facility(county='NEAR', npi=123454321,
                                      zip='99999',
                                      organization_name='Nearby Medical '
                                      'Center', local_code='NMC'))
        kw = {
            'visit_id': '284999^^^&650903.98473.0179.6039.1.333.1&ISO',
            'patient_class': 'E',
            'patient_id': '156999^^^&650903.98473.0179.6039.1.333.1&ISO',
            'admit_datetime': datetime.datetime(2007, 01, 01),
            'first_message': datetime.datetime(2007, 01, 01),
            'last_message': datetime.datetime(2007, 01, 01),
            'dim_facility_pk': 123454321}
        self.commit_test_obj(Visit(**kw))
        query = self.session.query(Visit)
        self.assertEquals(1, query.count())
        self.assertEquals(query.first().ever_in_icu, False)

if '__main__' == __name__:  # pragma: no cover
    unittest.main()
