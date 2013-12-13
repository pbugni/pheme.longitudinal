"""Table definitions and SQL ORM classes for the star schema -
see README.txt in same directory.

"""
import datetime
import getpass
import sys

from sqlalchemy import create_engine, text
from sqlalchemy import CHAR, VARCHAR, SMALLINT, NUMERIC, TEXT
from sqlalchemy import Boolean, DateTime, Integer
from sqlalchemy import Table, Column, ForeignKey, UniqueConstraint
from sqlalchemy import MetaData
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import mapper, relationship

from pheme.util.config import Config


class OrmObject(object):
    """Base class for all ORM tables

    Provides consistant string representation and an initilization
    method that loads fields by use of named arguments.

    Lifted directly from:
    http://www.sqlalchemy.org/trac/wiki/UsageRecipes/GenericOrmBaseClass

    and then patched to make work.

    """
    def __init__(self, **kw):
        """base initialization method

        Blindly sets all attributes passed as named parameters.  The
        check for the named attribute being in existance for the ORM
        didn't work.

        """
        for key in kw:
            if not key.startswith('_'):  # and key in self.__dict__:
                setattr(self, key, kw[key])

    def __repr__(self):
        """Generate string representation of the object

        NB the `last_updated` attribute is being intentionally repressed
        to prevent it from showing up and resulting in new objects
        retaining old timestamps using serialization methods.  See
        static_data.py for more info.
        """
        attrs = []
        for key in self.__dict__:
            if not key.startswith('_') and key != 'last_updated':
                attrs.append((key, getattr(self, key)))
        return "%s(%s)" % (self.__class__.__name__,
                           ', '.join(x[0] + '=' +
                                     repr(x[1]) for x in attrs))

metadata = MetaData()

dim_admission_source = Table(
    'dim_admission_source', metadata,
    Column('pk', CHAR(1), nullable=False, primary_key=True,
           autoincrement=False),
    Column('description', VARCHAR(100), nullable=False),
    Column('last_updated', DateTime, default=datetime.datetime.now(),
           onupdate=datetime.datetime.now(), index=True),)


class AdmissionSource(OrmObject):
    "List of fields used to query a unique instance"
    query_fields = ('pk',)


mapper(AdmissionSource, dim_admission_source)

dim_admission_temp = Table(
    'dim_admission_temp', metadata,
    Column('pk', Integer, nullable=False, primary_key=True),
    Column('degree_fahrenheit', NUMERIC, nullable=False),
    Column('last_updated', DateTime, default=datetime.datetime.now(),
           onupdate=datetime.datetime.now(), index=True))


class AdmissionTemp(OrmObject):
    "List of fields used to query a unique instance"
    query_fields = ('degree_fahrenheit',)


mapper(AdmissionTemp, dim_admission_temp)

dim_admission_o2sat = Table(
    'dim_admission_o2sat', metadata,
    Column('pk', Integer, nullable=False, primary_key=True),
    Column('o2sat_percentage', SMALLINT, nullable=False),
    Column('last_updated', DateTime, default=datetime.datetime.now(),
           onupdate=datetime.datetime.now(), index=True))


class AdmissionO2sat(OrmObject):
    "List of fields used to query a unique instance"
    query_fields = ('o2sat_percentage',)


mapper(AdmissionO2sat, dim_admission_o2sat)

dim_assigned_location = Table(
    'dim_assigned_location', metadata,
    Column('pk', Integer, nullable=False, primary_key=True),
    Column('location', VARCHAR(16), nullable=False),
    Column('last_updated', DateTime, default=datetime.datetime.now(),
           onupdate=datetime.datetime.now(), index=True),)


class AssignedLocation(OrmObject):
    "List of fields used to query a unique instance"
    query_fields = ('location',)


mapper(AssignedLocation, dim_assigned_location)

dim_ar = Table(
    'dim_ar', metadata,
    Column('pk', Integer, nullable=False,
           primary_key=True),
    Column('admit_reason', VARCHAR(80), index=True, nullable=False),
    Column('last_updated', DateTime, default=datetime.datetime.now(),
           onupdate=datetime.datetime.now(), index=True))


class AdmitReason(OrmObject):
    "List of fields used to query a unique instance"
    query_fields = ('admit_reason',)


mapper(AdmitReason, dim_ar)

dim_cc = Table(
    'dim_cc', metadata,
    Column('pk', Integer, nullable=False,
           primary_key=True),
    Column('chief_complaint', VARCHAR(80), index=True, nullable=False),
    Column('last_updated', DateTime, default=datetime.datetime.now(),
           onupdate=datetime.datetime.now(), index=True))


class ChiefComplaint(OrmObject):
    "List of fields used to query a unique instance"
    query_fields = ('chief_complaint',)


mapper(ChiefComplaint, dim_cc)

dim_location = Table(
    'dim_location', metadata,
    Column('pk', Integer, nullable=False,
           primary_key=True),
    Column('country', CHAR(3), nullable=True),
    Column('county', VARCHAR(8), index=True, nullable=True),
    Column('state', CHAR(2), index=True, nullable=True),
    Column('zip', VARCHAR(10), nullable=True),
    Column('last_updated', DateTime, default=datetime.datetime.now(),
           onupdate=datetime.datetime.now(), index=True),
    UniqueConstraint('county', 'state', 'zip',
                     name='unique_location'))


class Location(OrmObject):
    "List of fields used to query a unique instance"
    query_fields = ('country', 'county', 'state', 'zip')


mapper(Location, dim_location)

dim_disposition = Table(
    'dim_disposition', metadata,
    Column('code', SMALLINT, nullable=False, primary_key=True,
           autoincrement=False),
    Column('gipse_mapping', VARCHAR(16), nullable=False),
    Column('odin_mapping', VARCHAR(16), nullable=False),
    Column('description', VARCHAR(150), nullable=False),
    Column('last_updated', DateTime, default=datetime.datetime.now(),
           onupdate=datetime.datetime.now(), index=True))


class Disposition(OrmObject):
    "List of fields used to query a unique instance"
    query_fields = ('code',)


mapper(Disposition, dim_disposition)

dim_facility = Table(
    'dim_facility', metadata,
    Column('npi', Integer, nullable=False, autoincrement=False,
           primary_key=True),
    Column('local_code', CHAR(3), nullable=False),
    Column('organization_name', VARCHAR(80), nullable=False),
    Column('zip', VARCHAR(10), nullable=False),
    Column('county', VARCHAR(16), index=True, nullable=False),
    Column('last_updated', DateTime, default=datetime.datetime.now(),
           onupdate=datetime.datetime.now(), index=True))


class Facility(OrmObject):
    pass


mapper(Facility, dim_facility)

dim_lab_flag = Table(
    'dim_lab_flag', metadata,
    Column('pk', Integer, nullable=False, primary_key=True),
    Column('code', VARCHAR(20), nullable=False, index=True),
    Column('code_text', VARCHAR(80), nullable=True, index=True),
    Column('coding', VARCHAR(16), index=True, nullable=False),
    Column('last_updated', DateTime, default=datetime.datetime.now(),
           onupdate=datetime.datetime.now(), index=True),
    UniqueConstraint('code', 'coding', name='unique_lab_flag'))


class LabFlag(OrmObject):
    "List of fields used to query a unique instance"
    query_fields = ('code', 'coding')


mapper(LabFlag, dim_lab_flag)

MAX_RESULT_LEN = 500
dim_lab_result = Table(
    'dim_lab_result', metadata,
    Column('pk', Integer, nullable=False, primary_key=True),
    Column('coding', VARCHAR(32), index=True, nullable=True),
    Column('test_code', VARCHAR(32), index=True, nullable=False),
    Column('test_text', VARCHAR(120), index=True, nullable=True),
    Column('result', VARCHAR(MAX_RESULT_LEN), index=True, nullable=True),
    Column('result_unit', VARCHAR(50), index=True, nullable=True),
    Column('last_updated', DateTime, default=datetime.datetime.now(),
           onupdate=datetime.datetime.now(), index=True),
    UniqueConstraint('test_code', 'test_text', 'coding', 'result',
                     'result_unit', name='unique_lab'))


class LabResult(OrmObject):
    "List of fields used to query a unique instance"
    query_fields = ('test_code', 'test_text', 'coding', 'result',
                    'result_unit')

    def __init__(self, **kw):
        """Specialize the init method to truncate the result field to
        MAX_RESULT_LEN

        """
        if 'result' in kw and kw['result']:
            kw['result'] = kw['result'][:MAX_RESULT_LEN]
        super(LabResult, self).__init__(**kw)


mapper(LabResult, dim_lab_result)

dim_order_number = Table(
    'dim_order_number', metadata,
    Column('pk', Integer, nullable=False, primary_key=True),
    Column('filler_order_no', VARCHAR(80), index=True,
           nullable=False),
    Column('last_updated', DateTime, default=datetime.datetime.now(),
           onupdate=datetime.datetime.now(), index=True))


class OrderNumber(OrmObject):
    """Filler Order Number, directly from OBR-3"""

    "List of fields used to query a unique instance"
    query_fields = ('filler_order_no',)


mapper(OrderNumber, dim_order_number)

MAX_NOTE_LEN = 500
dim_note = Table(
    'dim_note', metadata,
    Column('pk', Integer, nullable=False, primary_key=True),
    Column('note', VARCHAR(MAX_NOTE_LEN), index=True, nullable=False),
    Column('last_updated', DateTime, default=datetime.datetime.now(),
           onupdate=datetime.datetime.now(), index=True))


class Note(OrmObject):
    """Note, captures the lab result notes field (NTE-3)

    """

    "List of fields used to query a unique instance"
    query_fields = ('note',)

    def __init__(self, **kw):
        """Specialize the init method to truncate the note field to
        MAX_NOTE_LEN

        """
        if 'note' in kw:
            kw['note'] = kw['note'][:MAX_NOTE_LEN]
        super(Note, self).__init__(**kw)


mapper(Note, dim_note)

"""Performing Lab, captures essentail bits of OBX-15
local_code: OBX-15.4

"""
dim_performing_lab = Table(
    'dim_performing_lab', metadata,
    Column('pk', Integer, nullable=False, primary_key=True),
    Column('local_code', VARCHAR(20), index=True, nullable=True,
           unique=True),
    Column('last_updated', DateTime, default=datetime.datetime.now(),
           onupdate=datetime.datetime.now(), index=True))


class PerformingLab(OrmObject):
    "List of fields used to query a unique instance"
    query_fields = ('local_code',)


mapper(PerformingLab, dim_performing_lab)


dim_pregnancy = Table(
    'dim_pregnancy', metadata,
    Column('pk', Integer, nullable=False, primary_key=True),
    Column('result', VARCHAR(30), index=True, nullable=False),
    Column('last_updated', DateTime, default=datetime.datetime.now(),
           onupdate=datetime.datetime.now(), index=True))


class Pregnancy(OrmObject):
    "List of fields used to query a unique instance"
    query_fields = ('result',)


mapper(Pregnancy, dim_pregnancy)

dim_race = Table(
    'dim_race', metadata,
    Column('pk', Integer, nullable=False, primary_key=True),
    Column('race', VARCHAR(60), nullable=False),
    Column('last_updated', DateTime, default=datetime.datetime.now(),
           onupdate=datetime.datetime.now(), index=True))


class Race(OrmObject):
    "List of fields used to query a unique instance"
    query_fields = ('race',)


mapper(Race, dim_race)

dim_ref_range = Table(
    'dim_ref_range', metadata,
    Column('pk', Integer, nullable=False, primary_key=True),
    Column('range', VARCHAR(16), nullable=False, unique=True,
           index=True),
    Column('last_updated', DateTime, default=datetime.datetime.now(),
           onupdate=datetime.datetime.now(), index=True))


class ReferenceRange(OrmObject):
    """Reference Range"""

    "List of fields used to query a unique instance"
    query_fields = ('range',)


mapper(ReferenceRange, dim_ref_range)

dim_service_area = Table(
    'dim_service_area', metadata,
    Column('pk', Integer, nullable=False, primary_key=True),
    Column('area', VARCHAR(60), nullable=False, unique=True),
    Column('last_updated', DateTime, default=datetime.datetime.now(),
           onupdate=datetime.datetime.now(), index=True))


class ServiceArea(OrmObject):
    "List of fields used to query a unique instance"
    query_fields = ('area',)


mapper(ServiceArea, dim_service_area)

"""Specimen Source, captures essentail bits of SPM-4 or alternatively
when SPM-4 is not available, OBX-15

source: SPM-4.4 or OBX-15.1.4

"""
dim_specimen_source = Table(
    'dim_specimen_source', metadata,
    Column('pk', Integer, nullable=False, primary_key=True),
    Column('source', VARCHAR(20), index=True, nullable=False),
    Column('last_updated', DateTime, default=datetime.datetime.now(),
           onupdate=datetime.datetime.now(), index=True))


class SpecimenSource(OrmObject):
    "List of fields used to query a unique instance"
    query_fields = ('source',)


mapper(SpecimenSource, dim_specimen_source)


dim_flu_vaccine = Table(
    'dim_flu_vaccine', metadata,
    Column('pk', Integer, nullable=False, primary_key=True),
    Column('status', VARCHAR(30), nullable=False),
    Column('last_updated', DateTime, default=datetime.datetime.now(),
           onupdate=datetime.datetime.now(), index=True))


class FluVaccine(OrmObject):
    "List of fields used to query a unique instance"
    query_fields = ('status',)


mapper(FluVaccine, dim_flu_vaccine)

dim_h1n1_vaccine = Table(
    'dim_h1n1_vaccine', metadata,
    Column('pk', Integer, nullable=False, primary_key=True),
    Column('status', VARCHAR(30), nullable=False),
    Column('last_updated', DateTime, default=datetime.datetime.now(),
           onupdate=datetime.datetime.now(), index=True))


class H1N1Vaccine(OrmObject):
    "List of fields used to query a unique instance"
    query_fields = ('status',)


mapper(H1N1Vaccine, dim_h1n1_vaccine)

dim_dx = Table(
    'dim_dx', metadata,
    Column('pk', Integer, nullable=False, primary_key=True),
    Column('icd9', VARCHAR(10), index=True, nullable=False, unique=True),
    Column('description', VARCHAR(80),),
    Column('last_updated', DateTime, default=datetime.datetime.now(),
           onupdate=datetime.datetime.now(), index=True))


class Diagnosis(OrmObject):
    "List of fields used to query a unique instance"
    query_fields = ('icd9',)


mapper(Diagnosis, dim_dx)

fact_visit = Table(
    'fact_visit', metadata,
    Column('pk', Integer, nullable=False,
           primary_key=True),
    Column('visit_id', VARCHAR(60), index=True, nullable=False),
    Column('patient_class', CHAR(1), index=True, nullable=False),
    Column('patient_id', VARCHAR(60), nullable=False),
    Column('admit_datetime', DateTime, index=True, nullable=False),
    Column('first_message', DateTime, default=None, nullable=False),
    Column('last_message', DateTime, default=None, nullable=False),
    Column('discharge_datetime', DateTime, nullable=True),
    Column('age', SMALLINT, default=None, index=True, nullable=True),
    Column('dob', Integer, default=None, index=True, nullable=True),
    Column('gender', CHAR(1), default='U', nullable=False),
    Column('ever_in_icu', Boolean, default=False, index=True,
           nullable=False),
    Column('influenza_test_summary', SMALLINT, default=99, index=True,
           nullable=False),
    Column('dim_ar_pk', ForeignKey('dim_ar.pk', ondelete='CASCADE'),
           index=True, nullable=True),
    Column('dim_cc_pk', ForeignKey('dim_cc.pk', ondelete='CASCADE'),
           index=True, nullable=True),
    Column('dim_disposition_pk',
           ForeignKey('dim_disposition.code', ondelete='CASCADE'),
           index=True, nullable=True),
    Column('dim_facility_pk',
           ForeignKey('dim_facility.npi', ondelete='CASCADE'),
           nullable=False, index=True),
    Column('dim_location_pk',
           ForeignKey('dim_location.pk', ondelete='CASCADE'),
           nullable=True),
    Column('dim_service_area_pk',
           ForeignKey('dim_service_area.pk', ondelete='CASCADE'),
           nullable=True),
    Column('dim_flu_vaccine_pk',
           ForeignKey('dim_flu_vaccine.pk', ondelete='CASCADE'),
           nullable=True),
    Column('dim_h1n1_vaccine_pk',
           ForeignKey('dim_h1n1_vaccine.pk', ondelete='CASCADE'),
           nullable=True),
    Column('dim_admission_temp_pk',
           ForeignKey('dim_admission_temp.pk', ondelete='CASCADE'),
           nullable=True),
    Column('dim_admission_source_pk',
           ForeignKey('dim_admission_source.pk', ondelete='CASCADE'),
           nullable=True),
    Column('dim_admission_o2sat_pk',
           ForeignKey('dim_admission_o2sat.pk', ondelete='CASCADE'),
           nullable=True),
    Column('dim_assigned_location_pk',
           ForeignKey('dim_assigned_location.pk', ondelete='CASCADE'),
           nullable=True),
    Column('dim_pregnancy_pk',
           ForeignKey('dim_pregnancy.pk', ondelete='CASCADE'),
           index=True, nullable=True),
    Column('dim_race_pk',
           ForeignKey('dim_race.pk', ondelete='CASCADE'),
           nullable=True),
    Column('last_updated', DateTime, default=datetime.datetime.now(),
           onupdate=datetime.datetime.now(), index=True),
    UniqueConstraint('visit_id', 'patient_class',
                     name='unique_visit_patient_class'))


class Visit(OrmObject):
    pass


mapper(Visit, fact_visit)

assoc_visit_dx = Table(
    'assoc_visit_dx', metadata,
    Column('fact_visit_pk',
           ForeignKey('fact_visit.pk', ondelete='CASCADE'),
           nullable=False, primary_key=True),
    Column('dim_dx_pk',
           ForeignKey('dim_dx.pk', ondelete='CASCADE'),
           nullable=False, primary_key=True),
    # status == (W)orking, (A)dmitting, (F)inal
    Column('status', CHAR(1), primary_key=True, nullable=False),
    Column('dx_datetime', DateTime, nullable=True, index=True,
           primary_key=True),
    Column('last_updated', DateTime, default=datetime.datetime.now(),
           onupdate=datetime.datetime.now(), index=True),
    Column('rank', SMALLINT, nullable=False, default=0))


class VisitDiagnosisAssociation(OrmObject):
    "List of fields used to query a unique instance"
    query_fields = ('fact_visit_pk', 'dim_dx_pk', 'status')


mapper(VisitDiagnosisAssociation, assoc_visit_dx,
       properties={'visit': relationship(Visit),
                   'dx': relationship(Diagnosis)})

assoc_visit_lab = Table(
    'assoc_visit_lab', metadata,
    Column('fact_visit_pk',
           ForeignKey('fact_visit.pk', ondelete='CASCADE'),
           primary_key=True, nullable=False),
    Column('dim_lab_result_pk',
           ForeignKey('dim_lab_result.pk', ondelete='CASCADE'),
           primary_key=True, nullable=False),
    Column('dim_lab_flag_pk',
           ForeignKey('dim_lab_flag.pk', ondelete='CASCADE'),
           index=True, nullable=True),
    Column('dim_order_number_pk',
           ForeignKey('dim_order_number.pk', ondelete='CASCADE'),
           index=True, nullable=True),
    Column('dim_ref_range_pk',
           ForeignKey('dim_ref_range.pk', ondelete='CASCADE'),
           index=True, nullable=True),
    Column('dim_note_pk',
           ForeignKey('dim_note.pk', ondelete='CASCADE'),
           index=True, nullable=True),
    Column('dim_performing_lab_pk',
           ForeignKey('dim_performing_lab.pk', ondelete='CASCADE'),
           index=True, nullable=True),
    Column('dim_specimen_source_pk',
           ForeignKey('dim_specimen_source.pk', ondelete='CASCADE'),
           index=True, nullable=True),
    # status == (P)reliminary, (A)ctive, (F)inal, (X)??
    Column('status', CHAR(1), primary_key=True, nullable=False),
    Column('report_datetime', DateTime, default=None,
           index=True, nullable=True),
    Column('collection_datetime', DateTime, default=None,
           index=True, nullable=True),
    Column('last_updated', DateTime, default=datetime.datetime.now(),
           onupdate=datetime.datetime.now(), index=True))


class VisitLabAssociation(OrmObject):
    "List of fields used to query a unique instance"
    query_fields = ('fact_visit_pk', 'dim_lab_result_pk', 'status')


mapper(VisitLabAssociation, assoc_visit_lab,
       properties={'visit': relationship(Visit),
                   'lab': relationship(LabResult),
                   'lab_flag': relationship(LabFlag),
                   'performing_lab': relationship(PerformingLab),
                   'specimen_source': relationship(SpecimenSource),
                   'note': relationship(Note),
                   'order_number': relationship(OrderNumber),
                   'reference_range': relationship(ReferenceRange)})

internal_export_delta = Table(
    'internal_export_delta', metadata,
    Column('pk', Integer, nullable=False,
           primary_key=True),
    Column('export_time', DateTime, index=True, nullable=False),
    Column('audience', VARCHAR(60), nullable=True),
    Column('sequence', Integer, nullable=False),
    Column('filename', TEXT, nullable=False))


class ExportDelta(OrmObject):
    """Keeps track of when database exports are run"""
    pass


mapper(ExportDelta, internal_export_delta)

internal_message_processed = Table(
    'internal_message_processed', metadata,
    Column('hl7_msh_id', Integer, primary_key=True, autoincrement=False),
    Column('message_datetime', DateTime, nullable=False, index=True),
    Column('visit_id', VARCHAR(255), nullable=False, index=True),
    Column('processed_datetime', DateTime, default=None),)


class MessageProcessed(OrmObject):
    """Used to maintain processed status of all HL/7 messages

    All messages from the data warehouse and their processed status
    reside in the 'internal_message_processed' table (with the
    exception of latency).  A null value in the `processed_datetime`
    column implies the message has yet to be processed.

    The `hl7_msh_id` value refers to the data warehouse
    hl7_msh.hl7_msh_id column.  As this is spanning multiple
    databases, enfored referential integrity is impossible to do at
    the database level - take care to handle this carefully!

    """
    pass


mapper(MessageProcessed, internal_message_processed)


internal_reportable_region = Table(
    'internal_reportable_region', metadata,
    Column('region_name', VARCHAR(50), primary_key=True),
    Column('dim_facility_pk',
           ForeignKey('dim_facility.npi', ondelete='CASCADE'),
           nullable=False, primary_key=True))


class ReportableRegion(OrmObject):
    """Regions arbitrarily defined by facility

    Data sharing agreements are creating regions for which we can
    report data on, such as the"Spokane Regional Health District".

    The reportable_regions table is designed to be useful when
    generating reports for a particular region.  The expectation is
    that a set of facilities will define a region.  A report tool can
    thus join with rows in this table when querying for the respective
    result set.

    """
    pass


mapper(ReportableRegion, internal_reportable_region)


internal_report = Table(
    'internal_report', metadata,
    Column('pk', Integer, primary_key=True),
    Column('processed_datetime', DateTime, nullable=False),
    Column('file_path', VARCHAR(255), nullable=False),
    Column('report_method', VARCHAR(255), nullable=False),
    Column('metadata', VARCHAR(255), default=None))


class Report(OrmObject):
    """Report details

    A variety of reports will be generated from the various tables.
    This table maintains a history of what report_method was used to
    generate the report, any interesting metadata, and processing
    date, useful for audits and to help catch when updates have
    arrived since a report was processed.

    """
    pass


mapper(Report, internal_report)

"""
================================================================
End table definitions.
=================================================================
"""


def create_essence_view(engine):
    """Create the view used by the essence report"""

    essence_view = """
        create or replace view essence as
        select fact_visit.pk as visit_pk,
        dim_facility.organization_name as hospital,
        to_char(fact_visit.admit_datetime, 'MM/DD/YYYY') as visit_date,
        to_char(fact_visit.admit_datetime, 'HH24:MI:SS') as visit_time,
        fact_visit.gender as gender,
        fact_visit.age as age,
        dim_cc.chief_complaint as chief_complaint,
        dim_location.zip as zip,
        dim_disposition.gipse_mapping as gipse_disposition,
        dim_disposition.odin_mapping as odin_disposition,
        fact_visit.patient_id as patient_id,
        fact_visit.visit_id as visit_id,
        fact_visit.patient_class as patient_class,
        dim_admission_temp.degree_fahrenheit as measured_temperature,
        dim_admission_o2sat.o2sat_percentage as o2_saturation,
        dim_flu_vaccine.status as influenza_vaccine,
        dim_h1n1_vaccine.status as h1n1_vaccine
        from fact_visit
        left join dim_facility
          on dim_facility.npi = dim_facility_pk
        left join dim_location
          on dim_location.pk = dim_location_pk
        left join dim_cc
          on dim_cc.pk = dim_cc_pk
        left join dim_disposition
          on dim_disposition.code = dim_disposition_pk
        left join dim_admission_temp
          on dim_admission_temp.pk = dim_admission_temp_pk
        left join dim_admission_o2sat
          on dim_admission_o2sat.pk = dim_admission_o2sat_pk
        left join dim_flu_vaccine
          on dim_flu_vaccine.pk = dim_flu_vaccine_pk
        left join dim_h1n1_vaccine
          on dim_h1n1_vaccine.pk = dim_h1n1_vaccine_pk
        """
    engine.execute(text(essence_view))


def create_tables(user=None, password=None, database=None,
                  enable_delete=False):
    """Create the longitudinal database tables.

    NB - the config [longitudinal]database_user is granted SELECT
    INSERT and UPDATE permissions (plus DELETE if enable_delete is set).

    :param user: database user with table creation grants
    :param password: the database password
    :param database: the database name to populate
    :param enable_delete: testing hook, override for testing needs

    """
    engine = create_engine("postgresql://%s:%s@localhost/%s" %
                           (user, password, database))

    # Can't find a way to code the cascade to a view
    # drop manually as the createTables will otherwise fail.
    try:
        engine.execute(text("DROP VIEW IF EXISTS essence"))
    except OperationalError:  # pragma: no cover
        # Assume it's a new database.  Otherwise subsequent failures
        # will catch any real trouble.
        pass

    metadata.drop_all(bind=engine)
    metadata.create_all(bind=engine)

    def bless_user(user):
        engine.execute("""BEGIN; GRANT SELECT, INSERT, UPDATE %(delete)s ON
                       assoc_visit_dx,
                       assoc_visit_lab,
                       dim_admission_o2sat,
                       dim_admission_source,
                       dim_admission_temp,
                       dim_assigned_location,
                       dim_cc,
                       dim_disposition,
                       dim_dx,
                       dim_facility,
                       dim_flu_vaccine,
                       dim_h1n1_vaccine,
                       dim_lab_flag,
                       dim_lab_result,
                       dim_location,
                       dim_note,
                       dim_order_number,
                       dim_performing_lab,
                       dim_pregnancy,
                       dim_race,
                       dim_ref_range,
                       dim_service_area,
                       dim_specimen_source,
                       fact_visit,
                       internal_export_delta,
                       internal_message_processed,
                       internal_report,
                       internal_reportable_region
                       TO %(user)s; COMMIT;""" %
                       {'delete': ", DELETE" if enable_delete else '',
                        'user': user})

        # Sequences also require UPDATE
        engine.execute("""BEGIN; GRANT SELECT, UPDATE ON
                       dim_admission_o2sat_pk_seq,
                       dim_admission_temp_pk_seq,
                       dim_assigned_location_pk_seq,
                       dim_cc_pk_seq,
                       dim_dx_pk_seq,
                       dim_flu_vaccine_pk_seq,
                       dim_h1n1_vaccine_pk_seq,
                       dim_lab_flag_pk_seq,
                       dim_lab_result_pk_seq,
                       dim_location_pk_seq,
                       dim_note_pk_seq,
                       dim_order_number_pk_seq,
                       dim_performing_lab_pk_seq,
                       dim_pregnancy_pk_seq,
                       dim_race_pk_seq,
                       dim_ref_range_pk_seq,
                       dim_service_area_pk_seq,
                       dim_specimen_source_pk_seq,
                       fact_visit_pk_seq,
                       internal_export_delta_pk_seq,
                       internal_report_pk_seq
                       TO %(user)s; COMMIT;""" % {'user': user})

    # Provide configured user necessary privileges
    bless_user(Config().get('longitudinal', 'database_user'))

    # Add any views
    create_essence_view(engine)


def main():  # pragma: no cover
    """Entry point to (re)create the table using config settings"""
    config = Config()
    database = config.get('longitudinal', 'database')
    print "destroy and recreate database %s ? "\
        "('destroy' to continue): " % database,
    answer = sys.stdin.readline().rstrip()
    if answer != 'destroy':
        print "aborting..."
        sys.exit(1)

    user = config.get('longitudinal', 'database_user')
    password = config.get('longitudinal', 'database_password')
    create_tables(user, password, database)


if __name__ == '__main__':  # pragma: no cover
    """ If run as a standalone, recreate the tables. """
    main()
