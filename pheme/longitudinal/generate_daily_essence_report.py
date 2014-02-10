#!/usr/bin/env python
from datetime import datetime, timedelta
import logging
from optparse import OptionParser
import os
import re

from pheme.util.config import Config, configure_logging
from pheme.util.datefile import Datefile
from pheme.util.util import parseDate
from pheme.util.pg_access import AlchemyAccess, DirectAccess
from pheme.longitudinal.tables import Report
from pheme.webAPIclient.archive import document_find, document_store
from pheme.webAPIclient.transfer import PHINMS_client, Distribute_client

# Front end to generation of daily essence reports from database

usage = """ %prog [options] database date

Generates a daily report from the requested database for the requested
date.  Includes the 24 hour period for that date.  All admissions and
any subsequent updates for admissions from that date as best known at
current time.  Also includes any updates to previous days admissions
that haven't previously been generated.

database - name of the database supporting the essence view.
date     - YYYY-MM-DD or YYYYMMDD format for the date of the report.

Try `%prog --help` for more information.
"""


def strSansNone(item):
    """Don't want the 'None' string, return str(item) or empty"""
    if item is None:
        return ''
    else:
        return str(item)


def raiseValueError(message):
    """Callable used as default callback for error messages"""
    raise ValueError(message)


class ReportCriteria(object):
    """Container to house common report criteria

    Essentially a list of properties with minor intel for setting and
    methods to fetch the criteria list for detailing decisions used in
    generating a report.

    """
    def __init__(self):
        # Maintain a dictionary for all criteria
        self._crit = dict()
        # Some attributes shouldn't get reset after report_method is
        # called.  Maintain a trivial attr list to enforce
        self._lock_attrs = ()

    def credentials(self, user, password):
        """Set user/password credentials for property validation

        The report criteria shouldn't depend on the db user, but some
        of the properties require database query validation.

        """
        self.user = user
        self.password = password

    @property
    def error_callback(self):
        "Registered error callback or function to raise ValueError"
        if not hasattr(self, '_error_callback'):
            return raiseValueError
        else:
            return self._error_callback

    @error_callback.setter
    def error_callback(self, func):
        """Provide a callback to report errors

        It is expected a call to self.error_callback will halt
        execution via a raised exception.

        """
        if 'error_callback' in self._lock_attrs:
            raise AttributeError("can't set attribute")
        self._error_callback = func

    @property
    def reportable_region(self):
        return self._crit.get('reportable_region')

    @reportable_region.setter
    def reportable_region(self, value):
        if 'reportable_region' in self._lock_attrs:
            raise AttributeError("can't set attribute")
        self._crit['reportable_region'] = value

    @property
    def start_date(self):
        return self._crit.get('start_date')

    @start_date.setter
    def start_date(self, value):
        if 'start_date' in self._lock_attrs:
            raise AttributeError("can't set attribute")
        if isinstance(value, basestring):
            value = parseDate(value)
        self._crit['start_date'] = value

    @property
    def end_date(self):
        return self._crit.get('end_date')

    @end_date.setter
    def end_date(self, value):
        if 'end_date' in self._lock_attrs:
            raise AttributeError("can't set attribute")
        if isinstance(value, basestring):
            value = parseDate(value)
        self._crit['end_date'] = value

    @property
    def reportable_region(self):
        return self._crit.get('reportable_region')

    @reportable_region.setter
    def reportable_region(self, value):
        if 'reportable_region' in self._lock_attrs:
            raise AttributeError("can't set attribute")
        # Confirm the requested region is in the db.
        if value:
            connection = DirectAccess(database=self.database,
                                      user=self.user,
                                      password=self.password)

            cursor = connection.raw_query("SELECT count(*) FROM "\
                                          "internal_reportable_region "\
                                          "WHERE region_name = '%s'" %
                                          value)
            if cursor.next()[0] < 1:
                self.error_callback("%s region not found in "\
                                    "internal_reportable_region table" %
                                    value)
            connection.close()
        self._crit['reportable_region'] = value

    @property
    def include_vitals(self):
        return self._crit.get('include_vitals')

    @include_vitals.setter
    def include_vitals(self, value):
        if 'include_vitals' in self._lock_attrs:
            raise AttributeError("can't set attribute")
        self._crit['include_vitals'] = value

    @property
    def include_updates(self):
        return self._crit.get('include_updates')

    @include_updates.setter
    def include_updates(self, value):
        if 'include_updates' in self._lock_attrs:
            raise AttributeError("can't set attribute")
        self._crit['include_updates'] = value

    @property
    def database(self):
        return self._crit.get('database')

    @database.setter
    def database(self, value):
        if 'database' in self._lock_attrs:
            raise AttributeError("can't set attribute")
        self._crit['database'] = value

    @property
    def patient_class(self):
        return self._crit.get('patient_class')

    @patient_class.setter
    def patient_class(self, value):
        if 'patient_class' in self._lock_attrs:
            raise AttributeError("can't set attribute")
        if value and value not in ('E', 'I', 'O'):
            self.error_callback("patient_class limited to one of [E,I,O]")
        self._crit['patient_class'] = value

    @property
    def report_method(self):
        """Persisted in database report.report_method

        Uniquely defines the report, used when including updates for
        noting when last like report was run.

        """
        # Look out for runtime changes, or errors where these
        # are set after using the report_method().  The attributes
        # used here should't change after this method is called, as
        # that would be misleading.
        self._lock_attrs = ('reportable_region',
                            'patient_class', 'include_vitals')
        details = ['essence_report', GenerateReport.__version__]
        for attr in self._lock_attrs:
            # If the value is 'True', store the attr name for the sake
            # of legibility
            detail = strSansNone(getattr(self, attr))
            if detail is True:
                detail = attr
            details.append(detail)
        return ':'.join(details)


class GenerateReport(object):
    """ Process options and generate the requested report.  Optionally
    persists the file to the filesystem, and uploads to the DOH sftp
    server.

    """
    __version__ = '0.2'
    config = Config()
    IGNORE_SITE = config.get('longitudinal', 'ignore_site', default='')

    # Order matters, create a tuple of paired values (reportColumn,
    # essenceColumn) - NB, the Diagnosis column is being bastardized.
    # Previously there was an SQL function to do the subselect, but it
    # ran way too slow.  Now contains the foreign key to join w/ the
    # diagnosis for the respective visit.

    diagnosis_column_index = 7
    patient_class_column_index = 11
    columns = (('Hosp', 'hospital'),
               ('Reg Date', 'visit_date'),
               ('Time', 'visit_time'),
               ('Sex', 'gender'),
               ('Age', 'age'),
               ('Reason For Visit', 'chief_complaint'),
               ('Zip Code', 'zip'),
               ('Diagnosis', 'visit_pk'),
               ('Admit Status', 'gipse_disposition'),
               ('Medical Record No.', 'patient_id'),
               ('Visit Record No.', 'visit_id'),
               ('Service Area', 'patient_class'),)
    assert(columns[diagnosis_column_index][1] == 'visit_pk')
    assert(columns[patient_class_column_index][1] == 'patient_class')

    def __init__(self, user=None, password=None, report_criteria=None,
                 datefile=None):
        """Initialize report generation.

        :param user: database user
        :param password: database password
        :param report_criteria: ReportCriteria defining specifics
        :param datefile: useful for persistent walks through time

        """
        self.user = user
        self.password = password
        self.criteria = report_criteria
        self.database = self.criteria.database
        if datefile:
            assert((self.criteria.start_date, self.criteria.end_date)
                   == datefile.get_date_range())
            self.datePersistence = datefile

        self._diags = {}
        self._prepare_output_file()
        self._prepare_columns()
        self._set_transport()

    def _prepare_columns(self):
        # Don't include the patient_class column if splitting out by
        # patient_class
        if self.criteria.patient_class:
            len_b4 = len(self.columns)
            self.columns =\
                self.columns[:self.patient_class_column_index] \
                + self.columns[self.patient_class_column_index + 1:]
            assert(len(self.columns) + 1 == len_b4)

    def _set_transport(self):
        """Plug in the appropriate transport mechanism"""
        # Transport strategies differ for the different reports
        if self.criteria.reportable_region:
            self._transport = Distribute_client(zip_first=True)
        else:
            self._transport = PHINMS_client(zip_first=True)

    def _generate_output_filename(self, start_date=None,
                                  end_date=None):
        start_date = self.criteria.start_date if start_date is None\
            else start_date
        end_date = self.criteria.end_date if end_date is None else end_date

        datestr = end_date.strftime('%Y%m%d')
        if start_date != end_date:
            datestr = '-'.join((start_date.strftime('%Y%m%d'),
                         end_date.strftime('%Y%m%d')))

        filename = self.criteria.report_method + '-' + datestr + '.txt'

        config = Config()
        tmp_dir = config.get('general', 'tmp_dir', default='/tmp')

        filepath = os.path.join(tmp_dir, filename)
        return filepath

    def _prepare_output_file(self):
        """Open the local filesystem file for output"""
        filepath = self.\
            _generate_output_filename(start_date=self.criteria.start_date,
                                      end_date=self.criteria.end_date)

        # watch for oversight errors; notify if like report exists -
        # unless it's size zero (from a previous failed run)
        if os.path.exists(filepath) and os.path.getsize(filepath):
            logging.warning("Found requested report file already "\
                             "exists - overwriting: '%s'"\
                             % filepath)

        self.output = open(filepath, 'w')
        self._output_filename = self.output.name

    @property
    def output_filename(self):
        if not hasattr(self, '_output_filename'):
            raise RuntimeError("prerequisite call to "\
                               "_prepare_output_file() "\
                "didn't happen!")
        return self._output_filename

    def _header(self):
        if self.criteria.include_vitals:
            columns = [c[0] for c in self.columns]
            columns += ('Measured Temperature', 'O2 Saturation',
                        'Self-Reported Influenza Vaccine',
                        'Self-Reported H1N1 Vaccine')
            return '|'.join(columns)

        else:
            return '|'.join([c[0] for c in self.columns])

    def _build_join_tables(self):
        """ Scope continues to grow, build all join tables necessary
        for the query.  Some are only necessary with certain features
        on.
        """
        # Always need the list of reportable visits
        self._build_visit_join_table()

        if self.criteria.include_vitals:
            self._build_vitals_join_table()

    def _build_visit_join_table(self):
        """ Helper in selection of visits for the report - this method
        builds a temporary table and populates it with the visit_pks
        that belong in the report.  This should include all visit_pks
        with the matching admit_datetime as well as any that have
        received updates since the last like report was produced.

        """
        # If include_vitals is on, we also need the visit_id to keep
        # the joins managable.  vitals don't have a patient class, so
        # you can't join on the same values.

        sql = "CREATE TEMPORARY TABLE reportable_pks (pk "\
            "integer not null unique)"
        selectCols = "fact_visit.pk"

        self._getConn()
        self.access.raw_query(sql)

        # If we're only selecting those facilites in a region, the SQL
        # is more complicated - build up the respective clauses.
        joinClause = regionClause = ""
        if self.criteria.reportable_region:
            joinClause = "JOIN internal_reportable_region ON "\
                "internal_reportable_region.dim_facility_pk = "\
                "fact_visit.dim_facility_pk"
            regionClause = "AND region_name = '%s'" %\
                self.criteria.reportable_region

        # Another HACK!  One site is not even wanted by the state DOH,
        # as it's being duplicated from another source, and ESSENCE
        # can't help but count them twice.  Remove this one site
        # regardless
        else:
            joinClause = "JOIN internal_reportable_region ON "\
                "internal_reportable_region.dim_facility_pk = "\
                "fact_visit.dim_facility_pk"
            regionClause = "AND region_name = '%s'" % self.IGNORE_SITE

        # Limit by patient_class if requested.  Note we may still end
        # up with visit ids that have changed patient classes, so more
        # pruning later is necessary.
        pc_limit = ""
        if self.criteria.patient_class:
            pc_limit = "AND patient_class = '%c'" %\
                self.criteria.patient_class

        # Start with all visits for the requested date range
        sql = "INSERT INTO reportable_pks SELECT %s FROM "\
              "fact_visit %s WHERE admit_datetime BETWEEN '%s' AND "\
              "'%s' %s %s" %\
              (selectCols, joinClause, self.criteria.start_date,
               self.criteria.end_date + timedelta(days=1),
               pc_limit, regionClause)

        self.access.raw_query(sql)

        if self.criteria.include_updates:
            # In this case, add all visits with updates since the
            # last run, but no newer than the requested date (in case
            # we're building reports forward from historical data)
            sql = "SELECT max(processed_datetime) FROM internal_report "\
                  "WHERE report_method = '%s'" % self.criteria.report_method

            cursor = self.access.raw_query(sql)
            last_report_generated = cursor.fetchall()[0][0]
            if last_report_generated is None:
                last_report_generated = '2009-01-01'  # our epoch
            logging.debug("including updates, last_report_generated: "\
                              "%s", last_report_generated)
            sql = "INSERT INTO reportable_pks SELECT %(sel_cols)s FROM "\
                  "fact_visit %(join_clause)s LEFT JOIN reportable_pks ON "\
                  "reportable_pks.pk = fact_visit.pk WHERE "\
                  "last_updated > '%(last_report)s' AND admit_datetime "\
                  "< '%(date)s' AND reportable_pks.pk IS NULL "\
                  "%(pc_limit)s %(region_clause)s" %\
                  {'sel_cols': selectCols,
                   'last_report': last_report_generated,
                   'date': self.criteria.end_date + timedelta(days=1),
                   'pc_limit': pc_limit,
                   'join_clause': joinClause,
                   'region_clause': regionClause}
            self.access.raw_query(sql)

        cursor = self.access.raw_query("SELECT COUNT(*) FROM "\
                                           "reportable_pks")
        logging.debug("%d visits to report on", cursor.fetchall()[0][0])

    def _build_vitals_join_table(self):
        """When report is to include vitals - we use an additional
        temporary table (visit_loinc_data) to hold the data for more
        timely queries.

        Like the rest of the report, the list of interesting visits is
        limited to the rows in the reportable_pks - see
        _build_join_table() for details.

        """
        raise ValueError('not ported yet')
        sql = """
          CREATE TEMPORARY TABLE visit_loinc_data (
            visit_id VARCHAR(255) not null,
            patient_class CHAR(1) default null,
            observation_id VARCHAR(255) not null,
            observation_result VARCHAR(255) not null)
          """
        self._getConn()
        self.access.raw_query(sql)

        sql = """
          INSERT INTO visit_loinc_data (visit_id, patient_class,
          observation_id, observation_result) SELECT visit.visit_id,
          visit.patient_class, observation_id,
          observation_result FROM visit JOIN hl7_visit ON
          visit.visit_id = hl7_visit.visit_id JOIN hl7_obx ON
          hl7_visit.hl7_msh_id = hl7_obx.hl7_msh_id JOIN
          reportable_pks ON reportable_pks.visit_id = visit.visit_id
          AND reportable_pks.patient_class = visit.patient_class
          WHERE
          observation_id in ('8310-5', '20564-1', '46077-4',
          '29544-4')
          """
        self.access.raw_query(sql)

    def _select_from_essence_view(self):
        """Build up the SQL select statement to be used in gathering
        the data for this report.

        """
        stmt = """SELECT %s FROM essence e JOIN reportable_pks ri
        ON e.visit_pk = ri.pk""" %\
            (','.join(['e.' + c[1] for c in self.columns]))
        return stmt

    def _select_diagnosis(self):
        """ Need to pull in all the diagnosis data for this report.
        This is saved in an instance dictionary for use in
        self._diagnosis to generate the list of diagnoses for each
        respective visit.

        A list of unique diagnoses ordered by rank is required.
        """
        # We order descending on dx_datetime as the most recent should
        # be best.  Add any others as the persistence mechanism only
        # saves a unique icd9 dx that has changed status.
        stmt = "SELECT fact_visit_pk, rank, icd9 "\
               "FROM assoc_visit_dx JOIN "\
               "dim_dx ON dim_dx_pk = dim_dx.pk JOIN "\
               "reportable_pks ON "\
               "assoc_visit_dx.fact_visit_pk = reportable_pks.pk "\
               "ORDER BY dx_datetime DESC"
        cursor = self.access.raw_query(stmt)
        for row in cursor.fetchall():
            visit_pk = row[0]
            if visit_pk in self._diags:
                self._diags[visit_pk].add(row[0], row[1], row[2])
            else:
                self._diags[visit_pk] = \
                    SortedDiagnosis(row[0], row[1], row[2])

    def _diagnosis(self, visit_pk):
        if visit_pk in self._diags:
            return [self._diags[visit_pk].__repr__(), ]
        else:
            return ['', ]

    def _select_vitals(self):
        """ Need to pull in all the vitals data for this report.
        This is saved in an instance dictionary for use in
        self._vitals_for_visit to generate the list of vitals for each
        respective visit.

        This is an effective NOP when self.criteria.include_vitals = False

        """
        if not self.criteria.include_vitals:
            return None

        self._vitals = {}
        stmt = """SELECT reportable_pks.visit_pk,
          observation_id, observation_result
          FROM visit_loinc_data JOIN reportable_pks ON
          reportable_pks.visit_id = visit_loinc_data.visit_id"""

        cursor = self.access.raw_query(stmt)
        for row in cursor.fetchall():
            visit_pk = row[0]
            if visit_pk in self._vitals:
                self._vitals[visit_pk].add(row[1], row[2])
            else:
                self._vitals[visit_pk] = \
                    Vitals(row[1], row[2])

    def _vitals_for_visit(self, visit_pk):
        """Returns the list of vitals for the visit in question.

        This is an effective NOP when self.criteria.include_vitals = False

        """
        if not self.criteria.include_vitals:
            return []

        if visit_pk in self._vitals:
            return self._vitals[visit_pk].__repr__()
        else:
            return Vitals().__repr__()

    def _write_report(self, save_report=False):
        """ Write out and potentially store the results.

        Generate results via database queries and write the results to
        self.output.

        :param save_report: If set, persist the document and related
          metadata to the mbds archive.

        returns the document ID, the mbds archive key, if saved

        """
        out = self.output
        print >> out, self._header()
        self._build_join_tables()
        self._select_diagnosis()
        self._select_vitals()
        cursor = self.access.raw_query(self._select_from_essence_view())
        for row in cursor.fetchall():
            # Each row is the colums up to the diagnosis + the
            # comma separated diagnosis + the rest of the columns
            # and finally with vitals if configured for such
            visit_pk = row[self.diagnosis_column_index]  # yuck, but true
            print >> out,\
                '|'.join([strSansNone(column) for column in
                          row[:self.diagnosis_column_index]] +
                         self._diagnosis(visit_pk) +
                         [strSansNone(column) for column in
                          row[self.diagnosis_column_index + 1:]] +
                         self._vitals_for_visit(visit_pk))

        # Close the file and persist to the document archive if
        # requested
        self.output.close()
        if save_report:
            metadata = {k: v for k, v in self.criteria._crit.items() if v
                        is not None}

            # At this point, all documents are of 'essence' type
            return document_store(document=self.output.name,
                                  allow_duplicate_filename=True,
                                  document_type='essence', **metadata)

    def _record_report(self, report_oid):
        """Record the details from this report generation in the db"""
        if not report_oid:
            return
        report = Report(processed_datetime=datetime.now(),
                        file_path=report_oid,
                        report_method=self.criteria.report_method)

        alchemy = AlchemyAccess(database=self.database)
        alchemy.session.add(report)
        alchemy.session.commit()
        alchemy.disconnect()

    def _transmit_report(self, report):
        """Transmit report using self._transport()"""
        logging.info("initiate upload of %s", report)
        self._transport.transfer_file(report)

    def _transmit_differences(self, report):
        """Compute differences from yesterday's like report; transport"""

        # This option really only makes sense on date range reports,
        # as updates hit older data than just 'yesterday'.
        if self.criteria.start_date == self.criteria.end_date:
            raise ValueError("difference calculation not supported on "\
                             "single day reports")
        # See if we can find a similar report in the archive from
        # yesterday
        search_criteria = {'report_method':
                           self.criteria.report_method,
                           'start_date': self.criteria.start_date -
                           timedelta(days=1), 'end_date':
                           self.criteria.end_date - timedelta(days=1)}
        old_doc = document_find(search_criteria, limit=1)
        if old_doc is None:
            logging.info("No comparable report found for difference "\
                         "generation")
            self._transmit_report(report)
        else:
            target_filename = self.\
                _generate_output_filename(start_date=self.criteria.start_date,
                                          end_date=self.criteria.end_date)
            # RemoveDuplicates not yet ported!!
            raise ValueError("RemoveDuplicates not ported")
            #from pheme.essence.remove_duplicates import RemoveDuplicates
            #rd = RemoveDuplicates(new_report=report,
            #                      old_report=old_doc,
            #                      out=target_filename)
            #rd.generate_report()
            #logging.info("initiate upload of difference %s", target_filename)
            #self._transport.transfer_file(target_filename)

    def _getConn(self):
        """ Local wrapper to get database connection
        """
        if hasattr(self, 'access'):
            return
        self.access = DirectAccess(database=self.database,
                                   user=self.user,
                                   password=self.password)

    def _closeConn(self):
        """ Local wrapper to close database connection
        """
        if hasattr(self, 'access'):
            self.access.close()

    def tearDown(self):
        "Public interface to clean up internals"
        self._closeConn()

    def execute(self, save_report=False, transmit_report=False,
                 transmit_differences=False):
        """Execute the report generation
        """
        logging.info("Initiate ESSENCE report generation [%s-%s] for %s",
                     self.criteria.start_date,
                     self.criteria.end_date,
                     self.criteria.report_method)

        self._getConn()
        report_oid = self._write_report(save_report)
        self._record_report(report_oid)
        if transmit_report:
            self._transmit_report(report_oid)
        if transmit_differences:
            self._transmit_differences(report_oid)
        self._closeConn()
        if hasattr(self, 'datePersistence'):
            self.datePersistence.bump_date()

        logging.info("Completed ESSENCE report generation [%s-%s] for %s",
                     self.criteria.start_date,
                     self.criteria.end_date,
                     self.criteria.report_method)


class SortedDiagnosis(object):
    """ Special class unlikely to have use beyond report generation -
    this is used to build up a list of diagnosis for a visit,
    maintaining order.  Capable of spitting it back out in the
    format as required by essence.

    Extending to hide duplicate diagnoses, where a unique diagnosis is
    defined by (icd9).  If a duplicate is added, it is ignored.

    """
    def __init__(self, visit_pk, rank, icd9):
        self.visit_pk = visit_pk
        self.ordered_list = []
        self.ordered_list.append({'rank': rank,
                                  'icd9': icd9})
        self._contains = set()
        self._contains.add(self._gen_key(icd9=icd9))

    def _gen_key(self, icd9):
        return icd9.__hash__()

    def add(self, visit_pk, rank, icd9):
        assert(self.visit_pk == visit_pk)
        key = self._gen_key(icd9=icd9)
        if key in self._contains:
            return

        placed = False
        for i, elem in enumerate(self.ordered_list):
            if rank < elem['rank']:
                self.ordered_list.insert(i, {'rank': rank,
                                             'icd9': icd9})
                placed = True
                break

        if not placed:
            self.ordered_list.append({'rank': rank,
                                      'icd9': icd9})
        self._contains.add(key)

    def __repr__(self):
        """Return space delimited string of ordered ICD9 codes"""
        return ' '.join([e['icd9'] for e in self.ordered_list])

obx5_5_1 = re.compile("<OBX.5><OBX.5.1>(.*?)</OBX.5.1></OBX.5>")


class Vitals(object):
    """Another helper class (like SortedDiagnosis) unlikely to have a
    utility beyond report creation.

    Manages taking query results and creating a very lightweigh object
    for the vitals currently of interest.

    Expected use case has an instance of this class for every visit_pk
    in the report where vitals were present.

    """

    def __init__(self, observation_id=None, observation_result=None):
        self.coded_data = {}
        if observation_id and observation_result:
            self.add(observation_id, observation_result)

    def add(self, observation_id, observation_result):
        if observation_id in self.coded_data:
            # Duplicate handling means keep the first value seen
            return
        self.coded_data[observation_id] =\
            self.stripXML(observation_result)

    def stripXML(self, observation_result):
        m = obx5_5_1.match(observation_result)
        if m:
            if ('</' in m.groups()[0]):
                raise ValueError("Smarter XML parser needed for '%s'"
                                 % observation_result)
            return m.groups()[0]
        return ''

    def __repr__(self):
        """Returns list representation of all vitals given.  The order
        must match that in the header, namely:

            columns += ('Measured Temperature', 'O2 Saturation',
                        'Self-Reported Influenza Vaccine',
                        'Self-Reported H1N1 Vaccine')

        Those map directly to the loinc codes (aka observation_ids):

            ('8310-5', '20564-1', '46077-4', '29544-4')

        Empty strings returned for any non existing values.

        """
        return [self.coded_data.get(loinc, '') for loinc in
                    ('8310-5', '20564-1', '46077-4', '29544-4')]


class ReportCommandLineInterface(object):
    """Command line interface to generating reports

    Collects arguments and assembles classes needed to generate any
    report.

    """
    def __init__(self):
        """initializer for CLI"""
        # All criteria used to uniquely define a report
        self.criteria = ReportCriteria()

        # Any additional attributes collected but not necessarily
        # unique to recreating a like report
        self.verbosity = 0
        self.datefile = None
        self.user = None
        self._password = None
        self.save_report = False
        self.transmit_report = False
        self.transmit_differences = False

    @property
    def password(self):
        return self._password

    @password.setter
    def password(self, value):
        """Password may be plain text or a file containing it"""
        # If the password argment is a readable file, fetch the
        # password from within
        if value and os.path.exists(value):
            passwordFile = open(value, 'r')
            value = passwordFile.readline().rstrip()
            passwordFile.close()
        self._password = value

    def process_args(self):
        """Process any optional arguments and possitional parameters

        Using the values provided, assemble ReportCriteria and
        Datefile instances to control report generation.

        """
        parser = OptionParser(usage=usage)
        # Provide the ReportCriteria instance an error callback so any
        # command line errors provoke the standard graceful exit with
        # warning text.
        self.criteria.error_callback = parser.error

        parser.add_option("-u", "--user", dest="user",
                          default=self.user, help="database user")
        parser.add_option("-p", "--password", dest="password",
                          default=self.password,
                          help="database password, or file containing "\
                              "just the password")
        parser.add_option("-c", "--countdown", dest="countdown",
                          default=None,
                          help="count {down,up} the start and end dates "\
                              "set to 'forwards' or 'backwards' "\
                              "if desired")
        parser.add_option("-i", "--include-updates",
                          action='store_true', dest="includeUpdates",
                          default=False, help="include "\
                              "visits updated since last similar report")
        parser.add_option("--include-vitals",
                          action='store_true', dest="includeVitals",
                          default=False, help="include "\
                              "vitals (measured temperature, O2 "\
                              "saturation, influenza and H1N1 vaccine "\
                              "data) as additional columns in the "\
                              "report")
        parser.add_option("-k", "--patient-class",
                          dest="patient_class",
                          default=None, help="use "\
                          "to filter report on a specific patient "\
                          "class [E,I,O]")
        parser.add_option("-r", "--region", dest="region",
                          default=None,
                          help="reportable region defining limited set "\
                              "of facilities to include, by default "\
                              "all  facilities are included")
        parser.add_option("-s", "--save-and-upload",
                          action='store_true', dest="save_upload",
                          default=False, help="save file and upload to "\
                              "DOH")
        parser.add_option("-x", "--save-without-upload",
                          action='store_true', dest="save_only",
                          default=False, help="save file but don't upload")
        parser.add_option("-d", "--upload-diff",
                          action='store_true', dest="upload_diff",
                          default=False, help="upload differences only "\
                              "(from yesterdays like report) to DOH")
        parser.add_option("-t", "--thirty-days",
                          action='store_true', dest="thirty_days",
                          default=False, help="include 30 days up to "\
                              "requested date ")
        parser.add_option("-v", "--verbose", dest="verbosity",
                          action="count", default=self.verbosity,
                          help="increase output verbosity")

        (options, args) = parser.parse_args()
        if len(args) != 2:
            parser.error("incorrect number of arguments")

        # Database to query
        self.criteria.database = args[0]
        self.user = options.user
        self.password = options.password
        self.criteria.credentials(user=self.user,
                                  password=self.password)

        # Potential region restriction
        self.criteria.reportable_region = options.region

        # Potential patient class restriction
        self.criteria.patient_class = options.patient_class

        # Potential to include vitals (not tied to gipse format)
        self.criteria.include_vitals = options.includeVitals

        # Potential inclusion of updates
        self.criteria.include_updates = options.includeUpdates

        # Report date(s) and potential step direction.
        # NB - several options affect report_method and must be set
        # first!

        initial_date = parseDate(args[1])
        config = Config()
        ps_file = os.path.join(config.get('general', 'tmp_dir',
                                default='/tmp'),
                                self.criteria.report_method)
        step = options.thirty_days and 30 or None
        direction = options.countdown
        self.datefile = Datefile(initial_date=initial_date,
                                 persistence_file=ps_file,
                                 direction=direction,
                                 step=step)
        self.criteria.start_date, self.criteria.end_date =\
            self.datefile.get_date_range()

        # What to do once report is completed.  Complicated, protect
        # user from themselves!
        self.save_report = options.save_upload or \
            options.save_only or options.upload_diff
        self.transmit_report = options.save_upload
        self.transmit_differences = options.upload_diff

        if options.save_only and options.save_upload:
            parser.error("save-without-upload and save-and-upload "\
                         "are mutually exclusive")
        if options.save_only and options.upload_diff:
            parser.error("save-without-upload and upload-diff "\
                         "are mutually exclusive")
        if options.upload_diff and options.save_upload:
            parser.error("upload-diff and save-and-upload"\
                         "are mutually exclusive")

        # Can't transmit w/o saving
        if options.save_upload or options.upload_diff:
            assert(self.save_report)
        # Sanity check
        if options.save_only:
            assert(self.save_report and not self.transmit_report and
                   not self.transmit_differences)

        # How verbosely to log
        self.verbosity = options.verbosity

    def execute(self):
        """Use the collected info to launch execution"""
        configure_logging(verbosity=self.verbosity,
                         logfile="%s.log" % self.criteria.report_method)

        gr = GenerateReport(user=self.user,
                            password=self.password,
                            report_criteria=self.criteria,
                            datefile=self.datefile)
        gr.execute(save_report=self.save_report,
                    transmit_report=self.transmit_report,
                    transmit_differences=self.transmit_differences)


def main():
    cli = ReportCommandLineInterface()
    cli.process_args()
    cli.execute()

if __name__ == "__main__":
    main()
