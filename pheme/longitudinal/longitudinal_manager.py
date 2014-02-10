#!/usr/bin/env python

from datetime import timedelta
import logging
from optparse import OptionParser
import os
from multiprocessing import JoinableQueue, Process, Lock
import time

from sqlalchemy.sql import and_

from .longitudinal_worker import LongitudinalWorker
from .tables import MessageProcessed
from pheme.util.datefile import Datefile
from pheme.util.lock import Lock as FileLock
from pheme.util.pg_access import AlchemyAccess, DirectAccess
from pheme.util.config import configure_logging
from pheme.util.util import parseDate, systemUnderLoad

usage = """%prog [options] data_warehouse data_mart

Manages the process of generating the best known state for the data
that comes in via hl7 messages (found in the data_warehouse).  This is
generally handled by playing forward the events (ordered by the
message_datatime from the facility) on a visit, and persisting the
best longitudinal record in the data_mart.

Try `%prog --help` for more information.
"""
LOCKFILE = "LONGITUDINAL_MANAGER"


class LongitudinalManager(object):
    """ Abstraction to handle which db, user, etc. the deduplication
    process should be run on.  Handles runtime arguments and
    execution.

    Manages the process by farming out the individual visit
    deduplication to a number of worker processes (necessary to take
    advatage of multi-core processor and database as the limiting
    factor).

    """
    # The gating issue is the number of postgres connections that are
    # allowed to run concurrently.  Setting this to N-1 (where N is
    # the number of cores) has proven the fastest and most reliable.
    NUM_PROCS = 5

    def __init__(self, data_warehouse=None, data_mart=None,
                 reportDate=None, database_user=None,
                 database_password=None, verbosity=0):
        self.data_warehouse = data_warehouse
        self.warehouse_port = 5432  # postgres default
        self.data_mart = data_mart
        self.mart_port = 5432  # postgres default
        self.reportDate = reportDate and parseDate(reportDate) or None
        self.database_user = database_user
        self.database_password = database_password
        self.dir, thisFile = os.path.split(__file__)
        self.verbosity = verbosity
        self.queue = JoinableQueue()
        self.datefile = "/tmp/longitudinal_datefile"
        self.datePersistence = Datefile(initial_date=self.reportDate)
        self.lock = FileLock(LOCKFILE)
        self.skip_prep = False

    def __call__(self):
        return self.execute()

    def processArgs(self):
        """ Process any optional arguments and possitional parameters
        """
        parser = OptionParser(usage=usage)
        parser.add_option("-c", "--countdown", dest="countdown",
                          default=None,
                          help="count {down,up} date using date string in "\
                              "%s - set to 'forwards' or 'backwards' "\
                              "if desired" % self.datefile)
        parser.add_option("-d", "--date", dest="date", default=None,
                          help="single admission date to dedup "\
                          "(by default, checks the entire database)")
        parser.add_option("-s", "--skip-prep", dest="skip_prep",
                          default=False, action="store_true",
                          help="skip the expense of looking for new "\
                          "messages")
        parser.add_option("-v", "--verbose", dest="verbosity",
                          action="count", default=self.verbosity,
                          help="increase output verbosity")
        parser.add_option("-m", "--mart-port", dest="mart_port",
                          default=self.mart_port, type="int",
                          help="alternate port number for data mart")
        parser.add_option("-w", "--warehouse-port", dest="warehouse_port",
                          default=self.warehouse_port, type="int",
                          help="alternate port number for data warehouse")

        (options, args) = parser.parse_args()
        if len(args) != 2:
            parser.error("incorrect number of arguments")

        self.data_warehouse = args[0]
        self.data_mart = args[1]
        self.warehouse_port = parser.values.warehouse_port
        self.mart_port = parser.values.mart_port
        self.verbosity = parser.values.verbosity
        self.skip_prep = parser.values.skip_prep
        initial_date = parser.values.date and \
            parseDate(parser.values.date) or None
        self.datePersistence = Datefile(initial_date=initial_date,
                                        persistence_file=self.datefile,
                                        direction=parser.values.countdown)

        self.reportDate = self.datePersistence.get_date()

    def _prepDeduplicateTables(self):
        """ Add any missing rows to the MessageProcessed table

        This is the bridge between the data warehouse and the data
        mart.  In an effort to make the data mart independent of the
        warehouse, the processed message data is kept in the mart.  As
        we're dealing with two distinct databases, there's no
        referential integrity available at the database level, so care
        should be taken.

        """
        startTime = time.time()
        logging.info("Starting INSERT INTO internal_message_processed "
                     "at %s", startTime)

        # We can take advantage of an "add only" data_warehouse,
        # knowing the hl7_msh_id is a sequence moving in the positive
        # direction.  Simply add any values greater than the previous
        # max.

        stmt = "SELECT max(hl7_msh_id) from internal_message_processed"
        max_id = self.data_mart_access.engine.execute(stmt).first()[0]
        if not max_id:
            max_id = 0

        new_msgs = list()
        stmt = """SELECT hl7_msh_id, message_datetime, visit_id
        FROM hl7_msh JOIN hl7_visit USING (hl7_msh_id) WHERE
        hl7_msh_id > %d """ % max_id
        rs = self.data_warehouse_access.engine.execute(stmt)
        many = 500
        while True:
            results = rs.fetchmany(many)
            if not results:
                break
            for r in results:
                new_msgs.append(MessageProcessed(hl7_msh_id=r[0],
                                                 message_datetime=r[1],
                                                 visit_id=r[2]))

            self.data_mart_access.session.add_all(new_msgs)
            self.data_mart_access.session.commit()
            logging.debug("added %d new messages" % len(new_msgs))
            new_msgs = list()

        logging.info("Added new rows to internal_message_processed in %s",
                     time.time() - startTime)

    def _visitsToProcess(self):
        """ Look up all distinct visit ids needing attention

        Obtain unique list of visit_ids that have messages that
        haven't previously been processed.  If the user requested just
        one days worth (i.e. -d) only that days visits will be
        returned.

        """
        visit_ids = list()
        if not self.reportDate:
            logging.info("Launch deduplication for entire database")
            # Do the whole batch, that is, all that haven't been
            # processed before.
            stmt = """SELECT DISTINCT(visit_id) FROM
            internal_message_processed
            WHERE processed_datetime IS NULL"""
            rs = self.data_mart_access.engine.execute(stmt)
            many = 10000
            while True:
                results = rs.fetchmany(many)
                if not results:
                    break
                for r in results:
                    visit_ids.append(r[0])

        else:
            logging.info("Launch deduplication for %s",
                         self.reportDate)
            # Process the requested day only - as we can't join across
            # db boundaries - first acquire the full list of visits
            # for the requested day from the data_warehouse to use in
            # a massive 'in' clause

            stmt = """SELECT DISTINCT(visit_id) FROM hl7_visit WHERE
            admit_datetime BETWEEN '%s' AND '%s';""" %\
            (self.reportDate, self.reportDate + timedelta(days=1))
            self.access.raw_query(stmt)
            rs = self.data_warehouse_access.engine.execute(stmt)
            many = 1000
            potential_visit_ids = list()
            while True:
                results = rs.fetchmany(many)
                if not results:
                    break
                for r in results:
                    #tmp_table.insert(r[0])
                    potential_visit_ids.append(r[0])

            if potential_visit_ids:
                query = self.data_mart_access.session.query(\
                    MessageProcessed.visit_id).distinct().\
                    filter(and_(MessageProcessed.processed_datetime ==
                                None,
                                MessageProcessed.visit_id.\
                                in_(potential_visit_ids)))

                for r in query:
                    visit_ids.append(r[0])

        logging.info("Found %d visits needing attention",
                     len(visit_ids))
        return visit_ids

    def tearDown(self):
        """ Clean up any open handles/connections """
        # now done in execute when we're done with teh connections

    def execute(self):
        """ Start the process """
        # Initialize logging now (verbosity is now set regardless of
        # invocation method)
        configure_logging(verbosity=self.verbosity,
                          logfile="longitudinal-manager.log")

        logging.info("Initiate deduplication for %s",
                         (self.reportDate and self.reportDate or
                          "whole database"))
        # Only allow one instance of the manager to run at a time.
        if self.lock.is_locked():
            logging.warn("Can't continue, %s is locked ", LOCKFILE)
            return

        if systemUnderLoad():
            logging.warn("system under load - continue anyhow")

        try:
            self.lock.acquire()

            self.access = DirectAccess(database=self.data_warehouse,
                                       port=self.warehouse_port,
                                       user=self.database_user,
                                       password=self.database_password)
            self.data_warehouse_access = AlchemyAccess(
                database=self.data_warehouse,
                port=self.warehouse_port,
                user=self.database_user, password=self.database_password)
            self.data_mart_access = AlchemyAccess(
                database=self.data_mart, port=self.mart_port,
                user=self.database_user, password=self.database_password)

            startTime = time.time()
            if not self.skip_prep:
                self._prepDeduplicateTables()
            visits_to_process = self._visitsToProcess()

            # Now done with db access needs at the manager level
            # free up resources:
            self.data_mart_access.disconnect()
            self.data_warehouse_access.disconnect()
            self.access.close()

            # Set of locks used, one for each table needing protection
            # from asynchronous inserts.  Names should match table
            # minus 'dim_' prefix, plus '_lock' suffix
            # i.e. dim_location -> 'location_lock'
            table_locks = {'admission_source_lock': Lock(),
                           'admission_o2sat_lock': Lock(),
                           'admission_temp_lock': Lock(),
                           'assigned_location_lock': Lock(),
                           'admit_reason_lock': Lock(),
                           'chief_complaint_lock': Lock(),
                           'diagnosis_lock': Lock(),
                           'disposition_lock': Lock(),
                           'flu_vaccine_lock': Lock(),
                           'h1n1_vaccine_lock': Lock(),
                           'lab_flag_lock': Lock(),
                           'lab_result_lock': Lock(),
                           'location_lock': Lock(),
                           'note_lock': Lock(),
                           'order_number_lock': Lock(),
                           'performing_lab_lock': Lock(),
                           'pregnancy_lock': Lock(),
                           'race_lock': Lock(),
                           'reference_range_lock': Lock(),
                           'service_area_lock': Lock(),
                           'specimen_source_lock': Lock(),
                           }

            # If we have visits to process, fire up the workers...
            if len(visits_to_process) > 1:
                for i in range(self.NUM_PROCS):
                    dw = Process(target=LongitudinalWorker,
                                 kwargs={'queue': self.queue,
                                         'procNumber': i,
                                         'data_warehouse': self.data_warehouse,
                                         'warehouse_port': self.warehouse_port,
                                         'data_mart': self.data_mart,
                                         'mart_port': self.mart_port,
                                         'dbUser': self.database_user,
                                         'dbPass': self.database_password,
                                         'table_locks': table_locks,
                                         'verbosity': self.verbosity})
                    dw.daemon = True
                    dw.start()

                # Populate the queue
                for v in visits_to_process:
                    self.queue.put(v)

                # Wait on the queue until empty
                self.queue.join()

            # Common cleanup
            self.tearDown()
            self.datePersistence.bump_date()
            logging.info("Queue is empty - done in %s", time.time() -
                         startTime)
        finally:
            self.lock.release()


def main():
    dedup = LongitudinalManager()
    dedup.processArgs()

    # Real deal - time to execute the deduplication.
    dedup()

if __name__ == "__main__":
    """ If run as a standalone, run the deduplication process. """
    main()
