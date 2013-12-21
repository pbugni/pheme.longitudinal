import unittest
from multiprocessing import Lock, Process

from pheme.longitudinal.tables import create_tables
from pheme.longitudinal.tables import Pregnancy, Location
from pheme.longitudinal.select_or_insert import SelectOrInsert
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


class TestSelectOrCreate(unittest.TestCase):
    """Test the locking select or create mechanism"""
    def setUp(self):
        self.conn = db_connection(CONFIG_SECTION)
        self.lock = Lock()
        self.s_or_i = SelectOrInsert(self.lock, self.conn.session)
        self.remove_after_test = []

    def tearDown(self):
        # if not persisted, don't try to delete
        persisted = [obj for obj in self.remove_after_test if obj.pk
                     is not None]
        map(self.conn.session.delete, persisted)
        self.conn.session.commit()
        self.conn.disconnect()

    def testNonExisting(self):
        preg = Pregnancy(result='Patient Currently Pregnant')
        self.remove_after_test.append(preg)
        preg = self.s_or_i.fetch(preg)
        self.assertTrue(preg.pk)

    def testExisting(self):
        loc = Location(country='USA', county='king', state='WA',
                       zip='98101')
        self.remove_after_test.append(loc)
        query = self.conn.session.query(Location)
        self.assertFalse(query.count())
        self.assertFalse(loc.pk)
        loc = self.s_or_i.fetch(loc)
        self.assertTrue(loc.pk)

        # Look up same row
        l2 = Location()
        self.remove_after_test.append(l2)
        for f in l2.query_fields:
            setattr(l2, f, getattr(loc, f, None))
        l2 = self.s_or_i.fetch(l2)
        self.assertEquals(l2.pk, loc.pk)
        self.assertEquals(l2.county, loc.county)

        # Confirm a missing field doesn't fetch the same row
        l3 = Location(country='USA')
        self.remove_after_test.append(l3)
        l3 = self.s_or_i.fetch(l3)
        self.assertNotEquals(l3.pk, loc.pk)

        # Look up different row, where different attribute is set
        # after construction
        l4 = Location(country='USA')
        self.remove_after_test.append(l4)
        l4.zip = '55303'
        l4 = self.s_or_i.fetch(l4)
        self.assertNotEquals(l3.pk, l4.pk)


def process_hammer(proc_no, lock):  # pragma: no cover (out of process)
    """The target used from several concurrent processes to hammer on
    the same set of database objects.  Intended to test syncronization
    problems with unique constraints, etc.

    """
    conn = db_connection(CONFIG_SECTION)
    s_or_i = SelectOrInsert(lock, conn.session)
    #print "enter proc_no %d" % proc_no
    "Loops over the same set 3 times - this reliably breaks w/o locks"
    for i in range(0, 3):
        for r in range(98100, 98110):
            loc = Location(zip=str(r))
            #print "%d fetch zip %d" % (proc_no, r)
            loc = s_or_i.fetch(loc)
            assert(loc)
    conn.disconnect()


class MultiProcessTest(unittest.TestCase):
    """Hammer with multiple processes making asynchronous, colliding
    requests"""

    def setUp(self):
        self.conn = db_connection(CONFIG_SECTION)

    def tearDown(self):
        self.conn.session.query(Location).delete()
        self.conn.session.commit()
        self.conn.disconnect()

    def testMultiProc(self):
        "Asynchronously fire multi procs on same set of objs"
        query = self.conn.session.query(Location)
        self.assertEquals(query.count(), 0)

        lock = Lock()
        procs = [Process(target=process_hammer, args=(e, lock)) for e
                 in range(3)]
        [p.start() for p in procs]
        [p.join() for p in procs]

        self.assertEquals(query.count(), 10)
