from sqlalchemy.orm.exc import MultipleResultsFound, NoResultFound


class SelectOrInsert(object):
    """Syncronized class makes 'SELECT or INSERT' an atomic operation

    With multiple processes asynchronously adding data to the star
    schema, there's a fair chance more than one will request the same
    non-existing row from a dimension table.  This class handles the
    syncronization, as well as provides a single call to insert or
    return the existing row.

    To provide efficient access, this encapsulates a cache as well as
    a locking mechanism to avoid collisions.

    """
    def __init__(self, lock, session):
        self._lock = lock
        self._session = session

    def cache_lookup(self, obj):
        # TODO
        return None

    def cache_insert(self, obj):
        # TODO
        pass

    def fetch(self, obj):
        # First hit the cache - return a match if found
        ret = self.cache_lookup(obj)
        if ret:  # pragma: no cover  (Not implemented)
            return ret
        # Otherwise, need to insert in db and add to the cache
        try:
            self._lock.acquire()
            d = dict()
            for f in obj.query_fields:
                d[f] = getattr(obj, f, None)
            query = self._session.query(obj.__class__).\
                filter_by(**d)
            try:
                return query.one()
            except MultipleResultsFound:  # pragma: no cover
                raise  # reflect this situation up
            except NoResultFound:
                # Time to add it
                self._session.add(obj)
                self._session.commit()
                self.cache_insert(obj)
                return obj
        finally:
            self._lock.release()
