from pprint import pprint
import pickle
import os.path
from dateutil.tz import tzlocal, tzutc
from datetime import datetime,timedelta
class emailCache:
    def __init__(self,
                 address,
                 datadir='./metacache', service=None):
        self.address = address
        self.service = service
        self.datadir = datadir
        assert self.service != None, "Service is required."

        self.cache_file = os.path.join(
            datadir,
            address+'.cache')
        self.cache = self._load()

    def _load(self):
        os.makedirs(self.datadir, exist_ok=True)
        if os.path.exists(self.cache_file):
            with open(self.cache_file, 'rb') as cf:
                return pickle.load(cf)
        return {}
    def _save(self):
        os.makedirs(self.datadir, exist_ok=True)
        with open(self.cache_file, 'wb') as cf:
            pickle.dump(self.cache, cf)
    def __enter__(self):
        return self
    def __exit__(self, type, value, traceback):
        self._save()
    def update(self, id, value):
        "update a cached item"
        self.cache[id] = {
            'when': datetime.now(tz=tzutc()),
            'value': value
        }
        return value
    def getMetadata(self, id):
        """get the id from cache, if not in cache,
        get it from gmail (service) and add it to the cache
        """
        meta = None
        now = datetime.now(tz=tzutc())
        maxage = timedelta(hours=20)

        if id in self.cache and now - self.cache[id]['when'] < maxage :
            meta = self.cache[id]['value']
        else:
            meta = self.service.get(
                userId='me', id=id,
                format='metadata').execute()
            self.cache[id] = {
                'when': now,
                'value': meta
            }

        return meta
