import pandas as pd
import pickle
import requests
import datetime
import os.path
from os import path
from threading import Timer

class Dataset:
    def __init__(self, name, sub_package, refresh=0, cache=False, force_load=False):
        """Create a new database object from the WPRDC

        Arguements:
        name -- Name of databse in wprdc ckan representation
                Also appears in url as last section,
                For example: https://data.wprdc.org/dataset/parking-transactions -> 'parking-transactions'
        sub_package -- Name of the portion of the dataset, for example '2011 Crash Data'
        refresh -- refresh rate for database. How often to check for updates.
                    Setting to 0 disables checks. This is the default value
        cache -- to save the data locally in files that persist between program runs.
                    Use to decrease loading time at the expense of disk space. If a cache
                    file is not found the database is reloaded from the internet.
                    Setting to false prevents the use and automatic creation of a cache file.
                    Setting to true results in the automatic creation of a cache file
        force_load -- Force data reload. Only used if cache=True. Forces initial reload of cache file.
        """
        self.name = name
        self.sub_package = sub_package
        self.filename = name + '--' + self.sub_package + '.pyc'
        self.refresh = refresh
        if self.refresh: Timer(self.refresh, self.check_refresh).start()
        self.cache = cache
        if self.cache and force_load: self.reload()
        elif self.cache:
            try: self.load_from_file()
            except: self.reload()
        else: self.reload()

    def check_refresh(self):
        print('Check refresh')
        resource = self.load_metadata()
        time = datetime.datetime.strptime(resource['last_modified'][:19], '%Y-%m-%dT%H:%M:%S')
        if time.date() >  self.last_updated.date(): self.reload()
        if self.refresh: Timer(self.refresh, self.check_refresh).start()

    def save_to_file(self, filename=''):
        if filename: self.filename = filename
        file_obj = {
                'last_updated': datetime.datetime.now(),
                'data': self.data
                }
        pickle.dump(file_obj, open(self.filename, 'wb'))

    def load_from_file(self, filename=''):
        if filename: self.filename = filename
        file_obj = pickle.load( open(self.filename, 'rb') )
        self.last_updated = file_obj.last_updated
        self.data = file_obj.data

    def reload(self):
        resource = self.load_metadata()
        if resource['format'] != 'CSV': raise Exception('Unrecognized data format')
        print(resource['url'])
        self.data = pd.read_csv(resource['url'], parse_dates=True)
        self.last_updated = datetime.datetime.now()
        if self.cache: self.save_to_file()

    def load_metadata(self):
        res = requests.get(self.get_url())
        resources = res.json()['result']['resources']
        for resource in resources:
            if resource['name'] == self.sub_package: return resource
        raise Exception('Sub package not found')

    def get_url(self):
        return 'https://data.wprdc.org/api/3/action/package_show?id=' + self.name
