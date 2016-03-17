#! /usr/bin/env python

import sys
import sqlite3
import argparse

import jcd.common
import jcd.dao

class MinMax(object):

    StationsDayTable = "minmax_stations_day"

    def __init__(self, db, sample_schema, arguments):
        self._db = db
        self._sample_schema = sample_schema
        self._arguments = arguments
        assert self._db is not None
        assert self._sample_schema is not None
        assert self._arguments is not None
        self._create_tables_if_necessary()

    def _create_tables_if_necessary(self):
        if not self._db.has_table(self.StationsDayTable):
            self._create_stations_day_table()

    def _create_stations_day_table(self):
        if self._arguments.verbose:
            print "Creating table", self.StationsDayTable
        try:
            self._db.connection.execute(
                '''
                CREATE TABLE %s (
                date TEXT NOT NULL,
                contract_id INTEGER NOT NULL,
                station_number INTEGER NOT NULL,
                min_bikes INTEGER NOT NULL,
                max_bikes INTEGER NOT NULL,
                min_slots INTEGER NOT NULL,
                max_slots INTEGER NOT NULL,
                num_changes INTEGER NOT NULL,
                PRIMARY KEY (date, contract_id, station_number));
                ''' % self.StationsDayTable)
        except sqlite3.Error as error:
            print "%s: %s" % (type(error).__name__, error)
            raise jcd.common.JcdException(
                "Database error while creating table [%s]" % self.StationsDayTable)

    def _do_stations(self, date):
        try:
            self._db.connection.execute(
                '''
                INSERT OR REPLACE INTO %s
                SELECT ?,
                contract_id,
                station_number,
                MIN(available_bikes),
                MAX(available_bikes),
                MIN(available_bike_stands),
                MAX(available_bike_stands),
                COUNT(timestamp)
                FROM %s.%s
                GROUP BY contract_id, station_number
                ''' % (self.StationsDayTable, self._sample_schema, jcd.dao.ShortSamplesDAO.TableNameArchive),
                (date,))
        except sqlite3.Error as error:
            print "%s: %s" % (type(error).__name__, error)
            raise jcd.common.JcdException(
                "Database error while storing daily min max into table [%s]" % self.StationsDayTable)

    def run(self, date):
        self._do_stations(date)

class Activity(object):

    StationsDayTable = "activity_stations_day"

    def __init__(self, db, sample_schema, arguments):
        self._db = db
        self._sample_schema = sample_schema
        self._arguments = arguments
        assert self._db is not None
        assert self._sample_schema is not None
        assert self._arguments is not None
        self._create_tables_if_necessary()

    def _create_tables_if_necessary(self):
        if not self._db.has_table(self.StationsDayTable):
            self._create_stations_day_table()

    def _create_stations_day_table(self):
        if self._arguments.verbose:
            print "Creating table", self.StationsDayTable
        try:
            self._db.connection.execute(
                '''
                CREATE TABLE %s (
                date TEXT NOT NULL,
                contract_id INTEGER NOT NULL,
                station_number INTEGER NOT NULL,
                num_changes INTEGER NOT NULL,
                rank INTEGER,
                PRIMARY KEY (date, contract_id, station_number));
                ''' % self.StationsDayTable)
        except sqlite3.Error as error:
            print "%s: %s" % (type(error).__name__, error)
            raise jcd.common.JcdException(
                "Database error while creating table [%s]" % self.StationsDayTable)

    def _do_stations(self, date):
        try:
            self._db.connection.execute(
                '''
                INSERT OR REPLACE INTO %s
                    SELECT date(timestamp,'unixepoch') as date,
                        contract_id,
                        station_number,
                        COUNT(timestamp) as num_changes,
                        NULL
                    FROM %s.%s
                    WHERE date = ?
                    GROUP BY contract_id, station_number
                ''' % (self.StationsDayTable,
                       self._sample_schema,
                       jcd.dao.ShortSamplesDAO.TableNameArchive),
                (date,))
        except sqlite3.Error as error:
            print "%s: %s" % (type(error).__name__, error)
            raise jcd.common.JcdException(
                "Database error while storing daily min max into table [%s]" % self.StationsDayTable)

    def _get_stations(self, date):
        try:
            req = self._db.connection.execute(
                '''
                SELECT date, contract_id, station_number, num_changes, rank
                FROM %s
                WHERE date = ?
                ORDER BY num_changes DESC
                ''' % (self.StationsDayTable), (date,))
            while True:
                ranks = req.fetchmany(1000)
                if not ranks:
                    break
                for rank in ranks:
                    yield rank
        except sqlite3.Error as error:
            print "%s: %s" % (type(error).__name__, error)
            raise jcd.common.JcdException(
                "Database error while getting daily activity ranks")

    def _rank_stations(self, date):
        n_total = 0
        n_rank = 0
        last_value = None
        for rank in self._get_stations(date):
            d_rank = {
                "day": rank[0],
                "contract_id": rank[1],
                "station_number": rank[2],
                "events": rank[3],
                "rank": rank[4],
            }
            # one more sample done
            n_total += 1
            # first
            if last_value is None:
                n_rank = n_total
                last_value = rank[3]
            # not the same number of events
            if rank[3] != last_value:
                n_rank = n_total
                last_value = rank[3]
            # set rank
            d_rank["rank"] = n_rank
            print d_rank
            yield d_rank
        # TODO: what to do when there is nothing ?

    def _update_rank_stations(self, date):
        try:
            # update any existing contracts
            req = self._db.connection.executemany(
                '''
                UPDATE %s
                SET rank = :rank
                WHERE date = :day AND
                    contract_id = :contract_id AND
                    station_number = :station_number
                ''' % (self.StationsDayTable),
                self._rank_stations(date))
            # return number of inserted records
            print "updated", req.rowcount
            return req.rowcount
        except sqlite3.Error as error:
            print "%s: %s" % (type(error).__name__, error)
            raise jcd.common.JcdException("Database error while updating daily station activity rankings")

    def run(self, date):
        self._do_stations(date)
        self._update_rank_stations(date)

class App(object):

    def __init__(self, default_data_path, default_db_filename):
        # construct parser
        self._parser = argparse.ArgumentParser(
            description='Calculate min-max data from jcd stats')
        self._parser.add_argument(
            '--datadir',
            help='choose data folder (default: %s)' % default_data_path,
            default=default_data_path
        )
        self._parser.add_argument(
            '--dbname',
            help='choose db filename (default: %s)' % default_db_filename,
            default=default_db_filename
        )
        self._parser.add_argument(
            '--verbose', '-v',
            action='store_true',
            help='display operationnal informations'
        )
        self._parser.add_argument(
            'date',
            metavar='N',
            type=str,
            nargs='+',
            help='an integer for the accumulator')

    def run(self):
        # parse arguments
        arguments = self._parser.parse_args()
        with jcd.common.SqliteDB(arguments.dbname, arguments.datadir) as db_stats:
            for date in arguments.date:
                if arguments.verbose:
                    print "Processing", date
                # attach samples db
                schema = jcd.dao.ShortSamplesDAO.get_schema_name(date)
                filename = jcd.dao.ShortSamplesDAO.get_db_file_name(schema)
                db_stats.attach_database(filename, schema, arguments.datadir)
                # do processing
                MinMax(db_stats, schema, arguments).run(date)
                Activity(db_stats, schema, arguments).run(date)
                # detach samples db
                db_stats.detach_database(schema)

# main
if __name__ == '__main__':
    try:
        App("~/.jcd_v2", "stats.db").run()
    except KeyboardInterrupt:
        pass
