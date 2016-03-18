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
                PRIMARY KEY (date, contract_id, station_number)
                ) WITHOUT ROWID;
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
    ContractsDayTable = "activity_contracts_day"
    GlobalDayTable = "activity_global_day"

    StationsWeekTable = "activity_stations_week"
    ContractsWeekTable = "activity_contracts_week"
    GlobalWeekTable = "activity_global_week"

    StationsMonthTable = "activity_stations_month"
    ContractsMonthTable = "activity_contracts_month"
    GlobalMonthTable = "activity_global_month"

    StationsYearTable = "activity_stations_year"
    ContractsYearTable = "activity_contracts_year"
    GlobalYearTable = "activity_global_year"

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
        if not self._db.has_table(self.ContractsDayTable):
            self._create_contracts_day_table()
        if not self._db.has_table(self.GlobalDayTable):
            self._create_global_day_table()

        if not self._db.has_table(self.StationsWeekTable):
            self._create_stations_week_table()
        if not self._db.has_table(self.ContractsWeekTable):
            self._create_contracts_week_table()
        if not self._db.has_table(self.GlobalWeekTable):
            self._create_global_week_table()

        if not self._db.has_table(self.StationsMonthTable):
            self._create_stations_month_table()
        if not self._db.has_table(self.ContractsMonthTable):
            self._create_contracts_month_table()
        if not self._db.has_table(self.GlobalMonthTable):
            self._create_global_month_table()

        if not self._db.has_table(self.StationsYearTable):
            self._create_stations_year_table()
        if not self._db.has_table(self.ContractsYearTable):
            self._create_contracts_year_table()
        if not self._db.has_table(self.GlobalYearTable):
            self._create_global_year_table()

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
                rank_contract INTEGER,
                rank_global INTEGER,
                PRIMARY KEY (date, contract_id, station_number)
                ) WITHOUT ROWID;
                ''' % self.StationsDayTable)
        except sqlite3.Error as error:
            print "%s: %s" % (type(error).__name__, error)
            raise jcd.common.JcdException(
                "Database error while creating table [%s]" % self.StationsDayTable)

    def _create_contracts_day_table(self):
        if self._arguments.verbose:
            print "Creating table", self.ContractsDayTable
        try:
            self._db.connection.execute(
                '''
                CREATE TABLE %s (
                date TEXT NOT NULL,
                contract_id INTEGER NOT NULL,
                num_changes INTEGER NOT NULL,
                rank_global INTEGER,
                PRIMARY KEY (date, contract_id)
                ) WITHOUT ROWID;
                ''' % self.ContractsDayTable)
        except sqlite3.Error as error:
            print "%s: %s" % (type(error).__name__, error)
            raise jcd.common.JcdException(
                "Database error while creating table [%s]" % self.ContractsDayTable)

    def _create_global_day_table(self):
        if self._arguments.verbose:
            print "Creating table", self.GlobalDayTable
        try:
            self._db.connection.execute(
                '''
                CREATE TABLE %s (
                date TEXT NOT NULL,
                num_changes INTEGER NOT NULL,
                PRIMARY KEY (date)
                ) WITHOUT ROWID;
                ''' % self.GlobalDayTable)
        except sqlite3.Error as error:
            print "%s: %s" % (type(error).__name__, error)
            raise jcd.common.JcdException(
                "Database error while creating table [%s]" % self.GlobalDayTable)

    def _create_stations_week_table(self):
        if self._arguments.verbose:
            print "Creating table", self.StationsWeekTable
        try:
            self._db.connection.execute(
                '''
                CREATE TABLE %s (
                year_week TEXT NOT NULL,
                contract_id INTEGER NOT NULL,
                station_number INTEGER NOT NULL,
                num_changes INTEGER NOT NULL,
                rank_contract INTEGER,
                rank_global INTEGER,
                PRIMARY KEY (year_week, contract_id, station_number)
                ) WITHOUT ROWID;
                ''' % self.StationsWeekTable)
        except sqlite3.Error as error:
            print "%s: %s" % (type(error).__name__, error)
            raise jcd.common.JcdException(
                "Database error while creating table [%s]" % self.StationsWeekTable)

    def _create_contracts_week_table(self):
        if self._arguments.verbose:
            print "Creating table", self.ContractsWeekTable
        try:
            self._db.connection.execute(
                '''
                CREATE TABLE %s (
                year_week TEXT NOT NULL,
                contract_id INTEGER NOT NULL,
                num_changes INTEGER NOT NULL,
                rank_global INTEGER,
                PRIMARY KEY (year_week, contract_id)
                ) WITHOUT ROWID;
                ''' % self.ContractsWeekTable)
        except sqlite3.Error as error:
            print "%s: %s" % (type(error).__name__, error)
            raise jcd.common.JcdException(
                "Database error while creating table [%s]" % self.ContractsWeekTable)

    def _create_global_week_table(self):
        if self._arguments.verbose:
            print "Creating table", self.GlobalWeekTable
        try:
            self._db.connection.execute(
                '''
                CREATE TABLE %s (
                year_week TEXT NOT NULL,
                num_changes INTEGER NOT NULL,
                PRIMARY KEY (year_week)
                ) WITHOUT ROWID;
                ''' % self.GlobalWeekTable)
        except sqlite3.Error as error:
            print "%s: %s" % (type(error).__name__, error)
            raise jcd.common.JcdException(
                "Database error while creating table [%s]" % self.GlobalWeekTable)

    def _create_stations_month_table(self):
        if self._arguments.verbose:
            print "Creating table", self.StationsMonthTable
        try:
            self._db.connection.execute(
                '''
                CREATE TABLE %s (
                year_month TEXT NOT NULL,
                contract_id INTEGER NOT NULL,
                station_number INTEGER NOT NULL,
                num_changes INTEGER NOT NULL,
                rank_contract INTEGER,
                rank_global INTEGER,
                PRIMARY KEY (year_month, contract_id, station_number)
                ) WITHOUT ROWID;
                ''' % self.StationsMonthTable)
        except sqlite3.Error as error:
            print "%s: %s" % (type(error).__name__, error)
            raise jcd.common.JcdException(
                "Database error while creating table [%s]" % self.StationsMonthTable)

    def _create_contracts_month_table(self):
        if self._arguments.verbose:
            print "Creating table", self.ContractsMonthTable
        try:
            self._db.connection.execute(
                '''
                CREATE TABLE %s (
                year_month TEXT NOT NULL,
                contract_id INTEGER NOT NULL,
                num_changes INTEGER NOT NULL,
                rank_global INTEGER,
                PRIMARY KEY (year_month, contract_id)
                ) WITHOUT ROWID;
                ''' % self.ContractsMonthTable)
        except sqlite3.Error as error:
            print "%s: %s" % (type(error).__name__, error)
            raise jcd.common.JcdException(
                "Database error while creating table [%s]" % self.ContractsMonthTable)

    def _create_global_month_table(self):
        if self._arguments.verbose:
            print "Creating table", self.GlobalMonthTable
        try:
            self._db.connection.execute(
                '''
                CREATE TABLE %s (
                year_month TEXT NOT NULL,
                num_changes INTEGER NOT NULL,
                PRIMARY KEY (year_month)
                ) WITHOUT ROWID;
                ''' % self.GlobalMonthTable)
        except sqlite3.Error as error:
            print "%s: %s" % (type(error).__name__, error)
            raise jcd.common.JcdException(
                "Database error while creating table [%s]" % self.GlobalMonthTable)

    def _create_stations_year_table(self):
        if self._arguments.verbose:
            print "Creating table", self.StationsYearTable
        try:
            self._db.connection.execute(
                '''
                CREATE TABLE %s (
                year TEXT NOT NULL,
                contract_id INTEGER NOT NULL,
                station_number INTEGER NOT NULL,
                num_changes INTEGER NOT NULL,
                rank_contract INTEGER,
                rank_global INTEGER,
                PRIMARY KEY (year, contract_id, station_number)
                ) WITHOUT ROWID;
                ''' % self.StationsYearTable)
        except sqlite3.Error as error:
            print "%s: %s" % (type(error).__name__, error)
            raise jcd.common.JcdException(
                "Database error while creating table [%s]" % self.StationsYearTable)

    def _create_contracts_year_table(self):
        if self._arguments.verbose:
            print "Creating table", self.ContractsYearTable
        try:
            self._db.connection.execute(
                '''
                CREATE TABLE %s (
                year TEXT NOT NULL,
                contract_id INTEGER NOT NULL,
                num_changes INTEGER NOT NULL,
                rank_global INTEGER,
                PRIMARY KEY (year, contract_id)
                ) WITHOUT ROWID;
                ''' % self.ContractsYearTable)
        except sqlite3.Error as error:
            print "%s: %s" % (type(error).__name__, error)
            raise jcd.common.JcdException(
                "Database error while creating table [%s]" % self.ContractsYearTable)

    def _create_global_year_table(self):
        if self._arguments.verbose:
            print "Creating table", self.GlobalYearTable
        try:
            self._db.connection.execute(
                '''
                CREATE TABLE %s (
                year TEXT NOT NULL,
                num_changes INTEGER NOT NULL,
                PRIMARY KEY (year)
                ) WITHOUT ROWID;
                ''' % self.GlobalYearTable)
        except sqlite3.Error as error:
            print "%s: %s" % (type(error).__name__, error)
            raise jcd.common.JcdException(
                "Database error while creating table [%s]" % self.GlobalYearTable)

    def _do_activity_stations(self, date):
        try:
            self._db.connection.execute(
                '''
                INSERT OR REPLACE INTO %s
                    SELECT date(timestamp,'unixepoch') as date,
                        contract_id,
                        station_number,
                        COUNT(timestamp) as num_changes,
                        NULL,
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
                "Database error while storing daily stations activity into table [%s]" % self.StationsDayTable)

    def _do_activity_contracts(self, date):
        try:
            self._db.connection.execute(
                '''
                INSERT OR REPLACE INTO %s
                    SELECT date,
                        contract_id,
                        SUM(num_changes) as num_changes,
                        NULL
                    FROM %s
                    WHERE date = ?
                    GROUP BY contract_id
                ''' % (self.ContractsDayTable,
                       self.StationsDayTable),
                (date,))
        except sqlite3.Error as error:
            print "%s: %s" % (type(error).__name__, error)
            raise jcd.common.JcdException(
                "Database error while storing daily contracts activity into table [%s]" % self.ContractsDayTable)

    def _do_activity_global(self, date):
        try:
            self._db.connection.execute(
                '''
                INSERT OR REPLACE INTO %s
                    SELECT date,
                        SUM(num_changes) as num_changes
                    FROM %s
                    WHERE date = ?
                    GROUP BY date
                ''' % (self.GlobalDayTable,
                       self.ContractsDayTable),
                (date,))
        except sqlite3.Error as error:
            print "%s: %s" % (type(error).__name__, error)
            raise jcd.common.JcdException(
                "Database error while storing daily global activity into table [%s]" % self.GlobalDayTable)

    def _stations_day_get(self, date):
        try:
            req = self._db.connection.execute(
                '''
                SELECT date,
                    contract_id,
                    station_number,
                    num_changes,
                    rank_contract,
                    rank_global
                FROM %s
                WHERE date = ?
                ORDER BY num_changes DESC
                ''' % (self.StationsDayTable), (date,))
            while True:
                ranks = req.fetchmany(1000)
                if not ranks:
                    break
                for rank in ranks:
                    d_rank = {
                        "date": rank[0],
                        "contract_id": rank[1],
                        "station_number": rank[2],
                        "num_changes": rank[3],
                        "rank_contract": rank[4],
                        "rank_global": rank[5],
                    }
                    yield d_rank
        except sqlite3.Error as error:
            print "%s: %s" % (type(error).__name__, error)
            raise jcd.common.JcdException(
                "Database error while getting daily activity ranks")

    def _stations_day_compute_ranks(self, date):

        contracts = {}
        global_infos = {
            "n_total": None,
            "n_rank": None,
            "n_last": None
        }

        for rank in self._stations_day_get(date):

            ## calculate contract ranks
            # first
            if rank["contract_id"] not in contracts:
                contracts[rank["contract_id"]] = {
                    "c_total": 0,
                    "c_rank": 1,
                    "c_last": rank["num_changes"]
                }
            # quicker lookip
            contract_infos = contracts[rank["contract_id"]]
            # sample done for contract
            contract_infos["c_total"] += 1
            # not the same number of events
            if rank["num_changes"] != contract_infos["c_last"]:
                contract_infos["c_rank"] = contract_infos["c_total"]
                contract_infos["c_last"] = rank["num_changes"]
            # set rank
            rank["rank_contract"] = contract_infos["c_rank"]

            ## calculate global ranks
            # first
            if global_infos["n_last"] is None:
                global_infos["n_total"] = 0
                global_infos["n_rank"] = 1
                global_infos["n_last"] = rank["num_changes"]
            # sample done globally
            global_infos["n_total"] += 1
            # not the same number of events
            if rank["num_changes"] != global_infos["n_last"]:
                global_infos["n_rank"] = global_infos["n_total"]
                global_infos["n_last"] = rank["num_changes"]
            # set rank
            rank["rank_global"] = global_infos["n_rank"]

            ## return value
            yield rank

        # TODO: what to do when there is nothing ?

    def _stations_day_update_ranks(self, date):
        try:
            # update any existing contracts
            req = self._db.connection.executemany(
                '''
                UPDATE %s
                SET rank_global = :rank_global,
                    rank_contract = :rank_contract
                WHERE date = :date AND
                    contract_id = :contract_id AND
                    station_number = :station_number
                ''' % (self.StationsDayTable),
                self._stations_day_compute_ranks(date))
            # return number of inserted records
            return req.rowcount
        except sqlite3.Error as error:
            print "%s: %s" % (type(error).__name__, error)
            raise jcd.common.JcdException("Database error while updating daily station activity rankings")

    def run(self, date):
        self._do_activity_stations(date)
        self._do_activity_contracts(date)
        self._do_activity_global(date)
        self._stations_day_update_ranks(date)

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
            metavar='date',
            type=str,
            nargs='+',
            help='a date for which to build stats')

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
