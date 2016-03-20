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
            self._create_table_stations_custom(self.StationsDayTable, "start_of_day")
        if not self._db.has_table(self.ContractsDayTable):
            self._create_table_contracts_custom(self.ContractsDayTable, "start_of_day")
        if not self._db.has_table(self.GlobalDayTable):
            self._create_global_day_table()

        if not self._db.has_table(self.StationsWeekTable):
            self._create_table_stations_custom(self.StationsWeekTable, "start_of_week")
        if not self._db.has_table(self.ContractsWeekTable):
            self._create_table_contracts_custom(self.ContractsWeekTable, "start_of_week")
        if not self._db.has_table(self.GlobalWeekTable):
            self._create_global_week_table()

        if not self._db.has_table(self.StationsMonthTable):
            self._create_table_stations_custom(self.StationsMonthTable, "start_of_month")
        if not self._db.has_table(self.ContractsMonthTable):
            self._create_table_contracts_custom(self.ContractsMonthTable, "start_of_month")
        if not self._db.has_table(self.GlobalMonthTable):
            self._create_global_month_table()

        if not self._db.has_table(self.StationsYearTable):
            self._create_table_stations_custom(self.StationsYearTable, "start_of_year")
        if not self._db.has_table(self.ContractsYearTable):
            self._create_table_contracts_custom(self.ContractsYearTable, "start_of_year")
        if not self._db.has_table(self.GlobalYearTable):
            self._create_global_year_table()

    def _create_table_stations_custom(self, table_name, time_key_name):
        if self._arguments.verbose:
            print "Creating table", table_name
        try:
            self._db.connection.execute(
                '''
                CREATE TABLE %s (
                %s INTEGER NOT NULL,
                contract_id INTEGER NOT NULL,
                station_number INTEGER NOT NULL,
                num_changes INTEGER NOT NULL,
                rank_contract INTEGER,
                rank_global INTEGER,
                PRIMARY KEY (%s, contract_id, station_number)
                ) WITHOUT ROWID;
                ''' % (table_name, time_key_name, time_key_name))
        except sqlite3.Error as error:
            print "%s: %s" % (type(error).__name__, error)
            raise jcd.common.JcdException(
                "Database error while creating table [%s]" % table_name)

    def _create_table_contracts_custom(self, table_name, time_key_name):
        if self._arguments.verbose:
            print "Creating table", table_name
        try:
            self._db.connection.execute(
                '''
                CREATE TABLE %s (
                %s INTEGER NOT NULL,
                contract_id INTEGER NOT NULL,
                num_changes INTEGER NOT NULL,
                rank_global INTEGER,
                PRIMARY KEY (%s, contract_id)
                ) WITHOUT ROWID;
                ''' % (table_name, time_key_name, time_key_name))
        except sqlite3.Error as error:
            print "%s: %s" % (type(error).__name__, error)
            raise jcd.common.JcdException(
                "Database error while creating table [%s]" % table_name)

    def _create_global_day_table(self):
        if self._arguments.verbose:
            print "Creating table", self.GlobalDayTable
        try:
            self._db.connection.execute(
                '''
                CREATE TABLE %s (
                start_of_day INTEGER NOT NULL,
                num_changes INTEGER NOT NULL,
                PRIMARY KEY (start_of_day)
                ) WITHOUT ROWID;
                ''' % self.GlobalDayTable)
        except sqlite3.Error as error:
            print "%s: %s" % (type(error).__name__, error)
            raise jcd.common.JcdException(
                "Database error while creating table [%s]" % self.GlobalDayTable)

    def _create_global_week_table(self):
        if self._arguments.verbose:
            print "Creating table", self.GlobalWeekTable
        try:
            self._db.connection.execute(
                '''
                CREATE TABLE %s (
                start_of_week INTEGER NOT NULL,
                num_changes INTEGER NOT NULL,
                PRIMARY KEY (start_of_week)
                ) WITHOUT ROWID;
                ''' % self.GlobalWeekTable)
        except sqlite3.Error as error:
            print "%s: %s" % (type(error).__name__, error)
            raise jcd.common.JcdException(
                "Database error while creating table [%s]" % self.GlobalWeekTable)

    def _create_global_month_table(self):
        if self._arguments.verbose:
            print "Creating table", self.GlobalMonthTable
        try:
            self._db.connection.execute(
                '''
                CREATE TABLE %s (
                start_of_month INTEGER NOT NULL,
                num_changes INTEGER NOT NULL,
                PRIMARY KEY (start_of_month)
                ) WITHOUT ROWID;
                ''' % self.GlobalMonthTable)
        except sqlite3.Error as error:
            print "%s: %s" % (type(error).__name__, error)
            raise jcd.common.JcdException(
                "Database error while creating table [%s]" % self.GlobalMonthTable)

    def _create_global_year_table(self):
        if self._arguments.verbose:
            print "Creating table", self.GlobalYearTable
        try:
            self._db.connection.execute(
                '''
                CREATE TABLE %s (
                start_of_year INTEGER NOT NULL,
                num_changes INTEGER NOT NULL,
                PRIMARY KEY (start_of_year)
                ) WITHOUT ROWID;
                ''' % self.GlobalYearTable)
        except sqlite3.Error as error:
            print "%s: %s" % (type(error).__name__, error)
            raise jcd.common.JcdException(
                "Database error while creating table [%s]" % self.GlobalYearTable)

    def _do_activity_stations_day(self, date):
        params = {"date": date}
        try:
            self._db.connection.execute(
                '''
                INSERT OR REPLACE INTO %s
                    SELECT strftime('%%s',
                            timestamp,
                            'unixepoch',
                            'start of day'),
                        contract_id,
                        station_number,
                        COUNT(timestamp),
                        NULL,
                        NULL
                    FROM %s.%s
                    WHERE timestamp BETWEEN
                        strftime('%%s', :date, 'start of day') AND
                        strftime('%%s', :date, 'start of day', '+1 day') - 1
                    GROUP BY contract_id, station_number
                ''' % (self.StationsDayTable,
                       self._sample_schema,
                       jcd.dao.ShortSamplesDAO.TableNameArchive),
                params)
        except sqlite3.Error as error:
            print "%s: %s" % (type(error).__name__, error)
            raise jcd.common.JcdException(
                "Database error while storing daily stations activity into table [%s]" % self.StationsDayTable)

    def _do_activity_contracts_day(self, date):
        try:
            self._db.connection.execute(
                '''
                INSERT OR REPLACE INTO %s
                    SELECT start_of_day,
                        contract_id,
                        SUM(num_changes),
                        NULL
                    FROM %s
                    WHERE start_of_day = strftime('%%s', ?, 'start of day')
                    GROUP BY contract_id
                ''' % (self.ContractsDayTable,
                       self.StationsDayTable),
                (date,))
        except sqlite3.Error as error:
            print "%s: %s" % (type(error).__name__, error)
            raise jcd.common.JcdException(
                "Database error while storing daily contracts activity into table [%s]" % self.ContractsDayTable)

    def _do_activity_global_day(self, date):
        try:
            self._db.connection.execute(
                '''
                INSERT OR REPLACE INTO %s
                    SELECT start_of_day,
                        SUM(num_changes) as num_changes
                    FROM %s
                    WHERE start_of_day = strftime('%%s', ?, 'start of day')
                    GROUP BY start_of_day
                ''' % (self.GlobalDayTable,
                       self.ContractsDayTable),
                (date,))
        except sqlite3.Error as error:
            print "%s: %s" % (type(error).__name__, error)
            raise jcd.common.JcdException(
                "Database error while storing daily global activity into table [%s]" % self.GlobalDayTable)

    def _do_activity_stations_week(self, date):
        params = {"date": date}
        try:
            self._db.connection.execute(
                '''
                INSERT OR REPLACE INTO %s
                    SELECT start_of_day - strftime('%%w', start_of_day, 'unixepoch', '-1 day') * 86400,
                        contract_id,
                        station_number,
                        SUM(num_changes),
                        NULL,
                        NULL
                    FROM %s
                    WHERE start_of_day BETWEEN
                        strftime('%%s', :date, '-' || strftime('%%w', :date, '-1 day') || ' days', 'start of day') AND
                        strftime('%%s', :date, '-' || strftime('%%w', :date, '-1 day') || ' days', 'start of day', '+7 days') - 1
                    GROUP BY contract_id, station_number
                ''' % (self.StationsWeekTable,
                       self.StationsDayTable),
                params)
        except sqlite3.Error as error:
            print "%s: %s" % (type(error).__name__, error)
            raise jcd.common.JcdException(
                "Database error while storing weekly stations activity into table [%s]" % self.StationsWeekTable)

    def _do_activity_contracts_week(self, date):
        params = {"date": date}
        try:
            self._db.connection.execute(
                '''
                INSERT OR REPLACE INTO %s
                    SELECT start_of_week,
                        contract_id,
                        SUM(num_changes),
                        NULL
                    FROM %s
                    WHERE start_of_week = strftime('%%s', :date, '-' || strftime('%%w', :date, '-1 day') || ' days', 'start of day')
                    GROUP BY contract_id
                ''' % (self.ContractsWeekTable,
                       self.StationsWeekTable),
                params)
        except sqlite3.Error as error:
            print "%s: %s" % (type(error).__name__, error)
            raise jcd.common.JcdException(
                "Database error while storing weekly contracts activity into table [%s]" % self.ContractsWeekTable)

    def _do_activity_global_week(self, date):
        params = {"date": date}
        try:
            self._db.connection.execute(
                '''
                INSERT OR REPLACE INTO %s
                    SELECT start_of_week,
                        SUM(num_changes) as num_changes
                    FROM %s
                    WHERE start_of_week = strftime('%%s', :date, '-' || strftime('%%w', :date, '-1 day') || ' days', 'start of day')
                    GROUP BY start_of_week
                ''' % (self.GlobalWeekTable,
                       self.ContractsWeekTable),
                params)
        except sqlite3.Error as error:
            print "%s: %s" % (type(error).__name__, error)
            raise jcd.common.JcdException(
                "Database error while storing weekly global activity into table [%s]" % self.GlobalWeekTable)

    def _do_activity_stations_month(self, date):

        params = {"date": date}
        try:
            self._db.connection.execute(
                '''
                INSERT OR REPLACE INTO %s
                    SELECT strftime('%%s',
                            start_of_day,
                            'unixepoch',
                            'start of month'),
                        contract_id,
                        station_number,
                        SUM(num_changes),
                        NULL,
                        NULL
                    FROM %s
                    WHERE start_of_day BETWEEN
                        strftime('%%s', :date, 'start of month') AND
                        strftime('%%s', :date, 'start of month', '+1 month') - 1
                    GROUP BY contract_id, station_number
                ''' % (self.StationsMonthTable,
                       self.StationsDayTable),
                params)
        except sqlite3.Error as error:
            print "%s: %s" % (type(error).__name__, error)
            raise jcd.common.JcdException(
                "Database error while storing monthly stations activity into table [%s]" % self.StationsMonthTable)

    def _do_activity_contracts_month(self, date):
        try:
            self._db.connection.execute(
                '''
                INSERT OR REPLACE INTO %s
                    SELECT start_of_month,
                        contract_id,
                        SUM(num_changes),
                        NULL
                    FROM %s
                    WHERE start_of_month = strftime('%%s', ?, 'start of month')
                    GROUP BY contract_id
                ''' % (self.ContractsMonthTable,
                       self.StationsMonthTable),
                (date,))
        except sqlite3.Error as error:
            print "%s: %s" % (type(error).__name__, error)
            raise jcd.common.JcdException(
                "Database error while storing monthly contracts activity into table [%s]" % self.ContractsMonthTable)

    def _do_activity_global_month(self, date):
        try:
            self._db.connection.execute(
                '''
                INSERT OR REPLACE INTO %s
                    SELECT start_of_month,
                        SUM(num_changes) as num_changes
                    FROM %s
                    WHERE start_of_month = strftime('%%s', ?, 'start of month')
                    GROUP BY start_of_month
                ''' % (self.GlobalMonthTable,
                       self.ContractsMonthTable),
                (date,))
        except sqlite3.Error as error:
            print "%s: %s" % (type(error).__name__, error)
            raise jcd.common.JcdException(
                "Database error while storing monthly global activity into table [%s]" % self.GlobalMonthTable)

    def _do_activity_stations_year(self, date):
        params = {"date": date}
        try:
            self._db.connection.execute(
                '''
                INSERT OR REPLACE INTO %s
                    SELECT strftime('%%s',
                            start_of_month,
                            'unixepoch',
                            'start of year'),
                        contract_id,
                        station_number,
                        SUM(num_changes),
                        NULL,
                        NULL
                    FROM %s
                    WHERE start_of_month BETWEEN
                        strftime('%%s', :date, 'start of year') AND
                        strftime('%%s', :date, 'start of year', '+1 year') - 1
                    GROUP BY contract_id, station_number
                ''' % (self.StationsYearTable,
                       self.StationsMonthTable),
                params)
        except sqlite3.Error as error:
            print "%s: %s" % (type(error).__name__, error)
            raise jcd.common.JcdException(
                "Database error while storing yearly stations activity into table [%s]" % self.StationsYearTable)

    def _do_activity_contracts_year(self, date):
        try:
            self._db.connection.execute(
                '''
                INSERT OR REPLACE INTO %s
                    SELECT start_of_year,
                        contract_id,
                        SUM(num_changes),
                        NULL
                    FROM %s
                    WHERE start_of_year = strftime('%%s', ?, 'start of year')
                    GROUP BY contract_id
                ''' % (self.ContractsYearTable,
                       self.StationsYearTable),
                (date,))
        except sqlite3.Error as error:
            print "%s: %s" % (type(error).__name__, error)
            raise jcd.common.JcdException(
                "Database error while storing yearly contracts activity into table [%s]" % self.ContractsYearTable)

    def _do_activity_global_year(self, date):
        try:
            self._db.connection.execute(
                '''
                INSERT OR REPLACE INTO %s
                    SELECT start_of_year,
                        SUM(num_changes) as num_changes
                    FROM %s
                    WHERE start_of_year = strftime('%%s', ?, 'start of year')
                    GROUP BY start_of_year
                ''' % (self.GlobalYearTable,
                       self.ContractsYearTable),
                (date,))
        except sqlite3.Error as error:
            print "%s: %s" % (type(error).__name__, error)
            raise jcd.common.JcdException(
                "Database error while storing yearly global activity into table [%s]" % self.GlobalYearTable)

    def _stations_day_get(self, date):
        try:
            req = self._db.connection.execute(
                '''
                SELECT start_of_day,
                    contract_id,
                    station_number,
                    num_changes,
                    rank_contract,
                    rank_global
                FROM %s
                WHERE start_of_day = strftime('%%s', ?)
                ORDER BY num_changes DESC
                ''' % (self.StationsDayTable), (date,))
            while True:
                ranks = req.fetchmany(1000)
                if not ranks:
                    break
                for rank in ranks:
                    d_rank = {
                        "start_of_day": rank[0],
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
                WHERE start_of_day = :start_of_day AND
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
        self._do_activity_stations_day(date)
        self._do_activity_stations_week(date)
        self._do_activity_stations_month(date)
        self._do_activity_stations_year(date)
        self._do_activity_contracts_day(date)
        self._do_activity_contracts_week(date)
        self._do_activity_contracts_month(date)
        self._do_activity_contracts_year(date)
        self._do_activity_global_day(date)
        self._do_activity_global_week(date)
        self._do_activity_global_month(date)
        self._do_activity_global_year(date)
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
