#! /usr/bin/env python

import sys
import sqlite3
import argparse

import jcd.common
import jcd.dao

class MinMax(object):

    StationsDayTable = "minmax_stations_day"
    ContractsDayTable = "minmax_contracts_day"
    GlobalsDayTable = "minmax_global_day"

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
        if not self._db.has_table(self.GlobalsDayTable):
            self._create_globals_day_table()

    def _create_stations_day_table(self):
        if self._arguments.verbose:
            print "Creating table", self.StationsDayTable
        self._db.execute_single(
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
            ''' % self.StationsDayTable,
            None,
            "Database error while creating table [%s]" % self.StationsDayTable)

    def _create_contracts_day_table(self):
        if self._arguments.verbose:
            print "Creating table", self.ContractsDayTable
        self._db.execute_single(
            '''
            CREATE TABLE %s (
                start_of_day INTEGER NOT NULL,
                contract_id INTEGER NOT NULL,
                min_bikes INTEGER NOT NULL,
                max_bikes INTEGER NOT NULL,
                PRIMARY KEY (start_of_day, contract_id)
            ) WITHOUT ROWID;
            ''' % self.ContractsDayTable,
            None,
            "Database error while creating table [%s]" % self.ContractsDayTable)

    def _create_globals_day_table(self):
        if self._arguments.verbose:
            print "Creating table", self.GlobalsDayTable
        self._db.execute_single(
            '''
            CREATE TABLE %s (
                start_of_day INTEGER NOT NULL,
                min_bikes INTEGER NOT NULL,
                max_bikes INTEGER NOT NULL,
                PRIMARY KEY (start_of_day)
            ) WITHOUT ROWID;
            ''' % self.GlobalsDayTable,
            None,
            "Database error while creating table [%s]" % self.GlobalsDayTable)

    def _get_operation_samples(self, operation):
        return self._db.execute_fetch_generator(
            '''
            SELECT %s(timestamp) AS timestamp,
                contract_id,
                station_number,
                available_bikes,
                available_bike_stands
            FROM %s.%s
            GROUP BY contract_id, station_number
            ''' % (operation,
                   self._sample_schema,
                   jcd.dao.ShortSamplesDAO.TableNameArchive),
            None,
            "Database error while getting boundary samples",
            True)

    def _get_first_samples(self):
        return self._get_operation_samples("min")

    def _get_last_samples(self):
        return self._get_operation_samples("max")

    def _do_stations(self, date):
        if self._arguments.verbose:
            print "Update table", self.StationsDayTable, "for", date,
        inserted = self._db.execute_single(
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
            ''' % (self.StationsDayTable,
                   self._sample_schema,
                   jcd.dao.ShortSamplesDAO.TableNameArchive),
            (date,),
            "Database error while storing daily station min max into table [%s]" % self.StationsDayTable)
        if self._arguments.verbose:
            print "... %i records" % inserted
        return inserted

    def _do_contracts(self, date):
        if self._arguments.verbose:
            print "Update table", self.ContractsDayTable, "for", date,
        contracts = {}
        # initialize with first sample
        samples = self._get_first_samples()
        for sample in samples:
            if sample["contract_id"] not in contracts:
                contracts[sample["contract_id"]] = {
                    "stations": {},
                    # used only for db storage query
                    "date": date,
                    "contract_id": sample["contract_id"]
                }
            stations = contracts[sample["contract_id"]]["stations"]
            stations[sample["station_number"]] = sample["available_bikes"]
        for contract in contracts.itervalues():
            sum_bikes = sum(contract["stations"].itervalues())
            contract["cur"] = sum_bikes
            contract["min"] = sum_bikes
            contract["max"] = sum_bikes
        # read every sample in chronological
        samples = self._db.execute_fetch_generator(
            '''
            SELECT contract_id,
                station_number,
                available_bikes
            FROM %s.%s
            ORDER BY timestamp ASC
            ''' % (self._sample_schema,
                jcd.dao.ShortSamplesDAO.TableNameArchive),
            None,
            "Database error while getting daily samples",
            True)
        # analyze samples and update min-max
        for sample in samples:
            contract = contracts[sample["contract_id"]]
            stations = contract["stations"]
            old_bikes = stations[sample["station_number"]]
            new_bikes = sample["available_bikes"]
            stations[sample["station_number"]] = new_bikes
            delta = new_bikes - old_bikes
            if delta != 0:
                contract["cur"] += delta
                if delta > 0 and contract["cur"] > contract["max"]:
                    contract["max"] = contract["cur"]
                if delta < 0 and contract["cur"] < contract["min"]:
                    contract["min"] = contract["cur"]
        # write to db
        inserted = self._db.execute_many(
            '''
            INSERT OR REPLACE INTO %s (
                start_of_day,
                contract_id,
                min_bikes,
                max_bikes)
            VALUES(
                strftime('%%s', :date),
                :contract_id,
                :min,
                :max)
            ''' % self.ContractsDayTable,
            contracts.itervalues(),
            "Database error while storing daily contract min max into table [%s]" % self.ContractsDayTable)
        if self._arguments.verbose:
            print "... %i records" % inserted
        return inserted

    def _do_globals(self, date):
        if self._arguments.verbose:
            print "Update table", self.GlobalsDayTable, "for", date,
        # sum up contracts into globals
        inserted = self._db.execute_single(
            '''
            INSERT OR REPLACE INTO %s
                SELECT start_of_day,
                    sum(min_bikes),
                    sum(max_bikes)
                FROM %s
                WHERE start_of_day = strftime('%%s', ?, 'start of day')
                GROUP BY start_of_day
            ''' % (self.GlobalsDayTable, self.ContractsDayTable),
            (date,),
            "Database error while storing daily global min max into table [%s]" % self.ContractsDayTable)
        if self._arguments.verbose:
            print "... %i records" % inserted
        return inserted

    def run(self, date):
        self._do_stations(date)
        self._do_contracts(date)
        self._do_globals(date)

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
            self._create_table_global_custom(self.GlobalDayTable, "start_of_day")

        if not self._db.has_table(self.StationsWeekTable):
            self._create_table_stations_custom(self.StationsWeekTable, "start_of_week")
        if not self._db.has_table(self.ContractsWeekTable):
            self._create_table_contracts_custom(self.ContractsWeekTable, "start_of_week")
        if not self._db.has_table(self.GlobalWeekTable):
            self._create_table_global_custom(self.GlobalWeekTable, "start_of_week")

        if not self._db.has_table(self.StationsMonthTable):
            self._create_table_stations_custom(self.StationsMonthTable, "start_of_month")
        if not self._db.has_table(self.ContractsMonthTable):
            self._create_table_contracts_custom(self.ContractsMonthTable, "start_of_month")
        if not self._db.has_table(self.GlobalMonthTable):
            self._create_table_global_custom(self.GlobalMonthTable, "start_of_month")

        if not self._db.has_table(self.StationsYearTable):
            self._create_table_stations_custom(self.StationsYearTable, "start_of_year")
        if not self._db.has_table(self.ContractsYearTable):
            self._create_table_contracts_custom(self.ContractsYearTable, "start_of_year")
        if not self._db.has_table(self.GlobalYearTable):
            self._create_table_global_custom(self.GlobalYearTable, "start_of_year")

    def _create_table_stations_custom(self, table_name, time_key_name):
        if self._arguments.verbose:
            print "Creating table", table_name
        self._db.execute_single(
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
            ''' % (table_name, time_key_name, time_key_name),
            None,
            "Database error while creating table [%s]" % table_name)

    def _create_table_contracts_custom(self, table_name, time_key_name):
        if self._arguments.verbose:
            print "Creating table", table_name
        self._db.execute_single(
            '''
            CREATE TABLE %s (
                %s INTEGER NOT NULL,
                contract_id INTEGER NOT NULL,
                num_changes INTEGER NOT NULL,
                rank_global INTEGER,
                PRIMARY KEY (%s, contract_id)
            ) WITHOUT ROWID;
            ''' % (table_name, time_key_name, time_key_name),
            None,
            "Database error while creating table [%s]" % table_name)

    def _create_table_global_custom(self, table_name, time_key_name):
        if self._arguments.verbose:
            print "Creating table", table_name
        self._db.execute_single(
            '''
            CREATE TABLE %s (
                %s INTEGER NOT NULL,
                num_changes INTEGER NOT NULL,
                PRIMARY KEY (%s)
            ) WITHOUT ROWID;
            ''' % (table_name, time_key_name, time_key_name),
            None,
            "Database error while creating table [%s]" % table_name)

    def _do_activity_stations_custom(self, params):
        if self._arguments.verbose:
            print "Update table", params["target_table"], "for", params["date"],
        inserted = self._db.execute_single(
            '''
            INSERT OR REPLACE INTO %s
                SELECT %s,
                    contract_id,
                    station_number,
                    %s,
                    NULL,
                    NULL
                FROM %s
                WHERE %s BETWEEN %s AND %s
                GROUP BY contract_id, station_number
            ''' % (params["target_table"],
                   params["time_select"],
                   params["aggregate_select"],
                   params["source_table"],
                   params["where_select"],
                   params["between_first"],
                   params["between_last"]),
            params,
            "Database error while storing stations activity into table [%s]" % params["target_table"])
        if self._arguments.verbose:
            print "... %i records" % inserted
        return inserted

    def _do_activity_contracts_custom(self, params):
        if self._arguments.verbose:
            print "Update table", params["target_table"], "for", params["date"],
        inserted = self._db.execute_single(
            '''
            INSERT OR REPLACE INTO %s
                SELECT %s,
                    contract_id,
                    SUM(num_changes),
                    NULL
                FROM %s
                WHERE %s
                GROUP BY contract_id
            ''' % (params["target_table"],
                   params["time_select"],
                   params["source_table"],
                   params["where_clause"]),
            params,
            "Database error while storing daily contracts activity into table [%s]" % params["target_table"])
        if self._arguments.verbose:
            print "... %i records" % inserted
        return inserted

    def _do_activity_global_custom(self, params):
        if self._arguments.verbose:
            print "Update table", params["target_table"], "for", params["date"],
        inserted = self._db.execute_single(
            '''
            INSERT OR REPLACE INTO %s
                SELECT %s,
                    SUM(num_changes)
                FROM %s
                WHERE %s
                GROUP BY %s
            ''' % (params["target_table"],
                   params["time_select"],
                   params["source_table"],
                   params["where_clause"],
                   params["time_select"]),
            params,
            "Database error while storing daily global activity into table [%s]" % params["target_table"])
        if self._arguments.verbose:
            print "... %i records" % inserted
        return inserted

    @staticmethod
    def _rank_generic(items, value_index, global_outdex, section_index=None, section_outdex=None):
        # used for section ranking
        sections = {}
        # used for global ranking
        global_infos = {
            "g_total": None,
            "g_rank": None,
            "g_last": None
        }
        # do the ranking, one item at a time
        for item in items:
            ## calculate section ranks (only if asked)
            if section_index is not None and section_outdex is not None:
                # first
                if item[section_index] not in sections:
                    sections[item[section_index]] = {
                        "s_total": 0,
                        "s_rank": 1,
                        "s_last": item[value_index]
                    }
                # quicker lookip
                section_infos = sections[item[section_index]]
                # sample done for section
                section_infos["s_total"] += 1
                # not the same number of events
                if item[value_index] != section_infos["s_last"]:
                    section_infos["s_rank"] = section_infos["s_total"]
                    section_infos["s_last"] = item[value_index]
                # set section rank
                item[section_outdex] = section_infos["s_rank"]
            ## calculate global ranks
            # first
            if global_infos["g_last"] is None:
                global_infos["g_total"] = 0
                global_infos["g_rank"] = 1
                global_infos["g_last"] = item[value_index]
            # sample done globally
            global_infos["g_total"] += 1
            # not the same number of events
            if item[value_index] != global_infos["g_last"]:
                global_infos["g_rank"] = global_infos["g_total"]
                global_infos["g_last"] = item[value_index]
            # set global rank
            item[global_outdex] = global_infos["g_rank"]
            ## return updated item
            yield item

    def _stations_update_ranking_custom(self, params, expr_date, table_name, timefield_name):
        # read from db
        raw_items = self._db.execute_fetch_generator(
            '''
            SELECT %s,
                contract_id,
                station_number,
                num_changes,
                rank_contract,
                rank_global
            FROM %s
            WHERE %s = %s
            ORDER BY num_changes DESC
            ''' % (timefield_name, table_name, timefield_name, expr_date),
            params,
            "Database error while getting activity ranks",
            True)
        # rank item
        ranked_items = Activity._rank_generic(
            raw_items,
            "num_changes",
            "rank_global",
            "contract_id",
            "rank_contract")
        # write to db
        updated = self._db.execute_many(
            '''
            UPDATE %s
            SET rank_global = :rank_global,
                rank_contract = :rank_contract
            WHERE %s = :%s AND
                contract_id = :contract_id AND
                station_number = :station_number
            ''' % (table_name, timefield_name, timefield_name),
            ranked_items,
            "Database error while updating %s" % table_name)
        # return number of updated records
        return updated

    def _contracts_update_ranking_custom(self, params, expr_date, table_name, timefield_name):
        # read from db
        raw_items = self._db.execute_fetch_generator(
            '''
            SELECT %s,
                contract_id,
                num_changes,
                rank_global
            FROM %s
            WHERE %s = %s
            ORDER BY num_changes DESC
            ''' % (timefield_name, table_name, timefield_name, expr_date),
            params,
            "Database error while getting activity ranks",
            True)
        # rank item
        ranked_items = Activity._rank_generic(
            raw_items,
            "num_changes",
            "rank_global",
            None,
            None)
        # write to db
        updated = self._db.execute_many(
            '''
            UPDATE %s
            SET rank_global = :rank_global
            WHERE %s = :%s AND
                contract_id = :contract_id
            ''' % (table_name, timefield_name, timefield_name),
            ranked_items,
            "Database error while updating %s" % table_name)
        # return number of updated records
        return updated

    def run(self, date):
        # daily station
        self._do_activity_stations_custom({
            "date": date,
            "target_table": self.StationsDayTable,
            "time_select": "strftime('%s', timestamp, 'unixepoch', 'start of day')",
            "aggregate_select": "COUNT(timestamp)",
            "source_table": "%s.%s" % (self._sample_schema, jcd.dao.ShortSamplesDAO.TableNameArchive),
            "where_select": "timestamp",
            "between_first": "strftime('%s', :date, 'start of day')",
            "between_last": "strftime('%s', :date, 'start of day', '+1 day') - 1"
        })
        self._stations_update_ranking_custom(
            {"date": date},
            "strftime('%s', :date, 'start of day')",
            self.StationsDayTable,
            "start_of_day")
        # weekly station
        self._do_activity_stations_custom({
            "date": date,
            "target_table": self.StationsWeekTable,
            "time_select": "start_of_day - strftime('%w', start_of_day, 'unixepoch', '-1 day') * 86400",
            "aggregate_select": "SUM(num_changes)",
            "source_table": self.StationsDayTable,
            "where_select": "start_of_day",
            "between_first": "strftime('%s', :date, '-' || strftime('%w', :date, '-1 day') || ' days', 'start of day')",
            "between_last": "strftime('%s', :date, '-' || strftime('%w', :date, '-1 day') || ' days', 'start of day', '+7 days') - 1"
        })
        self._stations_update_ranking_custom(
            {"date": date},
            "strftime('%s', :date, '-' || strftime('%w', :date, '-1 day') || ' days', 'start of day')",
            self.StationsWeekTable,
            "start_of_week")
        # monthly station
        self._do_activity_stations_custom({
            "date": date,
            "target_table": self.StationsMonthTable,
            "time_select": "strftime('%s', start_of_day, 'unixepoch', 'start of month')",
            "aggregate_select": "SUM(num_changes)",
            "source_table": self.StationsDayTable,
            "where_select": "start_of_day",
            "between_first": "strftime('%s', :date, 'start of month')",
            "between_last": "strftime('%s', :date, 'start of month', '+1 month') - 1"
        })
        self._stations_update_ranking_custom(
            {"date": date},
            "strftime('%s', :date, 'start of month')",
            self.StationsMonthTable,
            "start_of_month")
        # yearly station
        self._do_activity_stations_custom({
            "date": date,
            "target_table": self.StationsYearTable,
            "time_select": "strftime('%s', start_of_month, 'unixepoch', 'start of year')",
            "aggregate_select": "SUM(num_changes)",
            "source_table": self.StationsMonthTable,
            "where_select": "start_of_month",
            "between_first": "strftime('%s', :date, 'start of year')",
            "between_last": "strftime('%s', :date, 'start of year', '+1 year') - 1"
        })
        self._stations_update_ranking_custom(
            {"date": date},
            "strftime('%s', :date, 'start of year')",
            self.StationsYearTable,
            "start_of_year")
        # daily contract
        self._do_activity_contracts_custom({
            "date": date,
            "target_table": self.ContractsDayTable,
            "time_select": "start_of_day",
            "source_table": self.StationsDayTable,
            "where_clause": "start_of_day = strftime('%s', :date, 'start of day')",
        })
        self._contracts_update_ranking_custom(
            {"date": date},
            "strftime('%s', :date, 'start of day')",
            self.ContractsDayTable,
            "start_of_day")
        # weekly contract
        self._do_activity_contracts_custom({
            "date": date,
            "target_table": self.ContractsWeekTable,
            "time_select": "start_of_week",
            "source_table": self.StationsWeekTable,
            "where_clause": "start_of_week = strftime('%s', :date, '-' || strftime('%w', :date, '-1 day') || ' days', 'start of day')",
        })
        self._contracts_update_ranking_custom(
            {"date": date},
            "strftime('%s', :date, '-' || strftime('%w', :date, '-1 day') || ' days', 'start of day')",
            self.ContractsWeekTable,
            "start_of_week")
        # monthly contract
        self._do_activity_contracts_custom({
            "date": date,
            "target_table": self.ContractsMonthTable,
            "time_select": "start_of_month",
            "source_table": self.StationsMonthTable,
            "where_clause": "start_of_month = strftime('%s', :date, 'start of month')",
        })
        self._contracts_update_ranking_custom(
            {"date": date},
            "strftime('%s', :date, 'start of month')",
            self.ContractsMonthTable,
            "start_of_month")
        # yearly contract
        self._do_activity_contracts_custom({
            "date": date,
            "target_table": self.ContractsYearTable,
            "time_select": "start_of_year",
            "source_table": self.StationsYearTable,
            "where_clause": "start_of_year = strftime('%s', :date, 'start of year')",
        })
        self._contracts_update_ranking_custom(
            {"date": date},
            "strftime('%s', :date, 'start of year')",
            self.ContractsYearTable,
            "start_of_year")
        # daily global
        self._do_activity_global_custom({
            "date": date,
            "target_table": self.GlobalDayTable,
            "time_select": "start_of_day",
            "source_table": self.ContractsDayTable,
            "where_clause": "start_of_day = strftime('%s', :date, 'start of day')",
        })
        # weekly global
        self._do_activity_global_custom({
            "date": date,
            "target_table": self.GlobalWeekTable,
            "time_select": "start_of_week",
            "source_table": self.ContractsWeekTable,
            "where_clause": "start_of_week = strftime('%s', :date, '-' || strftime('%w', :date, '-1 day') || ' days', 'start of day')",
        })
        # monthly global
        self._do_activity_global_custom({
            "date": date,
            "target_table": self.GlobalMonthTable,
            "time_select": "start_of_month",
            "source_table": self.ContractsMonthTable,
            "where_clause": "start_of_month = strftime('%s', :date, 'start of month')",
        })
        # yearly global
        self._do_activity_global_custom({
            "date": date,
            "target_table": self.GlobalYearTable,
            "time_select": "start_of_year",
            "source_table": self.ContractsYearTable,
            "where_clause": "start_of_year = strftime('%s', :date, 'start of year')",
        })

class App(object):

    def __init__(self, default_data_path, default_statdb_filename, default_appdb_filename):
        # construct parser
        self._parser = argparse.ArgumentParser(
            description='Calculate min-max data from jcd stats')
        self._parser.add_argument(
            '--datadir',
            help='choose data folder (default: %s)' % default_data_path,
            default=default_data_path
        )
        self._parser.add_argument(
            '--statdbname',
            help='choose stats db filename (default: %s)' % default_statdb_filename,
            default=default_statdb_filename
        )
        self._parser.add_argument(
            '--appdbname',
            help='choose app db filename (default: %s)' % default_appdb_filename,
            default=default_appdb_filename
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
        with jcd.common.SqliteDB(arguments.statdbname, arguments.datadir) as db_stats:
            for date in arguments.date:
                if arguments.verbose:
                    print "Processing", date
                # attach db
                schema = jcd.dao.ShortSamplesDAO.get_schema_name(date)
                filename = jcd.dao.ShortSamplesDAO.get_db_file_name(schema)
                db_stats.attach_database(filename, schema, arguments.datadir)
                db_stats.attach_database(arguments.appdbname, "app", arguments.datadir)
                # do processing
                MinMax(db_stats, schema, arguments).run(date)
                Activity(db_stats, schema, arguments).run(date)
                # detach db
                db_stats.detach_database("app")
                db_stats.detach_database(schema)

# main
if __name__ == '__main__':
    try:
        App("~/.jcd_v2", "stats.db", "app.db").run()
    except KeyboardInterrupt:
        pass
