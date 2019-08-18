import sqlite3
import yaml
import pathlib

data_folder = (pathlib.Path(__file__).parent.parent / 'data').resolve()


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


class MetricDB:
    """
       Custom wrapper around an sqlite database with a plausibly easier-to-use API
    """
    def __init__(self, key):
        filepath = data_folder / '{}.db'.format(key)
        self.raw_db = sqlite3.connect(str(filepath), detect_types=sqlite3.PARSE_DECLTYPES)
        sqlite3.register_adapter(bool, int)
        sqlite3.register_converter("bool", lambda v: bool(int(v)))
        self.raw_db.row_factory = dict_factory

        structure_filepath = data_folder / '{}.yaml'.format(key)
        self.db_structure = yaml.load(open(str(structure_filepath)))
        self._update_database_structure()

    def query(self, query):
        """ Run the specified query and return all the rows (as dictionaries)"""
        try:
            cursor = self.raw_db.cursor()
            cursor.execute(query)
            return cursor.fetchall()
        except sqlite3.OperationalError as e:
            print(e)
            print(query)
            exit(0)

    def execute(self, command, params=None):
        """ Execute the given command with the parameters. Return nothing """
        try:
            cur = self.raw_db.cursor()
            if params is None:
                cur.execute(command)
            else:
                cur.execute(command, params)
        except sqlite3.Error as e:
            print(e)
            print(command)
            print(params)
            exit(0)

    def reset(self):
        """ Clear all of the data out of the database and recreate the tables """
        db = self.raw_db.cursor()
        for table in self.db_structure['tables']:
            db.execute('DROP TABLE IF EXISTS %s' % table)
        self._update_database_structure()

    def close(self, print_table_sizes=True):
        """ Write data to database. Possibly print the number of rows in each table """
        if print_table_sizes:
            for table in self.db_structure['tables']:
                print('{}({})'.format(table, self.count(table)))
        self.raw_db.commit()
        self.raw_db.close()

    # Convenience interfaces for special types of queries
    def lookup_all(self, field, table, clause=''):
        """ Run a SELECT command with the specified field, table and clause, return an array of the matching values """
        values = []
        for row in self.query('SELECT {} from {} {}'.format(field, table, clause)):
            values.append(row[field])
        return values

    def lookup(self, field, table, clause=''):
        """ Run a SELECT command and return the first (only?) value """
        results = self.lookup_all(field, table, clause + ' LIMIT 1')
        if not results:
            return None
        return results[0]

    def count(self, table, clause=''):
        """ Return the number of results for a given query """
        return self.lookup('count(*)', table, clause)

    def dict_lookup(self, key_field, value_field, table, clause=''):
        """ Return a dictionary mapping the key_field to the value_field for some query """
        results = self.query('SELECT {}, {} from {} {}'.format(key_field, value_field, table, clause))
        return dict([(d[key_field], d[value_field]) for d in results])

    def unique_counts(self, table, ident_field):
        """ Return a dictionary mapping the different values of the ident_field column to how many times each appears"""
        return self.dict_lookup(ident_field, 'count(*)', table, 'GROUP BY ' + ident_field)

    def sum_counts(self, table, value_field, ident_field):
        """ Return a dictionary mapping the different values of the ident_field column to the sum of the value_field
            column in rows that match the key"""
        return self.dict_lookup(ident_field, 'sum({})'.format(value_field), table, 'GROUP BY ' + ident_field)

    # Convenience interfaces for inserting or updating a dictionary row into the table
    def insert(self, table, row_dict, replace_key=None):
        """ insert the given row into the table. If replace_key is specified, update the row with matching key """
        keys = row_dict.keys()

        values = []
        for k in keys:
            value = row_dict.get(k)
            values.append(value)
        query = 'INSERT INTO %s (%s) VALUES(%s)' % (table,
                                                    ', '.join(keys),
                                                    ', '.join(['?'] * len(values))
                                                    )
        self.execute(query, values)

    def update(self, table, row_dict, replace_key='id'):
        """ If there is a row where the specified field matches the row_dict's value of the field, update it.
            Otherwise, just insert it. """
        clause = 'WHERE {}={}'.format(replace_key, self.format_value(replace_key, row_dict[replace_key]))
        if self.count(table, clause) == 0:
            # If no matches, just insert
            self.insert(table, row_dict)

        v_query = []
        values = []
        for k in row_dict.keys():
            if k == replace_key:
                continue
            values.append(row_dict[k])
            v_query.append('{}=?'.format(k))
        value_str = ', '.join(v_query)
        query = 'UPDATE {} SET {} '.format(table, value_str) + clause
        self.execute(query, values)

    # DB Structure Operations
    def get_field_type(self, field):
        """ Returns the type of a given field, based on the db_structure """
        return self.db_structure['special_types'].get(field, self.db_structure.get('default_type', 'text'))

    def format_value(self, field, value):
        """ If the field's type is text, surround with quotes """
        if self.get_field_type(field) == 'text':
            return '"{}"'.format(value)
        else:
            return value

    def _table_exists(self, table):
        """ Returns a boolean of whether the table exists """
        return self.count('sqlite_master', "WHERE type='table' AND name='{}'".format(table)) > 0

    def _create_table(self, table):
        """ Creates the table based on the db_structure """
        types = []
        for key in self.db_structure['tables'][table]:
            tt = self.get_field_type(key)
            if key == 'id':
                tt += ' PRIMARY KEY'
            types.append('%s %s' % (key, tt))
        create_cmd = 'CREATE TABLE %s (%s)' % (table, ', '.join(types))
        self.execute(create_cmd)

    def _table_types(self, table):
        """ Return a dictionary mapping the name of each field in a table to its type (according to the db) """
        type_map = {}
        for row in self.query('PRAGMA table_info("{}")'.format(table)):
            type_map[row['name']] = row['type']
        return type_map

    def _update_table(self, table):
        """ Update the structure of an existing table (as needed) """
        type_map = self._table_types(table)
        for key in self.db_structure['tables'][table]:
            tt = self.get_field_type(key)
            if key not in type_map:
                self.execute('ALTER TABLE {} ADD COLUMN {} {}'.format(table, key, tt))

    def _update_database_structure(self):
        """ Create or update the structure of all tables """
        for table, keys in self.db_structure['tables'].items():
            if not self._table_exists(table):
                self._create_table(table)
            else:
                self._update_table(table)
