#! /usr/bin/env python
from __future__ import absolute_import, division, print_function, unicode_literals

from collections import deque
from collections import OrderedDict
from sets import Set
import json
import sys

reload(sys)
sys.setdefaultencoding('utf8')

DEBUG = False

LIT_STR = 'lit_str'
LIT_INT = 'lit_int'
COLUMN = 'column'

OP_EQUAL = "="
OP_NOT_EQUAL = "!="
OP_LESS_THAN = "<"
OP_LESS_THAN_EQUAL = "<="
OP_GREATER_THAN = ">"
OP_GREATER_THAN_EQUAL = ">="


def main():
    """
    The main function body that parses the input file and executes any valid
    queries.
    """
    args = sys.argv[1:]

    # Enable debugging mode.
    if len(args) == 2 and args[1] == "-d":
        global DEBUG
        DEBUG = True
        del args[1]

    if len(args) != 1:
        write_error("Expecting exactly 1 command-line argument, got {}.", len(args))

    # Load the query output from sql-to-json
    sql = read_input(args[0])
    execute_query(sql)


def execute_query(sql):
    """
    Parses and executes a query.
    """

    # Parse the from block and attempt to load the tables.
    all_tables, actual_tables = load_tables(sql['from'])

    # Parse the where block
    filters, joins = parse_where(all_tables, sql["where"])

    # # Parse the select block.
    # selected_columns = parse_select(all_tables, sql['select'])


    print('######################')
    for table, join in joins.iteritems():
        print(join)


    # # Execute the query if it parsed correctly.
    # query = Query(all_tables, actual_tables)

    # query.execute(filters, joins)
    # query.display_results(selected_columns)


class QueryParse(object):

    def __init__(self):
        self.root = None
        self.nodes = {}

    def add(self, join):

        right = None
        if join.right_table in self.nodes.keys():
            right = self.nodes[join.right_table]

        left = None
        if join.left_table in self.nodes.keys():
            left = self.nodes[join.right_table]

        # On adding a new left node make it the root.
        if left is None and right is None:
            node = QueryPlanNode(join)
            self.root = node
            self.nodes[join.left_table] = node
            self.nodes[join.right_tables] = None

        elif left is not None and join.right_table in self.nodes:
            pass

        elif left is None and join.right_table in self.nodes:
            pass

        elif left is not None and right is None:
            pass





class QueryPlanNode(object):

    def __init__(self, join):
        self.name = join.left_table
        self.right_node = None
        self.left_nodes = [ join ]

    def add(self, join):

        # No left or has right add left.
        if len(self.left_nodes) == 0 or self.right == None:
            self.left_nodes.append(join)

        # No right add right
        else:
            self.right_node = join


def read_input(file_path):
    """
    Attempts to open the first argument and read it as json.
    """
    try:
        with open(file_path) as query_file:
            return json.loads(query_file.read())
    except:
        write_error("Input file \"{}\" is not valid.", file_path)


def write(message):
    """
    Writes standard messages.
    """
    print(message)


def write_error(message, *args):
    """
    Writes an error message and exits.
    """
    sys.stderr.write(message.format(*args) + "\n")
    sys.exit(1)


def write_debug(message="", *args):
    """
    Writes a debug message if enabled.
    """
    if DEBUG:
        if len(args) == 0:
            print(message)
        else:
            print(message.format(*args))


def write_debug_short(message="", *args):
    """
    Writes a debug message if enables and shortens it to fit the screen.
    """
    if DEBUG:
        if len(args) == 0:
            print(shorten(str(message)))
        else:
            print(shorten(message.format(*args)))


def reverse_operation(operation):
    """
    A simple helper function that can be used to reverse an operation.
    """
    if operation == OP_LESS_THAN:
        return OP_GREATER_THAN
    elif operation == OP_GREATER_THAN:
        return OP_LESS_THAN
    elif operation == OP_LESS_THAN_EQUAL:
        return OP_GREATER_THAN_EQUAL
    elif operation == OP_GREATER_THAN_EQUAL:
        return OP_LESS_THAN_EQUAL
    return operation


def load_tables(from_clause):
    """
    Loads all the tables and returns references to them and their aliases.
    """
    write_debug()
    write_debug("- from")
    write_debug(from_clause)

    all_tables = {}
    actual_tables = {}
    for selected_table in from_clause:

        table_name = selected_table['source']['file']
        table_alias = selected_table['as']

        # Load table unless already loaded with different alias
        if table_name in all_tables:
            table = all_tables[table_name]
        else:
            table = Table(table_name)
            all_tables[table_name] = table

        # Track the alias
        if table_alias is not None:
            if table_alias in all_tables:
                write_error("The table alias \"{}\" has already been loaded", table_alias)
            all_tables[table_alias] = table
            actual_tables[table_alias] = table
        else:
            actual_tables[table_name] = table

    write_debug()
    write_debug("- tables loaded (all, actual)")
    write_debug(all_tables.keys())
    write_debug(actual_tables.keys())

    return (all_tables, actual_tables)


def parse_select(tables, select):
    """
    Parses the select clause into columns to be displayed.
    """
    write_debug()
    write_debug("- select")
    write_debug(select)

    selected_columns = []
    for selected_column in select:
        table_name = selected_column['source']['column']['table']
        column_name = selected_column['source']['column']['name']
        column_alias = selected_column['as']

        # If table and column, check that table for presense
        if table_name is not None and not tables[table_name].has_column(column_name):
            write_error("ERROR: Column reference \"{}\" does not exist in table \"{}\"",
                        column_name, table_name)

        # If no table, check for ambiguous column
        present_in_tables = { key for (key, value) in tables.iteritems() if value.has_column(column_name)}
        if table_name is None and len(present_in_tables) > 1:
            write_error("ERROR: Column reference \"{}\" is ambiguous; present in multiple tables: {}.", column_name, ", ".join(present_in_tables))

        if len(present_in_tables) == 1:
            table_name = present_in_tables.pop()
        selected_columns.append((table_name, column_name, column_alias))

    write_debug()
    write_debug("- selected columns")
    write_debug(selected_columns)

    return tuple(selected_columns)


def parse_where(tables, where):
    """
    Parses all the where clauses, these are the joins and filters
    """
    write_debug()
    write_debug("- where")
    write_debug(where)

    # Loaded filters and joins broken down by table.
    filters = OrderedDict()
    joins = OrderedDict()

    # Helper to detect compound joins.
    joins_right_side = Set()

    for where_clause in where:
        operation = where_clause['op']
        left = where_clause['left']
        right = where_clause['right']

        # If both sides are columns this is a join.
        if COLUMN in left and COLUMN in right:

            # Parse join
            join = parse_join(tables, operation, left, right)

            # Correct join order,
            # Standard compound join
            if join.left_table in joins and join.right_table in joins_right_side:
                pass

            # Reverse compound join
            elif join.right_table in joins and join.left_table in joins_right_side:
                operation = reverse_operation(operation)
                join.reverse()

            # Reserve the join to keep the order correct
            elif join.left_table in joins:
                operation = reverse_operation(operation)
                join.reverse()

            if join.left_table not in joins:
                joins[join.left_table] = []
            joins[join.left_table].append(join)

            joins_right_side.add(join.right_table)

        # Literal column is in the correct (left) space
        elif COLUMN in left and COLUMN not in right:

            column_filter = parse_filter(tables, operation, left, right)

            if column_filter.table not in filters:
                filters[column_filter.table] = []

            filters[column_filter.table].append(column_filter)

        # Keep the column on the right
        elif COLUMN in right and COLUMN not in left:

            # Reverse the filter.
            operation = reverse_operation(operation)
            left, right = right, left

            column_filter = parse_filter(tables, operation, left, right)

            if column_filter.table not in filters:
                filters[column_filter.table] = []

            filters[column_filter.table].append(column_filter)

        # If both sides are literals?
        elif COLUMN not in right and COLUMN not in left:
            write_error("Both sides of the filter are literals \"{}\" and \"{}\"\n",
                        *[right.values()[0], left.values()[0]])

    write_debug()
    write_debug(joins)
    write_debug(filters)

    return (filters, joins)


def parse_filter(tables, operation, left, right):
    """
    Parses a filter from the where clause
    """
    table_name = left['column']['table']
    column_name = left['column']['name']

    # If table and column, check that table for presense
    if table_name is not None and not tables[table_name].has_column(column_name):
        write_error("ERROR: Column reference \"{}\" does not exist in table \"{}\"", column_name, table_name)

    # If no table, check for ambiguous column
    present_in_tables = { key for (key, value) in tables.iteritems() if value.has_column(column_name)}
    if table_name is None and len(present_in_tables) > 1:
        write_error("ERROR: Column reference \"{}\" is ambiguous; present in multiple tables: {}.", column_name, ", ".join(present_in_tables))

    if len(present_in_tables) == 1:
        table_name = present_in_tables.pop()
    column_location = tables[table_name].column_location(column_name)
    left = (table_name, column_name, column_location)

    # Are filter types compatible.
    if tables[left[0]].column_type(left[1]) not in right:
        write_error("ERROR: Column filter types are incompatible.")

    return Filter(operation, left, right)


def parse_join(tables, operation, left, right):
    """
    Parses a join from the where clause
    """
    # Verify Left
    table_name = left['column']['table']
    column_name = left['column']['name']

    # If table and column, check that table for presense
    if table_name is not None and not tables[table_name].has_column(column_name):
        write_error("ERROR: Column reference \"{}\" does not exist in table \"{}\"", column_name, table_name)

    # If no table, check for ambiguous column
    present_in_tables = { key for (key, value) in tables.iteritems() if value.has_column(column_name)}
    if table_name is None and len(present_in_tables) > 1:
        write_error("ERROR: Column reference \"{}\" is ambiguous; present in multiple tables: {}.", column_name, ", ".join(present_in_tables))

    if len(present_in_tables) == 1:
        table_name = present_in_tables.pop()
    column_location = tables[table_name].column_location(column_name)
    left = (table_name, column_name, column_location)

    # Verify Right
    table_name = right['column']['table']
    column_name = right['column']['name']

    # If table and column, check that table for presense
    if table_name is not None and not tables[table_name].has_column(column_name):
        write_error("ERROR: Column reference \"{}\" does not exist in table \"{}\"", column_name, table_name)

    # If no table, check for ambiguous column
    present_in_tables = { key for (key, value) in tables.iteritems() if value.has_column(column_name)}
    if table_name is None and len(present_in_tables) > 1:
        write_error("ERROR: Column reference \"{}\" is ambiguous; present in multiple tables: {}.", column_name, ", ".join(present_in_tables))

    if len(present_in_tables) == 1:
        table_name = present_in_tables.pop()
    column_location = tables[table_name].column_location(column_name)
    right = (table_name, column_name, column_location)

    # Are join types compatible
    if tables[left[0]].column_type(left[1]) != tables[right[0]].column_type(right[1]):
        write_error("ERROR: Column join types are incompatible.")

    return Join(operation, left, right)


class Filter(object):
    """
    A simple abstraction that represents the fields that are required for a filter.
    """

    def __init__(self, operation, left, right):
        """
        Initializes a new filter.
        """
        write_debug("Filter {} op {} filter {}".format(operation, left, right))
        self.column = left[2]
        self.column_name = left[1]
        self.filter = right.values()[0]
        self.left = left
        self.operation = operation
        self.right = right
        self.table = left[0]
        self.type = right.keys()[0]

    def apply(self, row_data):
        """
        Determines is a row should be included in a result set.
        """
        if self.operation == OP_EQUAL:
            return row_data == self.filter
        elif self.operation == OP_NOT_EQUAL:
            return row_data != self.filter
        elif self.operation == OP_GREATER_THAN:
            return row_data > self.filter
        elif self.operation == OP_GREATER_THAN_EQUAL:
            return row_data >= self.filter
        elif self.operation == OP_LESS_THAN:
            return row_data < self.filter
        elif self.operation == OP_LESS_THAN_EQUAL:
            return row_data <= self.filter

    def __repr__(self):
        return "Filter \"{}\" {} \"{}\"".format(self.left, self.operation, self.right)


class Join(object):
    """
    A simple abstraction that represents the fields tat are required for joins.
    """

    def __init__(self, operation, left, right):
        """
        Initializes a new join operation.

        operation: The operation to perform.
        left: A tuple (table, column, column #)
        right: A tuple (table, column, column #)
        """
        write_debug("Join {} op {} join {}", *[operation, left, right])
        self.left_column = left[2]
        self.left_column_name = left[1]
        self.left_table = left[0]
        self.operation = operation
        self.right_column = right[2]
        self.right_column_name = right[1]
        self.right_table = right[0]

    def join(self, tables):
        """
        Returns the mappings between two joined columns.
        """

        left_column = tables[self.left_table].column_reverse_index[self.left_column]
        right_column = tables[self.right_table].column_reverse_index[self.right_column]

        write_debug_short("\nLeft Column Reverse Index \"{}->{}\"\n{}",
                          *[self.left_table, self.left_column_name, left_column])
        write_debug_short("\nRight Column Reverse Index \"{}->{}\"\n{}",
                          *[self.right_table, self.right_column_name, right_column])

        if self.operation == OP_EQUAL:
            return self.__equals(left_column, right_column)

        elif self.operation == OP_NOT_EQUAL:
            return self.__not_equals(left_column, right_column)

        elif self.operation == OP_GREATER_THAN:
            return self.__greater_than(left_column, right_column)

        elif self.operation == OP_GREATER_THAN_EQUAL:
            return self.__greater_than_equals(left_column, right_column)

        elif self.operation == OP_LESS_THAN:
            return self.__less_than(left_column, right_column)

        elif self.operation == OP_LESS_THAN_EQUAL:
            return self.__less_than_equals(left_column, right_column)

    def reverse(self):
        """
        Reverse this joins
        """
        self.left_table, self.right_table = self.right_table, self.left_table
        self.left_column, self.right_column = self.right_column, self.left_column
        self.left_column_name, self.right_column_name = self.right_column_name, self.left_column_name
        self.operation = reverse_operation(self.operation)

    def __equals(self, left_column, right_column):
        rows = []
        right = Set()
        left = Set()

        for left_key, left_row_ids in left_column.iteritems():
            if left_key in right_column:
                rows.append((left_row_ids, right_column[left_key]))
                left = left.union(left_row_ids)
                right = right.union(right_column[left_key])

        return (rows, left, right)

    def __not_equals(self, left_column, right_column):
        rows = []
        right = Set()
        left = Set()

        for left_key, left_row_ids in left_column.iteritems():
            selected_rows = Set()
            for right_key, right_rows in right_column.iteritems():
                if left_key != right_key:
                    selected_rows = selected_rows.union(right_rows)
            rows.append((left_row_ids, selected_rows))
            left = left.union(left_row_ids)
            right = right.union(selected_rows)

        return (rows, left, right)

    def __greater_than(self, left_column, right_column):
        rows = []
        right = Set()
        left = Set()

        for left_key, left_row_ids in left_column.iteritems():
            selected_rows = Set()
            for right_key, right_rows in right_column.iteritems():
                if left_key > right_key:
                    selected_rows = selected_rows.union(right_rows)
            rows.append((left_row_ids, selected_rows))
            left = left.union(left_row_ids)
            right = right.union(selected_rows)

        return (rows, left, right)

    def __greater_than_equals(self, left_column, right_column):
        rows = []
        right = Set()
        left = Set()

        for left_key, left_row_ids in left_column.iteritems():
            selected_rows = Set()
            for right_key, right_rows in right_column.iteritems():
                if left_key >= right_key:
                    selected_rows = selected_rows.union(right_rows)
            rows.append((left_row_ids, selected_rows))
            left = left.union(left_row_ids)
            right = right.union(selected_rows)

        return (rows, left, right)

    def __less_than(self, left_column, right_column):
        rows = []
        right = Set()
        left = Set()

        for left_key, left_row_ids in left_column.iteritems():
            selected_rows = Set()
            for right_key, right_rows in right_column.iteritems():
                if left_key < right_key:
                    selected_rows = selected_rows.union(right_rows)
            rows.append((left_row_ids, selected_rows))
            left = left.union(left_row_ids)
            right = right.union(selected_rows)

        return (rows, left, right)

    def __less_than_equals(self, left_column, right_column):
        rows = []
        right = Set()
        left = Set()

        for left_key, left_row_ids in left_column.iteritems():
            selected_rows = Set()
            for right_key, right_rows in right_column.iteritems():
                if left_key <= right_key:
                    selected_rows = selected_rows.union(right_rows)
            rows.append((left_row_ids, selected_rows))
            left = left.union(left_row_ids)
            right = right.union(selected_rows)

        return (rows, left, right)

    def __repr__(self):
        return "Join \"{}->{}\" {} \"{}->{}\"".format(
            self.left_table, self.left_column_name, self.operation,
            self.right_table, self.right_column_name
        )

    def __str__(self):
        return "Join \"{}->{}\" {} \"{}->{}\"".format(
            self.left_table, self.left_column_name, self.operation,
            self.right_table, self.right_column_name
        )


class Query(object):
    """
    A result object contains the filtered and join data.
    """

    def __init__(self, all_tables, actual_tables):
        # Just aliases
        self.actual_tables = {}
        for key, table in actual_tables.iteritems():
            self.actual_tables[key] = table

        # Contains aliases
        self.all_tables = {}
        for key, table in all_tables.iteritems():
            self.all_tables[key] = table

        # A set comtaining the left most row ids, this is used to join table
        # together from right to left.
        self.temp_table_rows = None

        # A temporary table that stores the table and row ids that resulted from
        # this query.
        self.temp_table = None

    def execute(self, filters, joins):
        """
        Displays the results for the query.
        """
        write_debug(bcolors.OKGREEN + "Executing Query" + bcolors.ENDC)

        # Determine the correct order to filter and join the tables in.
        write_debug(bcolors.OKGREEN + "Table Order" + bcolors.ENDC)
        table_order = []
        while len(table_order) != len(self.actual_tables):
            for table in self.actual_tables:
                if len(table_order) == 0 and table not in joins:
                    table_order.append(table)
                    break
                elif table in joins and joins[table][0].right_table in table_order:
                    table_order.append(table)
                    break
        write_debug(table_order)

        # Process the tables in the order determines in the previous step,
        # this should always be right to left.
        self.temp_table_rows = Set()
        self.temp_table = {}
        for table_index, table in enumerate(table_order):
            write_debug()
            write_debug(bcolors.OKGREEN + "Processing Table: \"{}\"" + bcolors.ENDC, *[table])

            # Join the table.
            joined_rows = Set()
            if table in joins:
                previous_table = table_order[table_index - 1]
                write_debug("\nJoins")
                for table_join in joins[table]:
                    write_debug(table_join)
                    # Determine the rows involved in the join.
                    joined_rows, left, right = table_join.join(self.actual_tables)

                    # Remove the rows that are not involved in an inner join.
                    if table == previous_table:
                        for row_id in self.temp_table_rows.difference(left):
                            del self.temp_table[row_id]
                            self.temp_table_rows.remove(row_id)
                    else:
                        for row_id in self.temp_table_rows.difference(right):
                            del self.temp_table[row_id]
                            self.temp_table_rows.remove(row_id)

                    write_debug("\nJoined rows ({})", *[len(joined_rows)])
                    write_debug(joined_rows)
                    new_temp = {}
                    if table == previous_table:
                        for lids, rids in joined_rows:
                            # For each left id, join it to all right ids.
                            for rid in rids:
                                new_row_id = "{}:{}".format(table_join.right_table, rid)
                                new_temp[rid] = []
                                for lid in lids:
                                    if lid in self.temp_table:
                                        for current_row in self.temp_table[lid]:
                                            joined_row = current_row[:]
                                            joined_row.insert(0, new_row_id)
                                            new_temp[rid].append(joined_row)
                    else:
                        for lids, rids in joined_rows:
                            # For each left id, join it to all right ids.
                            for lid in lids:
                                new_row_id = "{}:{}".format(table, lid)
                                new_temp[lid] = []
                                for rid in rids:
                                    if rid in self.temp_table:
                                        for current_row in self.temp_table[rid]:
                                            joined_row = current_row[:]
                                            joined_row.insert(0, new_row_id)
                                            new_temp[lid].append(joined_row)

                    self.temp_table = new_temp
                    self.temp_table_rows = left

                    write_debug("\nJoined rows ({})", *[len(joined_rows)])
                    write_debug(joined_rows)

            # Filter the data
            #
            # If there are no filters assume all rows will be selected.
            # Otherwise the first filter will add any applicable rows to a set
            # of valid rows then subsequent filters will be used to refined that
            # original set.
            #
            # Possible performance improvements:
            # - Apply the most selective filter first.
            selected_rows = Set()
            if table in filters:
                write_debug('\nFilters')
                for index, column_filter in enumerate(filters[table]):
                    write_debug(column_filter)
                    for row_id in self.actual_tables[table].columns[column_filter.column]:
                        row_data, row_id = row_id
                        # If the set is empty add rows
                        if index == 0 and column_filter.apply(row_data):
                            selected_rows.add(row_id)
                        # Remove rows that don't match.
                        elif row_id in selected_rows and not column_filter.apply(row_data):
                            selected_rows.remove(row_id)
                write_debug("\nFiltered rows ({})", *[len(selected_rows)])
                write_debug(selected_rows)

            # No filter display all rows.
            else:
                if table_index == 0:
                    selected_rows = range(0, self.actual_tables[table].rows)
                else:
                    selected_rows = self.temp_table_rows

            write_debug(bcolors.OKBLUE + "\nTemp table pre ({}, {})" + bcolors.ENDC,
                *[len(self.temp_table), len(self.temp_table_rows)])
            write_debug_short(self.temp_table)
            write_debug_short(self.temp_table_rows)

            # Construct a temp table that contains the filtered rows and joined
            # columns.
            if table_index == 0:
                for row_id in selected_rows:
                    self.temp_table_rows.add(row_id)
                    self.temp_table[row_id] = [["{}:{}".format(table, row_id)]]
            else:
                if len(selected_rows) > 0:
                    for row_id in self.temp_table_rows.difference(selected_rows):
                        del self.temp_table[row_id]
                        self.temp_table_rows.remove(row_id)

            write_debug(bcolors.OKBLUE + "\nTemp table ({}, {})" + bcolors.ENDC,
                        *[len(self.temp_table), len(self.temp_table_rows)])
            write_debug_short(self.temp_table)
            write_debug_short(self.temp_table_rows)

    def display_results(self, display_columns):
        """
        Prints out the results of the query.
        """
        write_debug(bcolors.OKGREEN + "\nDisplay Results" + bcolors.ENDC)
        # Parse the selected columns
        row_format, headers, valid_columns = self.__row_format(display_columns)

        # Print the header
        header_row = row_format.format(*headers)
        write(header_row)
        write("-" * len(header_row))

        # Print the data
        current_row = deque()
        write_debug(bcolors.OKBLUE + "\nTemp table ({}, {})" + bcolors.ENDC,
            *[len(self.temp_table), len(self.temp_table_rows)])
        for rowset in self.temp_table.values():
            for row in rowset:
                for column in row:
                    table_name, row_id = column.split(":")
                    table = self.actual_tables[table_name]
                    row_id = int(row_id)
                    for column_metadata in table.metadata.values():
                        column_location = column_metadata[0]
                        if (table_name, column_location) in valid_columns:
                            current_row.append(table.columns[column_location][row_id][0])

                write(row_format.format(*current_row))
                current_row.clear()

    def __row_format(self, display_columns):
        """
        Returns the row format and the table headers and the set of columns that
        will be displayed.
        """
        headers = []
        row_format = []
        valid_columns = Set()

        # TODO: Need to find a better place to track the max row and reduce the
        # cost for determining width.
        max_column_widths = {}
        for display_column in display_columns:
            table = display_column[0]
            column = display_column[1]
            column_location = self.all_tables[table].column_location(column)

            if table not in max_column_widths:
                max_column_widths[table] = {}
            max_column_widths[table][column_location] = 0

        for rollup in self.temp_table.values():
            for row in rollup:
                for column in row:
                    table_name, row_id = column.split(":")
                    row_id = int(row_id)
                    if table_name in max_column_widths:
                        for column_location in max_column_widths[table_name].keys():
                            table = self.actual_tables[table_name]
                            column_width = len(str(table.columns[column_location][row_id][0]))
                            if column_width > max_column_widths[table_name][column_location]:
                                max_column_widths[table_name][column_location] = column_width

        write_debug(max_column_widths)

        # Create the row format.
        row_id = 0
        for display_column in display_columns:
            table = display_column[0]
            column = display_column[1]
            alias = display_column[2]

            # Use the alias if one is provided
            if alias is not None:
                header = alias
            else:
                header = column
            headers.append(header)

            column_location = self.all_tables[table].column_location(column)
            valid_columns.add((table, column_location))

            column_width = max_column_widths[table][column_location]
            if len(header) > column_width:
                column_width = len(header)

            # TODO: Determine the max length of the values in a column.
            row_format.append("{" + str(row_id) + ":" + str((column_width)) + "}")
            row_id += 1

        return (" | ".join(row_format), headers, valid_columns)


class Table(object):
    """
    A Table object contains the raw loaded.
    """

    def __init__(self, name):
        """
        Creates a new in memory table that can be queried.
        """
        self.name = name

        # A dictionary of column names to metadatadata (column_num, type, max_length)
        self.metadata = None
        self.columns = None
        self.column_sets = None
        self.column_reverse_index = None
        self.rows = 0

        self.__load_table()

    def __load_table(self):
        """
        Loads a table from disk and parses it into a columnar data store.
        """
        table_file_name = "{}.table.json".format(self.name)
        try:
            with open(table_file_name) as table_file:
                table_data = json.loads(table_file.read())

                # Column Header Data is stored as a tuple (name, type).
                if len(table_data[0][0]) != 2:
                    write_error("There was no header row defined for table \"{}\"", *[self.name])

                metadata = OrderedDict()
                for column_metadata in table_data[0]:
                    if column_metadata[1] == 'int':
                        column_type = LIT_INT
                    elif column_metadata[1] == 'str':
                        column_type = LIT_STR
                    else:
                        raise ValueError("no column type")
                    metadata[column_metadata[0]] = (len(metadata), column_type)

                columns = []
                column_sets = []
                column_reverse_index = []
                column_max_length = []
                for key in metadata.keys():
                    columns.append([])
                    column_sets.append(Set())
                    column_reverse_index.append({})
                    column_max_length.append(0)

                # Load columns
                row_index = -1
                for row_index, row in enumerate(table_data[1:]):
                    for column_index, column in enumerate(row):
                        columns[column_index].append((column, row_index))
                        if column in column_sets[column_index]:
                            column_reverse_index[column_index][column].append(row_index)
                        else:
                            column_sets[column_index].add(column)
                            column_reverse_index[column_index][column] = [ row_index ]

                        # New longest column
                        # TODO: Only do this for none strings
                        column_length = len(str(column))
                        if column_length > column_max_length[column_index]:
                            column_max_length[column_index] = column_length

                # Update metadata with max length information
                self.metadata = OrderedDict()
                for index, key in enumerate(metadata.keys()):
                    column_location = metadata[key][0]
                    column_type = metadata[key][1]
                    self.metadata[key] = (column_location, column_type, column_max_length[index])

                # How big is the table
                self.rows = row_index + 1

                # Sort Data
                sorted_column_reverse_index = []
                for reverse_index in column_reverse_index:
                    sorted_index = OrderedDict()
                    for key in sorted(reverse_index.iterkeys()):
                        sorted_index[key] = reverse_index[key]
                    sorted_column_reverse_index.append(sorted_index)

                if DEBUG:
                    write_debug(bcolors.OKGREEN + "Loaded \"{}\"" + bcolors.ENDC, *[table_file_name])
                    write_debug()
                    write_debug("- metadata")
                    write_debug(self.metadata)
                    write_debug()
                    write_debug("- data colums")
                    for column in columns:
                        write_debug_short(column)
                    write_debug()
                    write_debug("- data column_sets")
                    write_debug_short(column_sets)
                    write_debug()
                    write_debug("- data sorted_column_reverse_index")
                    write_debug_short(sorted_column_reverse_index)
                    write_debug()

                write("- Loaded \"{}\", {} rows.".format(table_file_name, self.rows))

                self.columns = columns
                self.column_sets = column_sets
                self.column_reverse_index = column_reverse_index
        except IOError as e:
            write_error("Unable to read table \"{}\" from disk\n", *[table_file_name])

    def column_width(self, column_name):
        """
        Returns the maximum width for a column.
        """
        if column_name not in self.metadata:
            write_error("The column {} could not be found in table {}", column_name, self.name)
        return self.metadata[column_name][2]

    def has_column(self, column_name):
        """
        Returns whether a table has a given column.
        """
        return column_name in self.metadata

    def column_location(self, column_name):
        """
        Returns the columnar location for a named column.
        """
        if column_name not in self.metadata:
            write_error("The column {} could not be found in table {}", column_name, self.name)
        return self.metadata[column_name][0]

    def column_type(self, column_name):
        """
        Returns the data type for a column.
        """
        if column_name not in self.metadata:
            write_error("The column {} could not be found in table {}", column_name, self.name)
        return self.metadata[column_name][1]

    def column(self, column_name):
        """
        Returns a full column.
        """
        if column_name not in self.metadata:
            write_error("The column {} could not be found in table {}", column_name, self.name)
        return self.columns[column_name]


class bcolors(object):
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

"""Text wrapping and filling.
"""

# Copyright (C) 1999-2001 Gregory P. Ward.
# Copyright (C) 2002, 2003 Python Software Foundation.
# Written by Greg Ward <gward@python.net>

import re

# Hardcode the recognized whitespace characters to the US-ASCII
# whitespace characters.  The main reason for doing this is that
# some Unicode spaces (like \u00a0) are non-breaking whitespaces.
_whitespace = '\t\n\x0b\x0c\r '

class TextWrapper:
    """
    Object for wrapping/filling text.  The public interface consists of
    the wrap() and fill() methods; the other methods are just there for
    subclasses to override in order to tweak the default behaviour.
    If you want to completely replace the main wrapping algorithm,
    you'll probably have to override _wrap_chunks().

    Several instance attributes control various aspects of wrapping:
      width (default: 70)
        the maximum width of wrapped lines (unless break_long_words
        is false)
      initial_indent (default: "")
        string that will be prepended to the first line of wrapped
        output.  Counts towards the line's width.
      subsequent_indent (default: "")
        string that will be prepended to all lines save the first
        of wrapped output; also counts towards each line's width.
      expand_tabs (default: true)
        Expand tabs in input text to spaces before further processing.
        Each tab will become 0 .. 'tabsize' spaces, depending on its position
        in its line.  If false, each tab is treated as a single character.
      tabsize (default: 8)
        Expand tabs in input text to 0 .. 'tabsize' spaces, unless
        'expand_tabs' is false.
      replace_whitespace (default: true)
        Replace all whitespace characters in the input text by spaces
        after tab expansion.  Note that if expand_tabs is false and
        replace_whitespace is true, every tab will be converted to a
        single space!
      fix_sentence_endings (default: false)
        Ensure that sentence-ending punctuation is always followed
        by two spaces.  Off by default because the algorithm is
        (unavoidably) imperfect.
      break_long_words (default: true)
        Break words longer than 'width'.  If false, those words will not
        be broken, and some lines might be longer than 'width'.
      break_on_hyphens (default: true)
        Allow breaking hyphenated words. If true, wrapping will occur
        preferably on whitespaces and right after hyphens part of
        compound words.
      drop_whitespace (default: true)
        Drop leading and trailing whitespace from lines.
      max_lines (default: None)
        Truncate wrapped lines.
      placeholder (default: ' [...]')
        Append to the last line of truncated text.
    """

    unicode_whitespace_trans = {}
    uspace = ord(' ')
    for x in _whitespace:
        unicode_whitespace_trans[ord(x)] = uspace

    # This funky little regex is just the trick for splitting
    # text up into word-wrappable chunks.  E.g.
    #   "Hello there -- you goof-ball, use the -b option!"
    # splits into
    #   Hello/ /there/ /--/ /you/ /goof-/ball,/ /use/ /the/ /-b/ /option!
    # (after stripping out empty strings).
    word_punct = r'[\w!"\'&.,?]'
    letter = r'[^\d\W]'
    whitespace = r'[%s]' % re.escape(_whitespace)
    nowhitespace = '[^' + whitespace[1:]
    wordsep_re = re.compile(r'''
        ( # any whitespace
          %(ws)s+
        | # em-dash between words
          (?<=%(wp)s) -{2,} (?=\w)
        | # word, possibly hyphenated
          %(nws)s+? (?:
            # hyphenated word
              -(?: (?<=%(lt)s{2}-) | (?<=%(lt)s-%(lt)s-))
              (?= %(lt)s -? %(lt)s)
            | # end of word
              (?=%(ws)s|\Z)
            | # em-dash
              (?<=%(wp)s) (?=-{2,}\w)
            )
        )''' % {'wp': word_punct, 'lt': letter,
                'ws': whitespace, 'nws': nowhitespace},
        re.VERBOSE)
    del word_punct, letter, nowhitespace

    # This less funky little regex just split on recognized spaces. E.g.
    #   "Hello there -- you goof-ball, use the -b option!"
    # splits into
    #   Hello/ /there/ /--/ /you/ /goof-ball,/ /use/ /the/ /-b/ /option!/
    wordsep_simple_re = re.compile(r'(%s+)' % whitespace)
    del whitespace

    # XXX this is not locale- or charset-aware -- string.lowercase
    # is US-ASCII only (and therefore English-only)
    sentence_end_re = re.compile(r'[a-z]'             # lowercase letter
                                 r'[\.\!\?]'          # sentence-ending punct.
                                 r'[\"\']?'           # optional end-of-quote
                                 r'\Z')               # end of chunk

    def __init__(self,
                 width=70,
                 initial_indent="",
                 subsequent_indent="",
                 expand_tabs=True,
                 replace_whitespace=True,
                 fix_sentence_endings=False,
                 break_long_words=True,
                 drop_whitespace=True,
                 break_on_hyphens=True,
                 tabsize=8,
                 max_lines=None,
                 placeholder=' [...]'):
        self.width = width
        self.initial_indent = initial_indent
        self.subsequent_indent = subsequent_indent
        self.expand_tabs = expand_tabs
        self.replace_whitespace = replace_whitespace
        self.fix_sentence_endings = fix_sentence_endings
        self.break_long_words = break_long_words
        self.drop_whitespace = drop_whitespace
        self.break_on_hyphens = break_on_hyphens
        self.tabsize = tabsize
        self.max_lines = max_lines
        self.placeholder = placeholder


    # -- Private methods -----------------------------------------------
    # (possibly useful for subclasses to override)

    def _munge_whitespace(self, text):
        """_munge_whitespace(text : string) -> string

        Munge whitespace in text: expand tabs and convert all other
        whitespace characters to spaces.  Eg. " foo\\tbar\\n\\nbaz"
        becomes " foo    bar  baz".
        """
        if self.expand_tabs:
            text = text.expandtabs(self.tabsize)
        if self.replace_whitespace:
            text = text.translate(self.unicode_whitespace_trans)
        return text


    def _split(self, text):
        """_split(text : string) -> [string]

        Split the text to wrap into indivisible chunks.  Chunks are
        not quite the same as words; see _wrap_chunks() for full
        details.  As an example, the text
          Look, goof-ball -- use the -b option!
        breaks into the following chunks:
          'Look,', ' ', 'goof-', 'ball', ' ', '--', ' ',
          'use', ' ', 'the', ' ', '-b', ' ', 'option!'
        if break_on_hyphens is True, or in:
          'Look,', ' ', 'goof-ball', ' ', '--', ' ',
          'use', ' ', 'the', ' ', '-b', ' ', option!'
        otherwise.
        """
        if self.break_on_hyphens is True:
            chunks = self.wordsep_re.split(text)
        else:
            chunks = self.wordsep_simple_re.split(text)
        chunks = [c for c in chunks if c]
        return chunks

    def _fix_sentence_endings(self, chunks):
        """_fix_sentence_endings(chunks : [string])

        Correct for sentence endings buried in 'chunks'.  Eg. when the
        original text contains "... foo.\\nBar ...", munge_whitespace()
        and split() will convert that to [..., "foo.", " ", "Bar", ...]
        which has one too few spaces; this method simply changes the one
        space to two.
        """
        i = 0
        patsearch = self.sentence_end_re.search
        while i < len(chunks)-1:
            if chunks[i+1] == " " and patsearch(chunks[i]):
                chunks[i+1] = "  "
                i += 2
            else:
                i += 1

    def _handle_long_word(self, reversed_chunks, cur_line, cur_len, width):
        """_handle_long_word(chunks : [string],
                             cur_line : [string],
                             cur_len : int, width : int)

        Handle a chunk of text (most likely a word, not whitespace) that
        is too long to fit in any line.
        """
        # Figure out when indent is larger than the specified width, and make
        # sure at least one character is stripped off on every pass
        if width < 1:
            space_left = 1
        else:
            space_left = width - cur_len

        # If we're allowed to break long words, then do so: put as much
        # of the next chunk onto the current line as will fit.
        if self.break_long_words:
            cur_line.append(reversed_chunks[-1][:space_left])
            reversed_chunks[-1] = reversed_chunks[-1][space_left:]

        # Otherwise, we have to preserve the long word intact.  Only add
        # it to the current line if there's nothing already there --
        # that minimizes how much we violate the width constraint.
        elif not cur_line:
            cur_line.append(reversed_chunks.pop())

        # If we're not allowed to break long words, and there's already
        # text on the current line, do nothing.  Next time through the
        # main loop of _wrap_chunks(), we'll wind up here again, but
        # cur_len will be zero, so the next line will be entirely
        # devoted to the long word that we can't handle right now.

    def _wrap_chunks(self, chunks):
        """_wrap_chunks(chunks : [string]) -> [string]

        Wrap a sequence of text chunks and return a list of lines of
        length 'self.width' or less.  (If 'break_long_words' is false,
        some lines may be longer than this.)  Chunks correspond roughly
        to words and the whitespace between them: each chunk is
        indivisible (modulo 'break_long_words'), but a line break can
        come between any two chunks.  Chunks should not have internal
        whitespace; ie. a chunk is either all whitespace or a "word".
        Whitespace chunks will be removed from the beginning and end of
        lines, but apart from that whitespace is preserved.
        """
        lines = []
        if self.width <= 0:
            raise ValueError("invalid width %r (must be > 0)" % self.width)
        if self.max_lines is not None:
            if self.max_lines > 1:
                indent = self.subsequent_indent
            else:
                indent = self.initial_indent
            if len(indent) + len(self.placeholder.lstrip()) > self.width:
                raise ValueError("placeholder too large for max width")

        # Arrange in reverse order so items can be efficiently popped
        # from a stack of chucks.
        chunks.reverse()

        while chunks:

            # Start the list of chunks that will make up the current line.
            # cur_len is just the length of all the chunks in cur_line.
            cur_line = []
            cur_len = 0

            # Figure out which static string will prefix this line.
            if lines:
                indent = self.subsequent_indent
            else:
                indent = self.initial_indent

            # Maximum width for this line.
            width = self.width - len(indent)

            # First chunk on line is whitespace -- drop it, unless this
            # is the very beginning of the text (ie. no lines started yet).
            if self.drop_whitespace and chunks[-1].strip() == '' and lines:
                del chunks[-1]

            while chunks:
                l = len(chunks[-1])

                # Can at least squeeze this chunk onto the current line.
                if cur_len + l <= width:
                    cur_line.append(chunks.pop())
                    cur_len += l

                # Nope, this line is full.
                else:
                    break

            # The current line is full, and the next chunk is too big to
            # fit on *any* line (not just this one).
            if chunks and len(chunks[-1]) > width:
                self._handle_long_word(chunks, cur_line, cur_len, width)
                cur_len = sum(map(len, cur_line))

            # If the last chunk on this line is all whitespace, drop it.
            if self.drop_whitespace and cur_line and cur_line[-1].strip() == '':
                cur_len -= len(cur_line[-1])
                del cur_line[-1]

            if cur_line:
                if (self.max_lines is None or
                    len(lines) + 1 < self.max_lines or
                    (not chunks or
                     self.drop_whitespace and
                     len(chunks) == 1 and
                     not chunks[0].strip()) and cur_len <= width):
                    # Convert current line back to a string and store it in
                    # list of all lines (return value).
                    lines.append(indent + ''.join(cur_line))
                else:
                    while cur_line:
                        if (cur_line[-1].strip() and
                            cur_len + len(self.placeholder) <= width):
                            cur_line.append(self.placeholder)
                            lines.append(indent + ''.join(cur_line))
                            break
                        cur_len -= len(cur_line[-1])
                        del cur_line[-1]
                    else:
                        if lines:
                            prev_line = lines[-1].rstrip()
                            if (len(prev_line) + len(self.placeholder) <=
                                    self.width):
                                lines[-1] = prev_line + self.placeholder
                                break
                        lines.append(indent + self.placeholder.lstrip())
                    break

        return lines

    def _split_chunks(self, text):
        text = self._munge_whitespace(text)
        return self._split(text)

    # -- Public interface ----------------------------------------------

    def wrap(self, text):
        """wrap(text : string) -> [string]

        Reformat the single paragraph in 'text' so it fits in lines of
        no more than 'self.width' columns, and return a list of wrapped
        lines.  Tabs in 'text' are expanded with string.expandtabs(),
        and all other whitespace characters (including newline) are
        converted to space.
        """
        chunks = self._split_chunks(text)
        if self.fix_sentence_endings:
            self._fix_sentence_endings(chunks)
        return self._wrap_chunks(chunks)

    def fill(self, text):
        """fill(text : string) -> string

        Reformat the single paragraph in 'text' to fit in lines of no
        more than 'self.width' columns, and return a new string
        containing the entire wrapped paragraph.
        """
        return "\n".join(self.wrap(text))


# -- Convenience interface ---------------------------------------------

def wrap(text, width=70, **kwargs):
    """Wrap a single paragraph of text, returning a list of wrapped lines.

    Reformat the single paragraph in 'text' so it fits in lines of no
    more than 'width' columns, and return a list of wrapped lines.  By
    default, tabs in 'text' are expanded with string.expandtabs(), and
    all other whitespace characters (including newline) are converted to
    space.  See TextWrapper class for available keyword args to customize
    wrapping behaviour.
    """
    w = TextWrapper(width=width, **kwargs)
    return w.wrap(text)

def fill(text, width=70, **kwargs):
    """Fill a single paragraph of text, returning a new string.

    Reformat the single paragraph in 'text' to fit in lines of no more
    than 'width' columns, and return a new string containing the entire
    wrapped paragraph.  As with wrap(), tabs are expanded and other
    whitespace characters converted to space.  See TextWrapper class for
    available keyword args to customize wrapping behaviour.
    """
    w = TextWrapper(width=width, **kwargs)
    return w.fill(text)

def shorten(text, width=70, **kwargs):
    """Collapse and truncate the given text to fit in the given width.

    The text first has its whitespace collapsed.  If it then fits in
    the *width*, it is returned as is.  Otherwise, as many words
    as possible are joined and then the placeholder is appended::

        >>> textwrap.shorten("Hello  world!", width=12)
        'Hello world!'
        >>> textwrap.shorten("Hello  world!", width=11)
        'Hello [...]'
    """
    w = TextWrapper(width=width, max_lines=1, **kwargs)
    return w.fill(' '.join(text.strip().split()))

if __name__ == '__main__':
    main()
