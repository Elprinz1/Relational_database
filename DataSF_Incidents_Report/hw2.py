from hw1 import *
from user_definition import *

import psycopg as ps


def select_all(func):
    """
    Q1. Complete the select_all() decorator, which 1) retrieve
    keyword arguments including user, host and dbname,
    2) executes a SQL query string returned
    from a function and 3) returns the output.
    Ex.
    When
    @select_all
    def return_incident_category_count(**kargs):
        # complete
    , calling
    return_incident_category_count(user='postgres',
                                   host='127.0.0.1',
                                   dbname='msds691_HW',
                                   n=5)
    returns [('Other Miscellaneous', 101), ('Larceny Theft', 90),
    ('Robbery', 72), ('Drug Offense', 60), ('Burglary', 52)]
    """

    def wrapper(**kargs):
        user = kargs['user']
        host = kargs['host']
        dbname = kargs['dbname']
        with ps.connect(f"user='{user}' \
                         host='{host}' \
                         dbname='{dbname}'") as conn:
            try:
                with conn.cursor() as curs:
                    query = func(**kargs)
                    curs.execute(query)
                    result = curs.fetchall()
            except Exception as e:
                print('Something went wrong:' + str(e))
                result = None
                conn.rollback()
        return result
    return wrapper


def check_query_args(**kargs):
    query = kargs['query']
    if 'explain' in kargs and kargs['explain'] is True:
        query = 'EXPLAIN ANALYZE VERBOSE ' + query
    if 'n' in kargs:
        query = query + f" LIMIT {kargs['n']}"
    return query


@select_all
def return_incident_category_count(**kargs):
    """
    Q2. Complete the return_incident_category_count() function.
    This function connects to the database using the parameters
    user, host, dbname, and n, and retrieves n records of
    incident_category along with their corresponding count
    from the incident_type table.
    The function only retrieves records
    where incident_category is not null and orders them
    by count in descending order.
    If there are rows with the same count,
    the function sorts them alphabetically by incident_category
    in ascending order.
    If the parameter n is not provided, the function returns all rows.
    """
    query = f"""
                SELECT incident_category, \
                COUNT(incident_category)
                FROM incident_type
                WHERE incident_type IS NOT NULL
                GROUP BY incident_category
                ORDER BY COUNT DESC, incident_category ASC
            """
    return check_query_args(query=query, **kargs)


@select_all
def return_incident_count_by_category_subcategory(**kargs):
    """
    Q3. Complete the return_incident_count_by_category_subcategory() function.
    This function connects to the database
    using the provided user, host, dbname,
    count_limit, and n parameters.
    It returns n records of incident_category, incident_subcategory,
    and their count (occurrence) in the incident table
    where the count is greater than count_limit.
    The output is ordered by occurrence in descending order.
    If there are records with the same count value, they are ordered
    by incident_category alphabetically (ascending).
    """
    query = f"""
                SELECT incident_category, \
                    incident_subcategory, \
                    COUNT(incident_subcategory)
                FROM incident
                JOIN incident_type
                ON incident_type.incident_code = \
                    incident.incident_code
                GROUP BY \
                    incident_category, incident_subcategory
                HAVING \
                    COUNT(incident_subcategory) > {kargs['count_limit']}
                ORDER BY COUNT DESC, incident_category
            """
    return check_query_args(query=query, **kargs)


@select_all
def return_count_by_location_report_type_incident_description(**kargs):
    """
    Q4. Complete
    the return_count_by_location_report_type_incident_description() function.
    This function connects to the database
    using the given user, host, dbname, year,
    and n parameters, and returns an output of n rows (if n is given) or
    all rows of the following columns: year (extracted from incident_datetime),
    month (also extracted from incident_datetime), longitude, latitude,
    neighborhood, report_type_description, incident_description,
    and the corresponding count, which is ordered by count in descending order,
    and then by year, month, longitude, latitude, report_type_description,
    and incident_description in ascending order.
    """
    query = f"""
            SELECT EXTRACT(YEAR FROM incident_datetime) AS year, \
                EXTRACT(MONTH FROM incident_datetime) AS month, \
                    incident.longitude, incident.latitude, \
                        location.neighborhood,
                report_type.report_type_description,
                incident_type.incident_description, COUNT(*)
            FROM incident
            JOIN location
            ON location.latitude = incident.latitude \
                AND location.longitude = incident.longitude
            JOIN incident_type
            ON incident_type.incident_code = \
                incident.incident_code
            JOIN report_type
            ON report_type.report_type_code = \
                incident.report_type_code
            GROUP BY year, month, incident.longitude,
            incident.latitude, location.neighborhood, \
                report_type.report_type_description, \
                incident_type.incident_description
            HAVING EXTRACT(YEAR FROM incident_datetime) = \
                {kargs['year']}
            ORDER BY COUNT DESC, \
                year ASC, month ASC, \
                incident.longitude ASC, \
                incident.latitude ASC,
                location.neighborhood ASC, \
                report_type.report_type_description ASC, \
                incident_type.incident_description ASC
            """
    return check_query_args(query=query, **kargs)


@select_all
def return_avg_interval_days_per_incident_code(**kargs):
    """
    Q5.
    Complete the return_avg_interval_days_per_incident_code() function.
    This function calculates the average number of days taken between
    incident_datetime and report_datetime for each incident_code.
    Using user, host, dbname, and n, this function connects to the database
    and returns n rows of incident_code, incident_description,
    and avg_interval_days,
    where avg_interval_days is the average difference between report_datetime
    and incident_datetime extracted as days.
    The output should be ordered by avg_interval_days in descending order.
    If there are multiple rows with the same avg_interval_days,
    order by incident_code in ascending order.
    If n is not given, it returns all the rows.
    """
    query = f"""
                SELECT incident.incident_code, \
                    incident_description, \
                    FLOOR(AVG(DATE_PART \
                    ('day', report_datetime - \
                    incident_datetime)))
                    AS avg_interval_days
                FROM incident
                JOIN incident_type
                ON incident_type.incident_code = \
                    incident.incident_code
                GROUP BY incident.incident_code, \
                    incident_type.incident_description
                ORDER BY avg_interval_days DESC, \
                    incident.incident_code ASC
            """
    return check_query_args(query=query, **kargs)


@select_all
def return_monthly_count(**kargs):
    """
    Q6.
    Complete the `return_monthly_count()` function.
    This function returns the number of incidents in each month of each year.
    Using `user`, `host`, `dbname`, and `n`, this function connects to
    the database and returns `n` rows of `year`, `jan`, `feb`, `mar`, `apr`,
    `may`, `jun`, `jul`, `aug`, `sep`, `oct`, `nov` and `dec`,
    where each column includes the number of incidents
    for the corresponding year and month, ordered by year in ascending order.
    If `n` is not given, it returns all the rows.
    """
    query = f"""
              SELECT
                year,
                SUM(CASE WHEN month = 1 \
                THEN Count ELSE NULL END) AS jan,
                SUM(CASE WHEN month = 2 \
                THEN Count ELSE NULL END) AS feb,
                SUM(CASE WHEN month = 3 \
                THEN Count ELSE NULL END) AS mar,
                SUM(CASE WHEN month = 4 \
                THEN Count ELSE NULL END) AS apr,
                SUM(CASE WHEN month = 5 \
                THEN Count ELSE NULL END) AS may,
                SUM(CASE WHEN month = 6 \
                THEN Count ELSE NULL END) AS jun,
                SUM(CASE WHEN month = 7 \
                THEN Count ELSE NULL END) AS jul,
                SUM(CASE WHEN month = 8 \
                THEN Count ELSE NULL END) AS aug,
                SUM(CASE WHEN month = 9 \
                THEN Count ELSE NULL END) AS sep,
                SUM(CASE WHEN month = 10 \
                THEN Count ELSE NULL END) AS oct,
                SUM(CASE WHEN month = 11 \
                THEN Count ELSE NULL END) AS nov,
                SUM(CASE WHEN month = 12 \
                THEN Count ELSE NULL END) AS dec
            FROM (
                SELECT
                    EXTRACT(YEAR FROM incident_datetime) \
                    AS year,
                    EXTRACT(MONTH FROM incident_datetime) \
                    AS month,
                    COUNT(*) as Count
                FROM incident
                GROUP BY year, month
            ) subquery
            GROUP BY year
            ORDER BY year ASC
            """
    return check_query_args(query=query, **kargs)


def create_index(**kargs):
    """
    Q7. Assuming that the query
    return_count_by_location_report_type_incident_description() (Q4)
    is the most frequently used query in your database,
    complete create_index() which creates indexes to improve its performance
    by at least 10%.
    For this question, you can assume that there will be no insertions or
    updates made to the database afterwards.
    Using streamlit, the create_index  will display the query improvement
    after you enter the absolute path of the data directory.
    """
    user = kargs['user']
    host = kargs['host']
    dbname = kargs['dbname']

    queryset = [
        "CREATE INDEX incident_year_month_index \
            ON incident \
            USING btree (EXTRACT(YEAR FROM incident_datetime), \
            EXTRACT(MONTH FROM incident_datetime));",
        "CREATE INDEX location_index \
            ON location \
            USING btree (latitude, longitude);",
        "CREATE INDEX incident_type_index \
            ON incident_type \
            USING hash (incident_code);",
        "CREATE INDEX report_type_index \
            ON report_type \
            USING hash (report_type_code);",
    ]
    with ps.connect(f"user='{user}' \
                    host='{host}' \
                    dbname='{dbname}'") as conn:
        try:
            with conn.cursor() as curs:
                for query in queryset:
                    curs.execute(query)
        except Exception as e:
            print("Something went wrong", e)
            conn.rollback
        conn.commit()

    return "Successfully created indexes"


# queryset = [
#         "CREATE INDEX incident_year_month_index \
#             ON incident \
#             USING btree (EXTRACT(YEAR FROM incident_datetime), \
#             EXTRACT(MONTH FROM incident_datetime));",
#         "CREATE INDEX location_index \
#             ON location \
#             USING btree (latitude, longitude);",
#         "CREATE INDEX incident_type_index \
#             ON incident_type \
#             USING hash (incident_code);",
#         "CREATE INDEX report_type_index \
#             ON report_type \
#             USING hash (report_type_code);",
#     ]


# queryset = [
#         "CREATE INDEX incident_year_index \
#             ON incident \
#             USING hash (EXTRACT(YEAR FROM incident_datetime));",
#         "CREATE INDEX incident_month_index \
#             ON incident \
#             USING hash (EXTRACT(MONTH FROM incident_datetime));",
#         "CREATE INDEX location_lat_index \
#             ON location \
#             USING hash(latitude);",
#          "CREATE INDEX location_long_index \
#             ON location \
#             USING hash (longitude);",
#         "CREATE INDEX incident_type_index \
#             ON incident_type \
#             USING hash (incident_code);",
#         "CREATE INDEX report_type_index \
#             ON report_type \
#             USING hash (report_type_code);",
#     ]
