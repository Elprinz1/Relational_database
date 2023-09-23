import psycopg as ps
from user_definition import *


def drop_tables(user, host, dbname):
    """
    This function connects to a database
    using user, host and dbname, and
    drops all the tables including
    report_type, incident_type, location and incident.
    This function should work regardless of
    the existence of the table without any errors.
    """
    with ps.connect(f"dbname={dbname} user={user} host={host}") as conn:
        with conn.cursor() as cur:
            query = '''
                DROP TABLE IF EXISTS  report_type,\
                    incident_type, location, incident;
                    '''
            cur.execute(query)
        conn.commit()
    return 'Tables Dropped'


def create_tables(user, host, dbname):
    """
    For the given user, host, and dbname,
    this function creates 4 different tables including
    report_type, incident_type, location and incident.
    The schema of each table is the following.
    report_type:
    report_type_code - varying character (length of 2), not null
    report_type_description - varying character (length of 100), not null
    primary key - report_type_code

    incident_type:
    incident_code - integer, not null
    incident_category - varying character (length of 100), null
    incident_subcategory - varying character (length of 100), null
    incident_description - varying character (length of 200), null
    primary key - incident_code

    location:
    longitude - real, not null
    latitude - real, not null
    supervisor_district - real, null
    police_district - varying character (length of 100), not null
    neighborhood - varying character (length of 100), null
    prinmary key - longitude, latitude

    incident:
    id - integer, not null
    incident_datetime - timestamp, not null
    report_datetime -  timestamp, not null
    longitude - real, null
    latitude - real, null
    report_type_code - varying character (length of 2), not null
    incident_code - integer, not null
    primary key - id
    foreign key - report_type_code, incident_code and
                  (longitude, latitude) pair.
    """
    with ps.connect(f"dbname={dbname} user={user} host={host}") as conn:
        with conn.cursor() as cur:
            query = '''
                        CREATE TABLE IF NOT EXISTS report_type
                        (
                            report_type_code VARCHAR(2) NOT NULL,
                            report_type_description VARCHAR(100) NOT NULL,
                            PRIMARY KEY (report_type_code)
                        );

                        CREATE TABLE IF NOT EXISTS incident_type
                        (
                            incident_code INTEGER NOT NULL,
                            incident_category VARCHAR(100) NULL,
                            incident_subcategory VARCHAR(100) NULL,
                            incident_description VARCHAR(200) NULL,
                            PRIMARY KEY (incident_code)
                        );

                        CREATE TABLE IF NOT EXISTS location
                        (
                            longitude REAL NOT NULL,
                            latitude REAL NOT NULL,
                            supervisor_district REAL NULL,
                            police_district VARCHAR(100) NOT NULL,
                            neighborhood VARCHAR(100) NULL,
                            PRIMARY KEY (longitude, latitude)
                        );

                        CREATE TABLE IF NOT EXISTS incident
                        (
                            id INTEGER NOT NULL,
                            incident_datetime TIMESTAMP NOT NULL,
                            report_datetime TIMESTAMP NOT NULL,
                            longitude REAL NULL,
                            latitude REAL NULL,
                            report_type_code VARCHAR(2) NOT NULL,
                            incident_code INTEGER NOT NULL,
                            PRIMARY KEY (id),
                            FOREIGN KEY (report_type_code) REFERENCES\
                                report_type (report_type_code) ON UPDATE\
                                CASCADE ON DELETE CASCADE,
                            FOREIGN KEY (incident_code) REFERENCES\
                                incident_type (incident_code) ON UPDATE\
                                CASCADE ON DELETE CASCADE,
                            FOREIGN KEY (longitude, latitude) REFERENCES\
                                location (longitude, latitude) ON UPDATE\
                                CASCADE ON DELETE CASCADE
                        );
                    '''
            cur.execute(query)

        conn.commit()

    return 'All Tables Created'


def copy_data(user, host, dbname, dir):
    """
    Using user, host, dbname, and dir,
    this function connects to the database and
    loads data to report_type, incident_type, location and incident
    from report_type.csv, incident_type.csv, location.csv and incident.csv
    located in dir.
    Note: each file includes a header.
    """
    data_list = [
        ("report_type", "report_type.csv"),
        ("incident_type", "incident_type.csv"),
        ("location", "location.csv"),
        ("incident", "incident.csv")
        ]
    with ps.connect(f"dbname={dbname} user={user} host={host}") as conn:
        with conn.cursor() as cur:
            for table, data in data_list:
                query = '''
                        COPY {}
                        FROM '{}/{}'
                        DELIMITER ','
                        CSV HEADER;
                        '''.format(table, dir, data)
                cur.execute(query)
        conn.commit()
    return 'Copied data successfully'


def return_distinct_neighborhood_police_district(user, host, dbname, n=None):
    """
    Using user, host, dbname, dir, and n,
    this function connects to the database and
    returns n unique rows of neighborhood and police_district
    in the location table,
    where neighborhood is not null.
    The returned output is ordered by neighborhood and police_district
    in ascending order.
    If n is not given, it returns all the rows.
    """
    with ps.connect(f"dbname={dbname} user={user} host={host}") as conn:
        with conn.cursor() as cur:
            if n:
                query = f'''
                            SELECT DISTINCT neighborhood, police_district
                            FROM location
                            WHERE neighborhood IS NOT NULL
                            ORDER BY neighborhood ASC, police_district ASC
                            LIMIT {n};
                        '''
            else:
                query = '''
                            SELECT DISTINCT neighborhood, police_district
                            FROM location
                            WHERE neighborhood IS NOT NULL
                            ORDER BY neighborhood ASC, police_district ASC;
                        '''
            cur.execute(query)
            records = cur.fetchall()
    return records


def return_distinct_time_taken(user, host, dbname, n=None):
    """
    Using user, host, dbname, dir, and n,
    this function connects to the database and
    returns n unique differences
    between report_datetime and incident_datetime in days
    in descending order.
    If n is not given, it returns all the rows.
    """
    with ps.connect(f"dbname={dbname} user={user} host={host}") as conn:
        with conn.cursor() as cur:
            if n:
                query = f'''
                            SELECT DISTINCT (EXTRACT\
                                (DAY FROM report_datetime -\
                                incident_datetime)) AS Difference
                            FROM incident
                            ORDER BY Difference DESC
                            LIMIT {n};
                        '''
            else:
                query = '''
                            SELECT DISTINCT (EXTRACT\
                                (DAY FROM report_datetime -\
                                incident_datetime)) AS Difference
                            FROM incident
                            ORDER BY Difference DESC;
                        '''
            cur.execute(query)
            records = cur.fetchall()
    return records


def return_incident_with_incident_substring(user,
                                            host,
                                            dbname,
                                            substr,
                                            n=None):
    """
    Using user, host, dbname, dir, substr, and n,
    this function connects to the database and
    returns n unique id and incident_datetime in the incident table
    where its incident_code corresonds to the incident_description
    that includes substr, a substring ordered by id in ascending order.
    The search for the existence of the given substring
    should be case-insensitive.
    If n is not given, it returns all the rows.
    """
    with ps.connect(f"dbname={dbname} user={user} host={host}") as conn:
        with conn.cursor() as cur:
            if n:
                query = f'''
                          SELECT DISTINCT id, incident_datetime
                            FROM incident
                            WHERE incident_code IN (
                                SELECT incident_code
                                FROM incident_type
                                WHERE LOWER(incident_description)\
                                    ILIKE '%{substr}%'
                                )
                            ORDER BY id ASC
                            LIMIT {n};
                        '''
            else:
                query = f'''
                           SELECT DISTINCT id, incident_datetime
                            FROM incident
                            WHERE incident_code IN (SELECT incident_code\
                                FROM incident_type
                                WHERE LOWER(incident_description)\
                                    ILIKE '%{substr}%'
                            )
                            ORDER BY id ASC;
                        '''
            cur.execute(query)
            records = cur.fetchall()
    return records


def return_incident_desc_for_report_type_desc(user,
                                              host,
                                              dbname,
                                              desc,
                                              n=None):
    """
    Using user, host, dbname, dir, substr, and n,
    this function connects to the database and
    returns n unique incident description in the incident_type table
    where its incident_code corresponds to desc as
    report_type_description ordered by incident_description in ascending order.
    The search of the report_type_description
    should be case-insensitive.
    If n is not given, it returns all the rows.
    """
    with ps.connect(f"dbname={dbname} user={user} host={host}") as conn:
        with conn.cursor() as cur:
            if n:
                query = f'''
                            SELECT DISTINCT incident_description
                            FROM incident_type
                            WHERE incident_code IN(
                                SELECT incident_code
                                FROM incident
                                WHERE report_type_code IN(
                                    SELECT report_type_code
                                    FROM report_type
                                    WHERE report_type_description\
                                        ILIKE '%{desc}%'
                                )
                            )
                            ORDER BY incident_description ASC
                            LIMIT {n};
                        '''
            else:
                query = f'''
                            SELECT DISTINCT incident_description
                            FROM incident_type
                            WHERE incident_code IN(
                                SELECT incident_code
                                FROM incident
                                WHERE report_type_code IN(
                                    SELECT report_type_code
                                    FROM report_type
                                    WHERE report_type_description\
                                        ILIKE '%{desc}%'
                                )
                            )
                            ORDER BY incident_description ASC;
                        '''
            cur.execute(query)
            records = cur.fetchall()
    return records


def update_report_type(user, host, dbname, from_str, to_str):
    """
    Using user, host, dbname, dir, and n,
    this function connects to the database and
    updates report_type_code from from_str to to_str
    on the report_type table.
    """
    with ps.connect(f"dbname={dbname} user={user} host={host}") as conn:
        with conn.cursor() as cur:
            query = '''
                        UPDATE report_type
                        SET report_type_code = %s
                        WHERE report_type_code = %s;
                    '''
            cur.execute(query, (to_str, from_str))
            conn.commit()
    return 'Updated successfully'
