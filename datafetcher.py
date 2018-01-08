import psycopg2
import psycopg2.extras

FETCHMANY_SIZE = 1000

def fetch_rows(cur, query, batch_size):
  """Queries the database and yields the results

  Parameters
  ----------
  cur
      A psycopg2 cursor
  query : str
      The query to run against the database
  batch_size : int
      Number of results to yield in each tuple

  Yields
  ------
  tuple
      Yields one tuple at a time containg `batch_size` dicts. Each dict contains a record from the result set"""

  cur.execute(query)

  intermediate_results = []

  rows = cur.fetchmany(FETCHMANY_SIZE)
  while rows is not None and len(rows) > 0:
    for row in rows:
      intermediate_results.append(row)

      if len(intermediate_results) == batch_size:
        data_to_yield = tuple(intermediate_results)
        intermediate_results = []
        yield data_to_yield

    rows = cur.fetchmany(FETCHMANY_SIZE)

  if len(intermediate_results) > 0:
    yield tuple(intermediate_results)

def fetch(config, new_rows_query, batch_size=1):
  """Performantly fetch rows from a database

  Parameters
  ----------
  config : dict
      Dictionary containg "dbname", "user", "password", "host", and "port"
  new_rows_query : str
      A query which returns the rows you want to process
  batch_size : int
      Max number of results to yield at a given time (default=1)

  Yields
  ------
  tuple of dict
      Yields `batch_size` tuple(s) containing rows (dicts) from the query result set"""

  conn_string = "dbname={dbname} user={user} password={password} host={host} port={port}".format(**config)

  with psycopg2.connect(conn_string) as conn:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:

      for record in fetch_rows(cur, new_rows_query, batch_size):
        yield record
