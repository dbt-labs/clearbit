
import psycopg2
import time

def RateLimited(maxPerSecond):
    minInterval = 1.0 / float(maxPerSecond)
    def decorate(func):
        lastTimeCalled = [0.0]
        def rateLimitedFunction(*args,**kargs):
            elapsed = time.clock() - lastTimeCalled[0]
            leftToWait = minInterval - elapsed
            if leftToWait>0:
                time.sleep(leftToWait)
            ret = func(*args,**kargs)
            lastTimeCalled[0] = time.clock()
            return ret
        return rateLimitedFunction
    return decorate

def fetch_rows(cur, query, handle_row, batch_size):
  cur.execute(query)

  rows = cur.fetchmany(batch_size)
  while rows is not None and len(rows) > 0:
    for row in rows:
      handle_row(row)

    rows = cur.fetchmany(batch_size)

def fetch(config, new_rows_query, callback):
  # TODO : get this from the config
  conn_string = "dbname={} user={} password={} host={} port={}".format('data_generator','Drew', '', 'localhost', 5439)

  with psycopg2.connect(conn_string) as conn:
    with conn.cursor() as cur:

      # TODO : get this from the config
      @RateLimited(config['per_second'])
      def handle_row(row):
        callback(row)

      batch_size = config.get('batch_size', 100)
      fetch_rows(cur, new_rows_query, handle_row, batch_size)
