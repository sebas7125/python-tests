import queries
import threading
from snowflake_connect import connection

def execute_query(queries):
    # Create a cursor for the connection
    cur = connection.cursor()
    try:
        # Flush cache
        cur.execute('alter session set use_cached_result = false')
        # Iterate for the number of queries in the thread
        for query in queries:
            # Execute each query
            cur.execute(query)
            # Fetch the results
            rows = cur.fetchall()
            for row in rows:
                print(row[0])
    except Exception as e:
        print("An unexpected error occurred:", e)
    finally:
        cur.close()

def multi_thread(queries):
    # Create and start a thread for each query
    threads = []
    for query in queries:
        if not isinstance(query, list):
            query = [query]
        thread = threading.Thread(target = execute_query, args = (query,))
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()


# execute_query(queries.count('lineitem_a'))
# execute_query(queries.count('lineitem_b'))

# execute_query(queries.join('lineitem10_a', 'lineitem10_b'))
# execute_query(queries.join('lineitem10_b', 'lineitem10_a'))
# execute_query(queries.join('lineitem10_a', 'lineitem100_b'))
# execute_query(queries.join('lineitem10_b', 'lineitem100_a'))
# execute_query(queries.join('lineitem100_a', 'lineitem100_b'))
# execute_query(queries.join('lineitem100_b', 'lineitem100_a'))

# queries = [  queries.join('lineitem10_a', 'lineitem10_b')
#             ,queries.join('lineitem10_b', 'lineitem10_a')
#             ,queries.join('lineitem10_a', 'lineitem100_b')
#             ,queries.join('lineitem10_b', 'lineitem100_a')
#             ,queries.join('lineitem100_a', 'lineitem100_b')
#             ,queries.join('lineitem100_b', 'lineitem100_a')
#             ,queries.join('lineitem10_a', 'lineitem1000_b')
#             ,queries.join('lineitem10_b', 'lineitem1000_a')
#             # ,queries.join('lineitem100_a', 'lineitem1000_b')
#             # ,queries.join('lineitem100_b', 'lineitem1000_a')
#             # ,queries.join('lineitem1000_a', 'lineitem1000_b')
#             # ,queries.join('lineitem1000_b', 'lineitem1000_a')
#         ]

queries = [ queries.join('lineitem100_a', 'lineitem100_b')
            ,queries.join('lineitem100_b', 'lineitem100_a')
            ,queries.join('lineitem100_a', 'lineitem100_b')
            ,queries.join('lineitem100_b', 'lineitem100_a')
            ,queries.join('lineitem100_a', 'lineitem100_b')
            ,queries.join('lineitem100_b', 'lineitem100_a')
            ,queries.join('lineitem100_a', 'lineitem100_b')
            ,queries.join('lineitem100_b', 'lineitem100_a')
            ,queries.join('lineitem100_a', 'lineitem100_b')
            ,queries.join('lineitem100_b', 'lineitem100_a')
            ,queries.join('lineitem100_a', 'lineitem100_b')
            ,queries.join('lineitem100_b', 'lineitem100_a')
            ,queries.join('lineitem100_a', 'lineitem100_b')
            ,queries.join('lineitem100_b', 'lineitem100_a')
            ,queries.join('lineitem100_a', 'lineitem100_b')
            ,queries.join('lineitem100_b', 'lineitem100_a')
            ,queries.join('lineitem100_a', 'lineitem100_b')
            ,queries.join('lineitem100_b', 'lineitem100_a')
            ,queries.join('lineitem100_a', 'lineitem100_b')
            ,queries.join('lineitem100_b', 'lineitem100_a')
        ]

multi_thread(queries)