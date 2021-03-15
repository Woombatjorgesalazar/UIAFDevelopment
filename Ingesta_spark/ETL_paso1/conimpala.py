# (Required) Install the impyla package
# !pip3 install impyla
import os
import pandas
from impala.dbapi import connect
from impala.util import as_pandas

# Connect to Impala using Impyla
# Secure clusters will require additional parameters to connect to Impala.
# Recommended: Specify IMPALA_HOST as an environment variable in your project settings

IMPALA_HOST = os.getenv('clusternode8', 'clusternode8.activos.uiaf.gov.co')
conn = connect(host=IMPALA_HOST, port=21050)

# Execute using SQL
cursor = conn.cursor()

cursor.execute('SELECT day,AVG(tip) AS avg_tip \
                FROM tips \
                WHERE sex ILIKE "%Female%" \
                GROUP BY day \
                ORDER BY avg_tip DESC')


# Pretty output using Pandas
tables = as_pandas(cursor)
tables