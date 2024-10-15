import snowflake.connector

ctx = snowflake.connector.connect(
user='RAMANJANEYAAWSSNOWFLAKEMOUNIKA',
password='Snowflake@12345',
account='ewniymx-sx75283'
)
cs = ctx.cursor()
cs.is_closed()
print(cs.is_closed())
try:
    cs.execute("select * from t1")
    one = cs.fetchone()
    print(one[0])
finally:
    cs.close()






