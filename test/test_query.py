import duckdb

con = duckdb.connect("data/warehouse/airbnb.duckdb")
con.execute("SET schema airbnb")

query = """
SELECT * FROM staging_raw_listings LIMIT 5;
"""