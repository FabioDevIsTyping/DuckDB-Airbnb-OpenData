CREATE OR REPLACE TABLE staging_raw_listings AS
SELECT * FROM read_csv_auto('${CSV_PATH}', header=True);
