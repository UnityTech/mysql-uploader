mysql-uploader
=======

Fast upload of Parquet data to MySQL.

For fast upload we do the following:
  1. Batch into target number of CSV files
  2. Provide hooks for sorting data based on primary key
  3. Use LOAD command to upload batch CSVs
  4. Parallel number of connections to MySQL
