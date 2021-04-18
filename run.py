import pandas as pd
import os
import sys

from web_scraping.web_scraping import scraper, transformer, writer, reader


URL = "https://id.wikipedia.org/wiki/Daftar_orang_terkaya_di_Indonesia"
DB_NAME = "web_scraping_db"
DB_USER = "username"
DB_PASSWORD = "secret"
DB_HOST = "db"
DB_PORT = "5432"
CONNECTION_STRING = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
TABLE_NAME = "orang_terkaya_indonesia"


def main() -> None:
    dfs = scraper.scrape(URL)
    df_2011 = transformer.transform2011(dfs[3], 2011)
    df_2013 = transformer.transform2013(dfs[4], 2013)
    df_2017 = transformer.transform2017(dfs[5], 2017)
    df_2019 = transformer.transform2019(dfs[6], 2019)
    df_2020 = transformer.transform2020(dfs[7], 2020)
    writer.write_to_postgres_2011(df=df_2011, db_name=DB_NAME, table_name=TABLE_NAME, connection_string=CONNECTION_STRING)
    writer.write_to_postgres_2013(df=df_2013, db_name=DB_NAME, table_name=TABLE_NAME, connection_string=CONNECTION_STRING)
    writer.write_to_postgres_2017(df=df_2017, db_name=DB_NAME, table_name=TABLE_NAME, connection_string=CONNECTION_STRING)
    writer.write_to_postgres_2019(df=df_2019, db_name=DB_NAME, table_name=TABLE_NAME, connection_string=CONNECTION_STRING)
    writer.write_to_postgres_2020(df=df_2020, db_name=DB_NAME, table_name=TABLE_NAME, connection_string=CONNECTION_STRING)
    result_df = reader.read_from_postgres(db_name=DB_NAME, table_name=TABLE_NAME, connection_string=CONNECTION_STRING)
    
    print("Daftar Orang Terkaya di Indonesia:")
    print(result_df.to_string())


if __name__ == "__main__":
    main()
