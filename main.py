#!/usr/bin/env python3

"""Programa para receber dados de uma database do Kaggle (.csv), e upar para 
uma database, normalizando em quatro tabelas: fMovies, dDirectors, dActors, 
dWriters.
"""

__version__ = "0.1.0"

import psycopg2
import pandas as pd

def create_database():
    #Connecting to postgres default database:
    conn = psycopg2.connect("host=127.0.0.1 dbname=postgres port=5433\
                            user=postgres password=PI314159")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    #Creating movies database:
    cur.execute("DROP DATABASE movies")
    cur.execute("CREATE DATABASE movies")

    conn.close()

    conn = psycopg2.connect("host=127.0.0.1 dbname=movies port=5433\
                            user=postgres password=PI314159")

    return cur, conn

# def drop_tables(cur, conn):
#     for query in drop_tables_queries:
#         cur.execute(query)
#         conn.commit()

# def create_tables(cur, conn):
#     for query in create_table_queries:
#         cur.execute(query)
#         conn.commit()

original_df = pd.read_csv("../Datasets/Hydra-Movie-Scrape.csv")

cleaned_original_df = original_df[["Title", "Year", "Short Summary", "Runtime"\
                                   ,"Rating", "Director", "Writers", "Cast"]]

## Task 1: Separate values in the 'Cast' column into diffent rows:

cast = cleaned_original_df["Cast"]

# Splitando by pipe:
cast_split = cast.str.split(pat="|", expand=True)

# Unpivotando the result:
unpivoted_cast_split = cast_split.stack()

# Removing index coming from the columns:
final_cast = unpivoted_cast_split.reset_index(level=1, drop=True)

# Transforming back to DF (it was a Series):
cast_df = pd.DataFrame(final_cast, columns=['Cast'])

# Merging it back to the original DF:
cleaned_original_df = original_df.drop(columns=['Cast']).join(cast_df)

cleaned_original_df.to_csv("test.csv")