#!/usr/bin/env python3

"""Programa para receber dados de uma database do Kaggle (.csv), e upar para 
uma database, normalizando em quatro tabelas: fMovies, dDirectors, dActors, 
dWriters.
"""

__version__ = "0.1.0"

import psycopg2
import pandas as pd
from sqlalchemy import create_engine

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


    return cur, conn


def upload_tables_to_postgres(dim_director, dim_writers, dim_cast, fact_movies):
    # Creating connection string:
    conn_string = "postgresql://postgres:PI314159@localhost:5433/movies"

    # Creating SQLAlchemy engine
    engine = create_engine(conn_string)

    # Uploadind dimension tables
    dim_director.to_sql('dim_director', engine, index=False, if_exists='replace')
    dim_writers.to_sql('dim_writers', engine, index=False, if_exists='replace')
    dim_cast.to_sql('dim_cast', engine, index=False, if_exists='replace')

    # Uploading fact table
    fact_movies.to_sql('fact_movies', engine, index=True, if_exists='replace')

    # Committting the transaction
    engine.dispose()  # Closing the connection

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


## Task 2: Creating IDs for Director, Writers and Cast

cleaned_original_df['Director_ID'] = pd.factorize(cleaned_original_df['Director'])[0]
cleaned_original_df['Writers_ID'] = pd.factorize(cleaned_original_df['Writers'])[0]
cleaned_original_df['Cast_ID'] = pd.factorize(cleaned_original_df['Cast'])[0]

# Task 3: Creating separate dimension tables for Director, Writers, and Cast

# reset_index() is used to drop the additional index that is created automatically with the df
dim_director = cleaned_original_df[['Director_ID', 'Director']].drop_duplicates().reset_index(drop=True)
dim_writers = cleaned_original_df[['Writers_ID', 'Writers']].drop_duplicates().reset_index(drop=True)
dim_cast = cleaned_original_df[['Cast_ID', 'Cast']].drop_duplicates().reset_index(drop=True)
fact_movies = cleaned_original_df[["Title", "Year", "Short Summary", "Runtime"\
                                   ,"Rating", "Director_ID", "Writers_ID", "Cast_ID"]]


# Calling create_database() function to create the 'movies' database
cur, conn = create_database()

# Calling upload_tables_to_postgres() function to upload tables to the 'movies' database
upload_tables_to_postgres(dim_director, dim_writers, dim_cast, fact_movies)

# Closing the connection
conn.close()