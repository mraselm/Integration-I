library(RPostgres)
library(DBI)
library(tidyverse)
library(httr2)
library(lubridate)
## Investigate which symbols we can search for ---------------
req <- request("https://alpha-vantage.p.rapidapi.com") %>%
  req_url_path("query") %>%
  req_url_query("keywords" = "Apple",
                "function" = "SYMBOL_SEARCH",
                "datatype" = "json") %>%
  req_headers('X-RapidAPI-Key' = '3e5584488fmshbf9fa2c43b183f1p1f04e2jsne5bd2854169b',
              'X-RapidAPI-Host' = 'alpha-vantage.p.rapidapi.com') 
resp <- req %>% 
  req_perform() 

symbols <- resp %>%
  resp_body_json()

symbols$bestMatches[[1]]
symbols$bestMatches[[2]]

## Extract and Transform  ------------------------------------------
# Extract data from Alpha Vantage
req <- request("https://alpha-vantage.p.rapidapi.com") %>%
  req_url_path("query") %>%
  req_url_query("interval" = "60min",
                "function" = "TIME_SERIES_INTRADAY",
                "symbol" = "AAPL",
                "datatype" = "json",
                "output_size" = "compact") %>%
  req_headers('X-RapidAPI-Key' = '3e5584488fmshbf9fa2c43b183f1p1f04e2jsne5bd2854169b',
              'X-RapidAPI-Host' = 'alpha-vantage.p.rapidapi.com') 
resp <- req %>% 
  req_perform() 

dat <- resp %>%
  resp_body_json()

# TRANSFORM timestamp to UTC time
timestamp <- lubridate::ymd_hms(names(dat$`Time Series (60min)`), tz = "US/Eastern")
timestamp <- format(timestamp, tz = "UTC-04")
# Prepare data.frame to hold results
df <- tibble(timestamp = timestamp,
             open = NA, high = NA, low = NA, close = NA, volume = NA)
# TRANSFORM data into a data.frame
for (i in 1:nrow(df)) {
  df[i,-1] <- as.data.frame(dat$`Time Series (60min)`[[i]])
}

df <- dplyr::select(df, timestamp, close, volume)
# Create table in Postgres ------------------------------------------------
# Put the credentials in this script
# Never push credentials to git!! --> use .gitignore on .credentials.R
source(".credentials.R")
# Function to send queries to Postgres
source("psql_queries.R")
# Create a new schema in Postgres on docker
psql_manipulate(cred = cred_psql_docker, 
                query_string = "CREATE SCHEMA class_prac_appl;")
# Create a table in the new schema 
psql_manipulate(cred = cred_psql_docker, 
                query_string = 
                  "create table class_prac_appl.prices (
	id serial primary key,
	timestamp timestamp(0) without time zone ,
	close numeric(30,4),
	volume numeric(30,4));")

# LOAD price data -------------------------------
psql_append_df(cred = cred_psql_docker,
               schema_name = "class_prac_appl",
               tab_name = "prices",
               df = df)

# Check results -----------------------------------------------------------
# Check that we can fetch the data again
psql_select(cred = cred_psql_docker, 
            query_string = 
              "select * from class_prac_appl.prices")
# If you wish, your can delete the schema (all the price data) from Postgres 
psql_manipulate(cred = cred_psql_docker, 
                query_string = "drop SCHEMA class_prac_appl cascade;")
