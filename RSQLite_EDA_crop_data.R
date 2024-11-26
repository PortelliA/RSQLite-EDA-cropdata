# RSQLite EDA

#To complete the assignment problems in this notebook you will be using subsetted snapshots of one dataset from Statistics Canada, and one from the Bank of Canada. 
#The links to the prepared datasets are provided in the next section; the interested student can explore the landing pages for the source datasets as follows:
# Canadian Principal Crops Data: https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=3210035901"
# Contains agricultural production measures for principal crops in Canada.
# Breakdown by province and territory from 1908 to 2020.
# Bank of Canada Daily Average Exchange Rates: https://www.bankofcanada.ca/rates/exchange/daily-exchange-rates
# Daily average exchange rates for multiple foreign currencies, in CAD.
# Includes the latest four years of data, updated daily at 16:30 ET.
# Snapshot used includes only USD-CAD exchange rates.

# Read the CSV files
Annual_Crop_Data <- read.csv("Annual_Crop_Data.csv")
Monthly_Farm_Prices <- read.csv("Monthly_Farm_Prices.csv")
Daily_FX <- read.csv("Daily_FX.csv")
Monthly_FX <- read.csv("Monthly_FX.csv")

# Initial check
summary(Annual_Crop_Data)
summary(Monthly_Farm_Prices)
summary(Daily_FX)
summary(Monthly_FX)


con <- dbConnect(RSQLite::SQLite(), dbname = "my_database.sqlite")

#Creating connection & placing files inside


dbExecute(con, "CREATE TABLE IF NOT EXISTS CROP_DATA (
    CD_ID INTEGER NOT NULL,
    YEAR DATE NOT NULL,
    CROP_TYPE VARCHAR(20) NOT NULL,
    GEO VARCHAR(20) NOT NULL,
    SEEDED_AREA INTEGER NOT NULL,
    HARVESTED_AREA INTEGER NOT NULL,
    PRODUCTION INTEGER NOT NULL,
    AVG_YIELD NUMERIC NOT NULL,
    PRIMARY KEY (CD_ID)
)")

dbExecute(con, "CREATE TABLE IF NOT EXISTS FARM_PRICES (
    CD_ID INTEGER NOT NULL,
    DATE DATE NOT NULL,
    CROP_TYPE VARCHAR(20) NOT NULL,
    GEO VARCHAR(20) NOT NULL,
    PRICE_PRERMT NUMERIC NOT NULL,
    PRIMARY KEY (CD_ID)
)")

dbExecute(con, "CREATE TABLE IF NOT EXISTS DAILY_FX (
    DFX_ID INTEGER NOT NULL,
    DATE DATE NOT NULL,
    FXUSDCAD NUMERIC NOT NULL,
    PRIMARY KEY (DFX_ID)
)")

dbExecute(con, "CREATE TABLE IF NOT EXISTS MONTHLY_FX (
    DFX_ID INTEGER NOT NULL,
    DATE DATE NOT NULL,
    FXUSDCAD NUMERIC NOT NULL,
    PRIMARY KEY (DFX_ID)
)")

dbWriteTable(con, "CROP_DATA", Annual_Crop_Data, overwrite = TRUE)
dbWriteTable(con, "FARM_PRICES", Monthly_Farm_Prices, overwrite = TRUE)
dbWriteTable(con, "DAILY_FX", Daily_FX, overwrite = TRUE)
dbWriteTable(con, "MONTHLY_FX", Monthly_FX, overwrite = TRUE)


# Check how the tables look
croptable <- dbGetQuery(con, "PRAGMA table_info(CROP_DATA)")
print(croptable)
farmtable <- dbGetQuery(con, "PRAGMA table_info(FARM_PRICES)")
print(farmtable)
dailytable <- dbGetQuery(con, "PRAGMA table_info(DAILY_FX)")
print(dailytable)
monthlytable <- dbGetQuery(con, "PRAGMA table_info(MONTHLY_FX)")
print(monthlytable)

  
# How many records are in the farm price dataset?
results1 <- dbGetQuery(con, "SELECT COUNT(*) FROM FARM_PRICES")
print(results1)
  
# Which geographies are included in the farm prices dataset?
results2 <- dbGetQuery(con, "SELECT DISTINCT GEO FROM FARM_PRICES")
print(results2)


# How many hectares of Rye were harvested in Canada in 1968?
rye_harvest_1968 <- dbGetQuery(con, "
  SELECT SUM(HARVESTED_AREA) as hectares_harvested 
  FROM CROP_DATA 
  WHERE CROP_TYPE = 'Rye' 
  AND GEO = 'Canada' 
  AND strftime('%Y', YEAR) = '1968'
")
print(rye_harvest_1968

  
# Query & Desplay the first 6 rows of the farm prices table for Rye
  ```{r}
results3 <- dbGetQuery(con, "SELECT * FROM FARM_PRICES WHERE CROP_TYPE = 'Rye' LIMIT 6")
print(results3)

  
# Which provinces grew Barley?
results4 <- dbGetQuery(con, "SELECT DISTINCT GEO FROM FARM_PRICES WHERE CROP_TYPE = 'Barley'")
print(results4)

  
# Find the first & last dates for the farm prices data
first_date <- dbGetQuery(con, "SELECT MIN(DATE) AS first_date FROM FARM_PRICES")
print(first_date)
last_date <- dbGetQuery(con, "SELECT MAX(DATE) AS last_date FROM FARM_PRICES")
print(last_date)
  

## Which crops have ever reached a farm pric greater than or equal to $350 per metric tonne
expensive_crops <- dbGetQuery(con, "SELECT DISTINCT CROP_TYPE FROM FARM_PRICES WHERE PRICE_PRERMT >= 350")
print(expensive_crops)

  
## Rank the crop types harvested in Saskatchewan in the year 2000 by their average yield. Which crop performed best?
list_order_crops <- dbGetQuery(con, "SELECT strftime('%Y', YEAR) AS YEAR, CROP_TYPE, AVG_YIELD 
                               FROM CROP_DATA 
                               WHERE GEO = 'Saskatchewan' AND strftime('%Y', YEAR) = '2000' ORDER BY AVG_YIELD DESC
                               ")
print(list_order_crops)

 
##Rank the crops and geographies by their average yield (KG per hectare) since the year 2000. Which crop and province had the highest average yield since the year 2000
highest_yields <- dbGetQuery(con, "
  SELECT CROP_TYPE, GEO, AVG(AVG_YIELD) AS average_yield
  FROM CROP_DATA
  WHERE STRFTIME('%Y', YEAR) >= '2000'
  GROUP BY CROP_TYPE, GEO
  ORDER BY average_yield DESC
                             ")

print(highest_yields)


  
  ## Use a subquery to determine how much wheat was harvested in Canada in the most recent year of the data
wheat_harvest <- dbGetQuery(con, "
    SELECT SUM(HARVESTED_AREA) AS total_harvested_wheat
    FROM CROP_DATA
    WHERE CROP_TYPE = 'Wheat' AND GEO = 'Canada' AND strftime('%Y', YEAR) = (
        SELECT strftime('%Y', MAX(YEAR)) FROM CROP_DATA
    )
")
print(wheat_harvest)

  
  ## Use an implicit inner join to calculate the monthly price per metric tonne of Conola grown in Saskatchewan in both Canadian & US dollars. Display the most recent 6 months of the data
canola_prices <- dbGetQuery(con, "
    SELECT c.DATE, c.PRICE_PRERMT AS price_CAD, (c.PRICE_PRERMT / f.FXUSDCAD) AS price_USD
    FROM FARM_PRICES AS c, MONTHLY_FX AS f
    WHERE c.CROP_TYPE = 'Canola' AND c.GEO = 'Saskatchewan' AND c.DATE = f.DATE
    ORDER BY c.DATE DESC
    LIMIT 6
")
print(canola_prices)





```{r }
dbDisconnect(con)
```