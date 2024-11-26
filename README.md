## Canadian Crop data EDA

### Overview

This project, a **final model task in an IBM course**, involves an exploratory data analysis (EDA) on datasets related to Canadian agriculture and financial exchange rates using `rSQLite` 

### Datasets

1. **Annual Crop Data**:
   - **Description**: Agricultural production measures for principal crops in Canada (1908-2020).
   - **Source**: [StatsCan Data Portal](https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=3210035901)

2. **Monthly Farm Prices**:
   - **Description**: Monthly average prices for various farm products across different Canadian regions.
   - **Source**: Custom preprocessed dataset.

3. **Daily Foreign Exchange Rates (Daily_FX)**:
   - **Description**: Daily average exchange rates for foreign currencies, focused on USD-CAD over the latest four years.
   - **Source**: [Bank of Canada](https://www.bankofcanada.ca/rates/exchange/daily-exchange-rates)

4. **Monthly Foreign Exchange Rates (Monthly_FX)**:
   - **Description**: Monthly average exchange rates for foreign currencies.
   - **Source**: Custom preprocessed dataset.

### Key Findings

#### Farm Price Records
- **Total records**: `2,678`

#### Rye Harvest in Canada (1968)
- **Total hectares harvested**: `274,100`

#### Barley Growth Regions
- **Provinces**: `Alberta` and `Saskatchewan`

#### Date Range for Farm Prices
- **First date**: `1985-01-01`
- **Last date**: `2020-12-01`

#### Expensive Crops
- **Canola**: Only crop with a price ≥ `$350` per metric tonne

#### Crop Yields in Saskatchewan (2000)
- **Barley**: `2,800 KG/ha`
- **Wheat**: `2,200 KG/ha`
- **Rye**: `2,100 KG/ha`
- **Canola**: `1,400 KG/ha`

#### Highest Average Yields Since 2000
- **Barley in Alberta**: `3,450.714 KG/ha`
- **Other notable yields**: Barley and wheat show consistently high yields across multiple regions.

#### Latest Year Wheat Harvest in Canada
- **Total harvested wheat**: `10,017,800` hectares

#### Recent Canola Prices in Saskatchewan
- **Range (CAD)**: `462.88` to `507.33` per metric tonne
- **Range (USD)**: `342.91` to `396.11` per metric tonne

I might update this file if I decide to explore it further which I already did during the time of completing it.

