# Retail Access vs. Social Cost: Store Locations, Sales Trends, and the Measured Social Impact of Legal Cannabis in Canada

## 🎯 Project Goal

This project undertakes an in-depth, multi-faceted analysis of the legalized cannabis market in Canada. It focuses on the interplay between **retail infrastructure (store locations)**, **economic performance (sales trends)**, and **social impact (cannabis-related crime data)** since legalization.

The primary objective is to synthesize disparate public data sources from Health Canada and Statistics Canada into a cohesive, interactive analytical report. This report will address key questions about the rollout of legalized cannabis, its commercial footprint, and its measurable impact on crime statistics across Canadian provinces and territories.

---

## 🏛️ Project Context

This project is a requirement for the **SEP 6DA3: Data Analytics and Big Data** course at **McMaster University**.

---

## 👥 Contributors

| Name                        | Student ID | Email                | GitHub Handle                                     |
| :-------------------------- | :--------- | :------------------- | :------------------------------------------------ |
| Moosani, Iliyan             | 400587184  | moosanii@mcmaster.ca | [`iliyanm10`](https://github.com/iliyanm10)       |
| Parekh, Aayushi Chintan     | 400678333  | pareka13@mcmaster.ca | [`aayushi2812`](https://github.com/aayushi2812)   |
| Patel, Jainish Shaileshbhai | 400550277  | patej118@mcmaster.ca | [`polonium31`](https://github.com/polonium31)     |
| Patel, Mayur Kalabhai       | 400552080  | patem245@mcmaster.ca | [`mayur045`](https://github.com/mayur045)         |
| Patel, Priyaben             | 400676356  | patep280@mcmaster.ca | [`perry0716`](https://github.com/perry0716)       |
| Virani, Jenil Salimbhai     | 400591994  | viranj1@mcmaster.ca  | [`jenil-virani`](https://github.com/jenil-virani) |

---

## 📊 Data Sources

The analysis relies on four primary publicly available data sources provided by the Government of Canada and Statistics Canada. All raw data is stored in the `data/01_raw_data` directory.

| Data Source                                 | Source URL                                                                                                                                                              | Content Summary                                                                                                                                         |
| :------------------------------------------ | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Store Locations** (Provincial Frameworks) | [Health Canada Link](https://www.canada.ca/en/health-canada/services/drugs-medication/cannabis/laws-regulations/provinces-territories.html)                             | Regulatory frameworks and links to provincial/territorial lists of licensed cannabis store operators. **Source for the master store location dataset.** |
| **Cannabis Sales**                          | [StatCan - Cannabis Sales](https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1010016401)                                                                            | Aggregate monthly cannabis sales data by province and territory.                                                                                        |
| **Monthly Retail Trade Sales**              | [StatCan - Retail Trade](https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=2010005601)                                                                              | Broader monthly retail trade data for context and normalization.                                                                                        |
| **Cannabis-Related Crimes (National)**      | [StatCan - Crime Stats](https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=3510017701)                                                                               | Statistics on offenses under the Cannabis Act.                                                                                                          |
| **Cannabis-Related Crimes (Local)**         | [Toronto Police Data](https://data.torontopolice.on.ca/datasets/ea0cfecdb1de416884e6b0bf08a9e195_0/explore) / [Vancouver Police Data](https://vpd.ca/crime-statistics/) | Local crime data for city-level density analysis.                                                                                                       |

---

## 💻 Analytical Views (Key Deliverables)

The analysis will focus on generating the following key views and insights:

1.  **Store Locations Master List:** A compiled and geo-coded master list of all licensed stores (Province, City, Postal Code) for centralization and public release.
2.  **Crime Trends Visualization:** Temporal analysis of cannabis-related crimes (by offense type) since legalization, using time-series charts.
3.  **Crime vs. Density Correlation:** City-level comparison of normalized store density (per capita/km²) against crime rates to quantify retail saturation impact.
4.  **Location Mapping:** Interactive city-level maps displaying precise store locations to provide spatial context for the density analysis.

---

## 📁 Repository Structure

<!-- TREEVIEW START -->

```
├── Dashboard Visuals # Images of different Dashboard Views.
├── data/
│   ├── 01_raw_data/              # Original, untouched data files (read-only)
│   │   ├── 01_store_locations/   # Raw data for provincial/territorial store lists.
│   │   ├── 02_cannabis_sales/    # Raw aggregate monthly sales data (StatCan).
│   │   ├── 03_retail_trade/      # Raw monthly retail trade data for normalization (StatCan).
│   │   ├── 04_crime_data/        # Raw national Cannabis Act offense data (StatCan).
│   │   └── 05_crime_by_city_data/ # Raw localized crime data (Toronto, Vancouver, etc.).
│   ├── 02_processed_data/    # Intermediate data (cleaned, aggregated, geo-coded, merged).
│   └── 03_final_outputs/     # Final datasets ready for use (e.g., master store list for Kaggle).
├── notebooks/                # Jupyter notebooks for modular analysis and reporting.
│    ├── data_ingestion_cleaning/  # Notebooks focused only on cleaning and initial processing.
│    │   ├── 01_store_locations_data_ingestion_cleaning.ipynb
│    │   ├── 02_cannabis_sales_data_ingestion_cleaning.ipynb
│    │   ├── 03_retail_trade_data_ingestion_cleaning.ipynb
│    │   ├── 04_crime_data_high_level_data_ingestion_cleaning.ipynb
│    │   └── 05_crime_by_city_data_ingestion_cleaning.ipynb
│    ├── exploratory_analysis/   # Notebooks focused only on visualization and descriptive stats.
│    │   ├── 01_store_locations_exploratory_analysis.ipynb
│    │   ├── 02_cannabis_sales_exploratory_analysis.ipynb
│    │   ├── 03_retail_trade_exploratory_analysis.ipynb
│    │   ├── 04_crime_data_high_level_exploratory_analysis.ipynb
│    │   └── 05_crime_by_city_data_exploratory_analysis.ipynb
├── src/                      # Reusable Python code (functions, classes, pipeline scripts)
│   └── data_pipeline.py      # Main script to run all data cleaning/processing steps automatically.
├── reports/                  # Final project deliverables and presentations.
│   ├── dashboards/           # Interactive visualization files.
│   │   └── project_dashboard.pbix # The Power BI dashboard file.
│   └── final_report.pdf      # The final written analysis and findings.
├── .gitignore                # Files/folders Git should ignore (e.g., environment files, large data).
└── README.md                 # Project overview and documentation.
```

<!-- TREEVIEW END -->
