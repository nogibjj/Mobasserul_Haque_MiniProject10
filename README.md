[![CICD](https://github.com/nogibjj/Mobasserul_Haque_MiniProject10/actions/workflows/cicd.yml/badge.svg)](https://github.com/nogibjj/Mobasserul_Haque_MiniProject10/actions/workflows/cicd.yml)

# PySpark Data Processing: Airline Safety Dataset

## Project Overview

This project demonstrates data processing using **PySpark** on the **Airline Safety** dataset sourced from [FiveThirtyEight](https://github.com/fivethirtyeight/data). The project performs key operations, including:
- Data extraction from a remote source.
- Loading and inspecting the dataset.
- Running SQL queries for specific insights.
- Transforming the dataset to derive additional insights.

By leveraging PySpark, the project showcases the scalability of distributed data processing, enabling efficient manipulation and analysis of large datasets.

---
## Directory Structure

```
├── .devcontainer
│   ├── devcontainer.json
│   └── Dockerfile
├── .github
│   └── workflows
│       └── cicd.yml
├── data
│   └── airline-safety.csv
├── myLib
│   └── lib.py
├── .coverage
├── .gitignore
├── Databricks_execution.PNG
├── main.py
├── Makefile
├── pyspark_output.md
├── README.md
├── requirements.txt
└── test_main.py

```

---

##  Dataset Description

The dataset contains airline safety statistics for various airlines over two periods: 1985-1999 and 2000-2014. Key columns include:
- `airline`: Airline name.
- `avail_seat_km_per_week`: Available seat kilometers per week.
- `incidents_85_99`, `fatal_accidents_85_99`, `fatalities_85_99`: Safety statistics for 1985-1999.
- `incidents_00_14`, `fatal_accidents_00_14`, `fatalities_00_14`: Safety statistics for 2000-2014.

---

##  PySpark Operations

### 1. Data Loading
The `load_data` function selects key columns and loads the data into a Spark DataFrame for processing.

**Sample Output**:
| airline               | avail_seat_km_per_week | incidents_85_99 | fatal_accidents_85_99 | fatalities_85_99 | incidents_00_14 | fatal_accidents_00_14 | fatalities_00_14 |
|-----------------------|------------------------:|----------------:|----------------------:|-----------------:|----------------:|----------------------:|-----------------:|
| Aer Lingus            |             320906734 |               2 |                     0 |                0 |               0 |                     0 |                0 |
| Aeroflot*             |            1197672318 |              76 |                    14 |              128 |               6 |                     1 |               88 |

---

### 2. Data Description
The `describe` function generates summary statistics, such as mean, standard deviation, and minimum/maximum values for numerical columns.

**Sample Output**:
| summary   | airline    | avail_seat_km_per_week | incidents_85_99 | fatalities_85_99 |
|-----------|------------|-----------------------:|----------------:|-----------------:|
| count     | 56         |               56      |         56      |           56     |
| mean      |            |       1.38462e+09     |          7.18   |         112.41   |

---

### 3. Spark SQL Queries
Queries extract insights using SQL syntax. For example:

**Query**:
```sql
SELECT * FROM AirlineSafety WHERE incidents_85_99 > 10
```
|    | airline                    |   avail_seat_km_per_week |   incidents_85_99 |   fatal_accidents_85_99 |   fatalities_85_99 |   incidents_00_14 |   fatal_accidents_00_14 |   fatalities_00_14 |
|---:|:---------------------------|-------------------------:|------------------:|------------------------:|-------------------:|------------------:|------------------------:|-------------------:|
|  0 | Aeroflot*                  |               1197672318 |                76 |                      14 |                128 |                 6 |                       1 |                 88 |
|  1 | Air France                 |               3004002661 |                14 |                       4 |                 79 |                 6 |                       2 |                337 |
|  2 | American*                  |               5228357340 |                21 |                       5 |                101 |                17 |                       3 |                416 |
|  3 | China Airlines             |                813216487 |                12 |                       6 |                535 |                 2 |                       1 |                225 |
|  4 | Delta / Northwest*         |               6525658894 |                24 |                      12 |                407 |                24 |                       2 |                 51 |
|  5 | Ethiopian Airlines         |                488560643 |                25 |                       5 |                167 |                 5 |                       2 |                 92 |
|  6 | Korean Air                 |               1734522605 |                12 |                       5 |                425 |                 1 |                       0 |                  0 |
|  7 | United / Continental*      |               7139291291 |                19 |                       8 |                319 |                14 |                       2 |                109 |
|  8 | US Airways / America West* |               2455687887 |                16 |                       7 |                224 |                11 |                       2 |                 23 |

### 4. Data Transformation

The `example_transform` function adds derived columns, such as:

- **`Total_Incidents`**: Sum of incidents over both periods.
- **`Total_Fatalities`**: Sum of fatalities over both periods.
- **`Risk_Flag`**: A binary flag indicating airlines with over 10 total incidents.

## Setup Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/nogibjj/Mobasserul_Haque_MiniProject10.git
cd Mobasserul_Haque_MiniProject10
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Run the Project

```bash
python main.py
```
![Databricks_execution](Databricks_execution.PNG)

## Testing

Run the test suite using Pytest:
```bash
pytest test_main.py
```

##  Key Learnings

- Efficiently processed large datasets using PySpark.
- Used Spark SQL for querying and filtering data.
- Applied data transformations to derive new insights.
- Logged operations for reproducibility and traceability.

































