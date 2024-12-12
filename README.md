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

































## Features

- **ETL Operations**: 
  - Extract data from CSV files.
  - Transform and load data into Databricks tables for analysis.
  
- **Data Transformation**: Cleaning and preprocessing of data to ensure consistency and accuracy, including handling missing values and converting data types.

- **Data Loading**: Efficient loading of transformed data into a Databricks table, enabling scalable querying and analysis.

- **Query Operations**:
  - Execute complex SQL queries using JOINs, GROUP BY, HAVING, and UNION.
  - Filter and sort data by employment rates, salary differences, and other attributes.
  
- **Logging and Output**:
  - Query results are outputted in a structured format for easy interpretation.
  - Errors and exceptions are logged during ETL and querying processes.

## Directory Structure

```
├── .devcontainer/
│   ├── devcontainer.json
│   └── Dockerfile
├── .github/
│   └── workflows/cicd.yml
├── data/
│   ├── grad-students.csv
    └── recent_grads.csv
├── myLib/
│   ├── __init__.py
│   ├── __pycache__/
│   ├── extract.py
│   ├── query.py
│   └── transform_load.py
├── .gitignore
├── main.py
├── Makefile
├── query_log.md
├── README.md  
├── requirements.txt
└── test_main.py
```
## Usage
To run the ETL process or execute queries, use the following commands:

### Extract Data
To extract data from the CSV files, run:

```python
python main.py extract
```
### Load Data
To transform and load data into the Databricks database, execute:
```python
python main.py load
```

### Load Data
To transform and load data into the Databricks database, execute:
```python
python main.py load
```
## Execute SQL Query
To run a SQL query against the Databricks database, use:

```python
python main.py query "<your_sql_query>"
```

## Complex SQL query 1:

```sql
SELECT 
    rg.Major, 
    rg.Major_category, 
    rg.Total AS Total_Undergrad_Grads, 
    gs.Grad_total AS Total_Grad_Students, 
    AVG(rg.Unemployment_rate) AS Avg_Undergrad_Unemployment_Rate, 
    AVG(gs.Grad_unemployment_rate) AS Avg_Grad_Unemployment_Rate, 
    AVG(rg.Median) AS Avg_Undergrad_Median_Salary, 
    AVG(gs.Grad_median) AS Avg_Grad_Median_Salary 
FROM 
    RecentGradsDB rg 
JOIN 
    GradStudentsDB gs 
ON 
    rg.Major_code = gs.Major_code 
GROUP BY 
    rg.Major_category, 
    rg.Major, 
    rg.Total, 
    gs.Grad_total 
HAVING 
    AVG(rg.Unemployment_rate) < 0.06 
ORDER BY 
    rg.Total DESC;

```
This SQL query joins two tables, RecentGradsDB and GradStudentsDB, and retrieves aggregate information about undergraduate and graduate employment, salary statistics, and unemployment rates for different majors

The query provides a list of majors along with details such as the total number of undergraduate and graduate students, the average unemployment rates, and the average median salaries for both undergraduate and graduate levels. The results are filtered to include only majors where the average undergraduate unemployment rate is below 6%, and the majors are sorted by the total number of undergraduates in descending order

### Expected output:

This output highlights majors with low unemployment rates and the comparison between undergraduate and graduate outcomes

![SQL_query1_output](SQL_query1_output.PNG)

## Complex SQL query 2:

```sql

SELECT Major, 'Undergrad' AS Degree_Level, Total AS Total_Students 
FROM RecentGradsDB 
WHERE Total > 5000 
UNION 
SELECT Major, 'Graduate' AS Degree_Level, Grad_total AS Total_Students 
FROM GradStudentsDB 
WHERE Grad_total > 5000 
ORDER BY Total_Students DESC;

```

This SQL query combines data from two different tables (`RecentGradsDB` and `GradStudentsDB`) to show majors that have more than 5,000 students at both undergraduate and graduate levels, and it orders the results by the total number of students in descending order.

`SELECT` statement Part1 (**Undergraduate data**):

-Retrieves the Major, assigns the string `'Undergrad'` to the Degree_Level, and selects the total number of undergraduate students (Total) from the `RecentGradsDB` table.

-**Filters** (`WHERE Total > 5000`) to include only majors with more than 5,000 undergraduate students.

`SELECT` statement Part2 (Graduate data):

-Retrieves the Major, assigns the string `'Graduate'` to the Degree_Level, and selects the total number of graduate students (Grad_total) from the `GradStudentsDB` table.

-**Filters** (`WHERE Grad_total > 5000`) to include only majors with more than 5,000 graduate students.

`UNION` operator:

Combines the results from the two SELECT statements, ensuring that any duplicates are removed. 

`ORDER BY` Total_Students DESC:

Orders the combined result set by the total number of students (Total_Students) in descending order, showing majors with the highest total first.

### Expected output:

The output consists of a combined and sorted list of majors that have more than 5,000 students, with each entry labeled according to the degree level. The majors are ordered by the total number of students, showing those with the highest student counts first.

![SQL_query2_output](SQL_query2_output.PNG)

## Testing
run below command to test the script
```python
pytest test_main.py
```

## References 
1. https://github.com/nogibjj/sqlite-lab
2. https://learn.microsoft.com/en-us/azure/databricks/dev-tools/python-sql-connector