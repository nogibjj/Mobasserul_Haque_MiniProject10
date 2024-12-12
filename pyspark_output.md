# PySpark Operation Logs

The operation is load data

The truncated output is: 

|    | airline               |   avail_seat_km_per_week |   incidents_85_99 |   fatal_accidents_85_99 |   fatalities_85_99 |   incidents_00_14 |   fatal_accidents_00_14 |   fatalities_00_14 |
|---:|:----------------------|-------------------------:|------------------:|------------------------:|-------------------:|------------------:|------------------------:|-------------------:|
|  0 | Aer Lingus            |                320906734 |                 2 |                       0 |                  0 |                 0 |                       0 |                  0 |
|  1 | Aeroflot*             |               1197672318 |                76 |                      14 |                128 |                 6 |                       1 |                 88 |
|  2 | Aerolineas Argentinas |                385803648 |                 6 |                       0 |                  0 |                 1 |                       0 |                  0 |
|  3 | Aeromexico*           |                596871813 |                 3 |                       1 |                 64 |                 5 |                       0 |                  0 |
|  4 | Air Canada            |               1865253802 |                 2 |                       0 |                  0 |                 2 |                       0 |                  0 |
|  5 | Air France            |               3004002661 |                14 |                       4 |                 79 |                 6 |                       2 |                337 |
|  6 | Air India*            |                869253552 |                 2 |                       1 |                329 |                 4 |                       1 |                158 |
|  7 | Air New Zealand*      |                710174817 |                 3 |                       0 |                  0 |                 5 |                       1 |                  7 |
|  8 | Alaska Airlines*      |                965346773 |                 5 |                       0 |                  0 |                 5 |                       1 |                 88 |
|  9 | Alitalia              |                698012498 |                 7 |                       2 |                 50 |                 4 |                       0 |                  0 |

The operation is describe data

The truncated output is: 

|    | summary   | airline         |   avail_seat_km_per_week |   incidents_85_99 |   fatal_accidents_85_99 |   fatalities_85_99 |   incidents_00_14 |   fatal_accidents_00_14 |   fatalities_00_14 |
|---:|:----------|:----------------|-------------------------:|------------------:|------------------------:|-------------------:|------------------:|------------------------:|-------------------:|
|  0 | count     | 56              |             56           |          56       |                56       |             56     |          56       |               56        |            56      |
|  1 | mean      |                 |              1.38462e+09 |           7.17857 |                 2.17857 |            112.411 |           4.125   |                0.660714 |            55.5179 |
|  2 | stddev    |                 |              1.46532e+09 |          11.0357  |                 2.86107 |            146.691 |           4.54498 |                0.858684 |           111.333  |
|  3 | min       | Aer Lingus      |              1.00197e+09 |           0       |                 0       |              0     |           0       |                0        |             0      |
|  4 | max       | Xiamen Airlines |              9.65347e+08 |           9       |                 8       |             98     |           8       |                3        |            92      |

The operation is Results for condition: incidents_85_99 > 10

The query is SELECT * FROM AirlineSafety WHERE incidents_85_99 > 10

The truncated output is: 

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

The operation is Results for condition: fatal_accidents_00_14 > 5

The query is SELECT * FROM AirlineSafety WHERE fatal_accidents_00_14 > 5

The truncated output is: 

| airline   | avail_seat_km_per_week   | incidents_85_99   | fatal_accidents_85_99   | fatalities_85_99   | incidents_00_14   | fatal_accidents_00_14   | fatalities_00_14   |
|-----------|--------------------------|-------------------|-------------------------|--------------------|-------------------|-------------------------|--------------------|

The operation is transform data

The truncated output is: 

|    | airline               |   avail_seat_km_per_week |   incidents_85_99 |   fatal_accidents_85_99 |   fatalities_85_99 |   incidents_00_14 |   fatal_accidents_00_14 |   fatalities_00_14 |   Total_Incidents |   Total_Fatalities |   Risk_Flag |
|---:|:----------------------|-------------------------:|------------------:|------------------------:|-------------------:|------------------:|------------------------:|-------------------:|------------------:|-------------------:|------------:|
|  0 | Aer Lingus            |                320906734 |                 2 |                       0 |                  0 |                 0 |                       0 |                  0 |                 2 |                  0 |           0 |
|  1 | Aeroflot*             |               1197672318 |                76 |                      14 |                128 |                 6 |                       1 |                 88 |                82 |                216 |           1 |
|  2 | Aerolineas Argentinas |                385803648 |                 6 |                       0 |                  0 |                 1 |                       0 |                  0 |                 7 |                  0 |           0 |
|  3 | Aeromexico*           |                596871813 |                 3 |                       1 |                 64 |                 5 |                       0 |                  0 |                 8 |                 64 |           0 |
|  4 | Air Canada            |               1865253802 |                 2 |                       0 |                  0 |                 2 |                       0 |                  0 |                 4 |                  0 |           0 |
|  5 | Air France            |               3004002661 |                14 |                       4 |                 79 |                 6 |                       2 |                337 |                20 |                416 |           1 |
|  6 | Air India*            |                869253552 |                 2 |                       1 |                329 |                 4 |                       1 |                158 |                 6 |                487 |           0 |
|  7 | Air New Zealand*      |                710174817 |                 3 |                       0 |                  0 |                 5 |                       1 |                  7 |                 8 |                  7 |           0 |
|  8 | Alaska Airlines*      |                965346773 |                 5 |                       0 |                  0 |                 5 |                       1 |                 88 |                10 |                 88 |           0 |
|  9 | Alitalia              |                698012498 |                 7 |                       2 |                 50 |                 4 |                       0 |                  0 |                11 |                 50 |           1 |

