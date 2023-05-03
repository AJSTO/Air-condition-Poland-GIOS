## 👨‍💻 Built with
<img src="https://img.shields.io/badge/Python-FFD43B?style=for-the-badge&logo=python&logoColor=blue" /> <img src="https://img.shields.io/badge/Docker-2CA5E0?style=for-the-badge&logo=docker&logoColor=white"/> <img src="https://img.shields.io/badge/Pandas-2C2D72?style=for-the-badge&logo=pandas&logoColor=white" /> <img src="https://www.devagroup.pl/blog/wp-content/uploads/2022/10/logo-Google-Looker-Studio.png" width="100" height="27,5" /> <img src="https://www.scitylana.com/wp-content/uploads/2019/01/Hello-BigQuery.png" width="100" height="27,5" />

##  Descripction about project

### ℹ️Project info

This project is created to visualize data captured from [GIOS API](https://powietrze.gios.gov.pl/pjp/content/api).

The measurement results of all pollutants made available via the API are displayed in the μg/m3 unit.

The dashboard presents the latest pollutant measurements along with the time of the last correct measurement.

The script collects data from the API every hour.

Type of pollutants:
- PM10
- NO2
- O3
- PM2.5
- SO2
- C6H6
- CO

## 🔎 Looker Studio
Link to generated dashboard in Looker: 


[GIOŚ -AIR QUALITY MEASUREMENTS](https://lookerstudio.google.com/reporting/5b78bf5e-6211-438e-989f-c1e4ac8644b8)
![IMG LOOKER](https://github.com/AJSTO/Air-condition-Poland-GIOS/blob/master/images/looker.gif)

## 🗒️Database created in BIGQUERY:

### Database diagram:
![DB DIAGRAM](https://github.com/AJSTO/Air-condition-Poland-GIOS/blob/master/images/database_diagram.png)

### Table station informations:
![IMG TABLE INFORMATIONS](https://github.com/AJSTO/Air-condition-Poland-GIOS/blob/master/images/table_station_information.png)

### Table measurements:
![IMG MEASUREMENTS](https://github.com/AJSTO/Air-condition-Poland-GIOS/blob/master/images/table_measurements.png)

## 🌲 Project tree
```bash
.
├── .gitignore
├── Dockerfile
├── README.md
├── config.yaml
├── credentials.json
├── gios_measurements.py
└── requirements.txt 

```

## 🔑 Setup 
To run properly this project you should set variables in files: 
### ./config.yaml:
- PROJECT_ID: PROJECT_ID  # Project ID in BIGQUERY
- DATASET_NAME: DATASET_ID  # Dataset ID in BIGQUERY
- TABLE_STATIONS: TABLE_STATIONS_ID # Table id with stations information in BIGQUERY
- TABLE_MEASUREMENTS: TABLE_MEASUREMENTS_ID  # Table id with measurements in BIGQUERY
- JSON_KEY_BQ: credentials.json # JSON key with credentials to BIGQUERY Project

Don't forget to add json file with credentials into working directory.

#### Building containter:

Create image of container:
```bash
  $  docker build --tag GIOS_PROJECT .
```
When created, run container:
```bash
  $  docker run --rm GIOS_PROJECT
```

#### Custom query (for Looker) to get lastest pollutant measurements for each sensor:

```ruby
SELECT 
  M.station_id, M.sensor_id, M.param_code, M.datetime, M.value, 
  CONCAT(S.gegrLat, ", ",S.gegrLon) AS coords, 
  CONCAT(S.stationName, ",\nAt time: ", M.datetime) AS informations
FROM (
  SELECT 
    station_id, sensor_id, param_code, MAX(datetime) AS datetime
  FROM 
    `YOUR_PROJECT_ID.DATASET_ID.TABLE_MEASUREMENTS_ID`
  GROUP BY 
    station_id, sensor_id, param_code
) AS T
JOIN `YOUR_PROJECT_ID.DATASET_ID.TABLE_MEASUREMENTS_ID` M
ON T.station_id = M.station_id AND T.sensor_id = M.sensor_id 
AND T.param_code = M.param_code AND T.datetime = M.datetime
JOIN `YOUR_PROJECT_ID.DATASET_ID.TABLE_STATIONS_INFO_ID` S
ON M.station_id = S.id
```


