# Spock Airflow
Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. 

## Components
![image](https://github.com/xorddotcom/spock-airflow/assets/60582132/256bf327-396f-4a77-8a1b-462ccdb38412)

## Flowchart
![image](https://github.com/xorddotcom/spock-airflow/assets/60582132/84b8c901-365b-479e-a015-95496013e0bf)

## Configurator Dag
![image](https://github.com/xorddotcom/spock-airflow/assets/60582132/7f94c6b0-3043-42bd-a08f-f324d42c2de0)

## Builder Dag
![image](https://github.com/xorddotcom/spock-airflow/assets/60582132/e98b34b7-5255-407d-821f-7321cf53662c)

## Operator Dag
![image](https://github.com/xorddotcom/spock-airflow/assets/60582132/720c97ec-6be1-4858-8684-c49f482fd4d5)

## Protocol Dag
![image](https://github.com/xorddotcom/spock-airflow/assets/60582132/a0bedd61-0aff-4679-ba3d-94b55a517dd7)

## Folder Structure
```js
include
├── dbt
│   └── models
│   │   └── protocol_positions
│   │   │   └── [PROTOCOL_NAME]
│   │   │   │   └── parse //for abis
│   │   │   │   └── transform //for transformations
│   │   │   │   └── check //for data quality checks
│   │   │   │   └── sql //for custom UDFs
```

## Transformation Stages
```js
  -  Extraction //Extracting and Filtering Protocol Logs from Public Datasets.
  -  Integration //Consolidating and Merging New Data with Previously Transformed Records.
  -  Synthesis //Synthesizing the Integrated Data and Generating Wallet Positions.
```
