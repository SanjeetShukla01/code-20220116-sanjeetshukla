## Python BMI Calculator Coding Challenge V7


[<img src="https://github.com/jackshukla7/code-20220116-sanjeetshukla/actions/workflows/publish.yml/badge.svg">](https://github.com/jackshukla7/code-20220116-sanjeetshukla/actions)

For problem description see this pdf file:
![file.pdf](Python_BMI_Calculator_Challenge_Rubeus.pdf)

## Description
This repository contains pyspark pipeline to Health Risk from json input data.
Input Data Contains following three fields
1. Gender
2. HeightCm
3. WeightKg

Based on above 3 columns, this pyspark code is deriving follwing insights:
1. BMI
2. BMI Category 
3. Health Risk

For calculation of BMI and Health Risk, following formula is used based on pdf file given here:

![img.jpg](resources/image/rules_for_derivation.jpg)


#### This application is using spark to process data and derive desired field. As spark is in memory distributed computing system. This application can scale to millions and billions of records. 
This application can be packaged and delivered to on-premise or cloud spark cluster environment using CD pipeline.


## Direcotry Structure
```
├── app.py           
├── src
│    ├──etl
│    │    ├──ingest.py
│    │    ├──load.py
│    │    ├──transform.py
│    ├──utils
│    │    ├──column_constants.py
│    │    ├──utils.py
├── test
│    ├──test_transform.py
│    ├──test_utils.py
├── config
│    ├──logging.conf
│    ├──pipeline.ini
├── resources
│    ├──image
│    ├──input
│    ├──output
├── requirements.txt
├── setup.py
├── LICENCE
├── README.md
├── .gitignore
├── .github
│    ├──.workflow
│    │    ├──publish.yml
│    │    ├──release.yml
│    ├── pull_request_template.md

```

### How to run this application:
1.  Clone this repository
2. Go to the project folder. Run the below command in terminal.

```pip install -r requirements.txt```
3. Go to project direcotry and app.py
4.  Run the application.
5. Output data will be available in 'output' folder.

Unit test for the application done using unittest library.
```
coverage run --source -m unittest discover && coverage report
```

## Features:
- Config Management             -- Present
- Logging                       -- Present
- Modular code                  -- Present
- Continuous Integration        -- Present
- Unit Tests                    -- Present
- Code Coverage Report          -- Present
- Continuous Deployment         -- TBD

