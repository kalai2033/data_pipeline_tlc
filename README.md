<div id="top"></div>

# Data pipeline with  NYC TLC Dataset

<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
      <ul>
        <li><a href="#built-with">Built With</a></li>
      </ul>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
        <li><a href="#installation">Installation</a></li>
      </ul>
    </li>
    <li><a href="#usage">Usage</a></li>
    <li><a href="#documentation">Documentation</a></li>
    <li><a href="#contact">Contact</a></li>
  </ol>
</details>


## About The Project

<div id="about-the-project">
This repository contains code for the data pipeline to process NYC TLC Dataset.
It returns the average trip for each month and its rolling mean. The following 
is the project structure:

### A typical top-level directory layout

    .
    ├── data                   # data for unit test
    ├── jupyter notebooks      # Solved jupyter notebook 
    ├── src                    # Source file (data_pipeline.py)
    ├── tests                  # unit tests
    ├── config.yaml            # settings for app
    ├── requirements.txt       # Dependencies
    └── README.md
</div>

### Built With
<div id="built-with">

This project is built using python 3.8 and dask primarily. The major
dependencies are specified in the requirements.txt file.
</div>

<p align="right">(<a href="#top">back to top</a>)</p>


## Getting Started
<div id="getting-started"></div>

### Prerequisites
<div id="prerequisites">
Dask is a flexible parallel computing library, and it helps to load the big datasets like NYC TLC dataset. It can be installed with
pip using following command.

  ```sh
  python -m pip install "dask[complete]"
```
</div>

### Installation
<div id="installation">
All the dependencies required for the project can be installed using the following
command:

 ```sh
  python -m pip install -r requirements.txt
```
</div>
<p align="right">(<a href="#top">back to top</a>)</p>


## Usage

<div id="usage">
To run the data pipeline, navigate to the src folder and execute the following
command.

```sh
  python data_pipeline.py
  ```

To run the unit tests, navigate to the root folder and execute the following
command.
 ```sh
  python -m unittest tests/test_data_pipeline.py
               
            (or)
                      
  python -m unittest discover
```

Note: Please remove the break statement in data_pipeline.py to calculate average trip for all the 
months. Use the config.yaml to change the configurations of the pipeline.
</div>
<p align="right">(<a href="#top">back to top</a>)</p>

### Documentation
<div id="documentation">
For scalable solution, this pipeline can be extended to have use big data
solutions like Google's BigQuery or Amazon's Redshift for storage. However,
given that the data is uploaded only once a month, it would make sense to use
a cost-effective storage solution like Postgres if performance is not an issue.
In case the pipeline is to run every day, streaming frameworks such as Apache Kafka
and orchestration tools like Airflow could be easily configured for streaming pipeline.
</div>
<p align="right">(<a href="#top">back to top</a>)</p>

## Contact
<div id="contact">
kalaiselvanpanneerselvam94@gmail.com or open a GitHub issue
</div>
