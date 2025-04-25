<div align="center">
<h3 align="center">Project_Loop</h3>

  <p align="center">
    Project_Loop is a project to build uptime/downtime report efficiently.
    <br />
  </p>
</div>

<!-- TABLE OF CONTENTS -->

## Table of Contents

  <ol>
    <li><a href="#about-the-project">About The Project</a></li>
    <li><a href="#built-with">Built with</a></li>
    <li><a href="#prerequisites">Prerequisites</a></li>
    <li><a href="#installation">Installation</a></li>
    <li><a href="#license">License</a></li>
  </ol>

<!-- ABOUT THE PROJECT -->

## About The Project
A python project, which is capable of ingesting data efficiently using `.parquet` file format, storing in`SQLite` database, and working on the data to produce the required uptime/downtime report.

### Built With


[![Python][Python]][Python-url]
[![FastAPI][FastAPI]][FastAPI-url]
[![Poetry][Poetry]][Poetry-url]
[![SQLite][SQLite]][SQLite-url]


## Prerequisites

To run the project in your local machine, you must have

1. **Python3**: Follow the instructions to [install](https://www.python.org/downloads/)
2. **Poetry**: Follow the instructions to [install](https://python-poetry.org/docs/#installation)

## Installation

Once you finish installation Node.js, follow the commands to setup the project locally on your machine

1. clone the project
   ```sh
   git clone https://github.com/Sandstorm831/project_loop.git
   ```
2. enter the project
   ```sh
   cd project_loop
   ```
3. Install python packages
   ```sh
   poetry install --no-root
   ```

4. Activate virtual env

   ```sh
   eval $(poetry env activate) 
   ```

5. Start the server
   ```sh
    uvicorn src.api:app --reload
   ```
This completes the set-up for this project, all the functionalities present in the application will now be live at `http://127.0.0.1:8000`, to interact with the api, you can use the `/docs` endpoint.

<!-- LICENSE -->

## Algorithm description
The main algorithm flow looks like this: 
1. A schema is defined according to the final format of csv output.
2. Declare a few useful datetime objects and strings prior.
3. Total number of unique store_ids are fetched for pagination.
4. Fetch all the store_ids on that page, and pull all the pings and working hours for all the store_ids fetched.
5. iterate over store_ids, filtering pings and working hours for the store_id, pings are sorted in ascending order by their timestamps.
6. We iterate over all the the timestamps, converted to local-timestamp, and whenever we find an active timestamp, we take a max stretch of 1 hour, 30 minutes before and after the timestamp as active.
7. Anykind of overlapping is dealt by keeping a track of last processed timestamp, and only those time-stretches are taken which lies inside the working hours.
8. We calculate the total working hours of the store
9. We calculate the final report and save it in the root of the project in the name of [report.csv](https://github.com/Sandstorm831/project_loop/blob/master/report.csv).


> You won't see any extrapolation, as I don't think it's needed. I have assumed all the active time-stamps as active, and all the missing and inactive time-stamps as inactive. To understand my thought process, please go through [this](https://github.com/Sandstorm831/project_loop/blob/master/info.txt)

## Results and Improvements
I am able to generate the whole report in `less then 10 seconds` and implement multi-threading. You can find the final report.csv [here](https://github.com/Sandstorm831/project_loop/blob/master/report.csv). There are a few way we can make the code more efficient: 
1. Make custom polars expressions to pipe basic calculation process more efficiently.
2. Incrementally build the final csv file, instead of storing all the data and building at once.

## License

Distributed under the GPL-3.0 license. See [LICENSE](./LICENSE) for more information.


[Python]: https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54
[Python-url]: https://www.python.org/
[FastAPI]: https://img.shields.io/badge/FastAPI-005571?style=for-the-badge&logo=fastapi
[FastAPI-url]: https://fastapi.tiangolo.com/
[Poetry]: https://img.shields.io/badge/Poetry-%233B82F6.svg?style=for-the-badge&logo=poetry&logoColor=0B3D8D
[Poetry-url]: https://python-poetry.org/
[SQLite]: https://img.shields.io/badge/sqlite-%2307405e.svg?style=for-the-badge&logo=sqlite&logoColor=white
[SQLite-url]: https://www.sqlite.org/