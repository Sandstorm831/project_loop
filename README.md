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
   poetry install
   ```

4. Activate virtual env

   ```sh
   poetry shell
   ```

5. Start the server
   ```sh
    uvicorn src.api:app --reload
   ```
This completes the set-up for this project, all the functionalities present in the application will now be live at `http://127.0.0.1:8000`, to interact with the api, you can use the `/docs` endpoint.

<!-- LICENSE -->


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