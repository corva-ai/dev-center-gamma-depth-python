# Dev Center Gamma Depth

Dev Center Gamma Depth is intended to provide a fully working example of a simple Dev Center application that receives events, processes data and communicates with the Corva APIs.

## Contents

This repository contains two sample applications that demonstrate the differences between scheduled and stream apps. Both applications provide the same functionality and produce the same output.

* scheduled
   * scheduled app runs periodically based on the incoming drilling data (e.g. every 10 minutes)
   * invoked with scheduler events
   * data records are fetched from the API
* stream
   * stream app runs immediately when new drilling data is received
   * invoked with queued data records

For more details, see [Python SDK](https://github.com/corva-ai/python-sdk)

## Prerequisites

* Python 3.8

## Set up the project

1. Clone the repository onto your computer: `git clone https://github.com/corva-ai/dev-center-gamma-depth-python.git`
2. Navigate to either `scheduled` or `stream` directory
3. Create a new virtual environment: `python3 -m venv venv`
4. Activate virtual environment: `source venv/bin/activate`
5. Install dependencies: `pip3 install -r test_requirements.txt`

## Run tests

```
$ venv/bin/python3 -m pytest tests
```

## Run code linter

```
$ venv/bin/python3 -m flake8
```
