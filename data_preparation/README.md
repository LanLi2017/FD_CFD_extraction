# Data Preparation with Duplicate Detection
This project is a utility that helps to automate the execution of [data.prep.dedup.java](https://gitlab.com/data.prep.dedup/data.prep.dedup.java), which is the main project that implements the steps described in the paper "Data Preparation for Duplicate Detection".

## Installation
This project requires Python to be executed. For compatibility reasons we have uploaded the anaconda packages we have used in the resources/environment.yml. Please import it into your own environment, before attempting to execute the scripts.
Such an import can be achieved using: conda env create -f environment.yml

## Execution
The execution can be started using the prepare.py script file.
Discovery of MDs is performed using this tool. Its execution is handled by this project.

## Configuration
Please consult the resources/reference.conf file, which is a HOCON configuration file that allows users to easily add multi-leve configurations in an YML equivalent-way. Database configuration is located at the top, along with paths for the datasets and execution's workspace. Additional datasets can also be added by providing the dataset, gold standard locations, and attributes to consider.