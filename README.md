# Development Environment Setup 

Repository for flywheel data analytics. 

## Pre-requisites 

* Create a file `.env` in the root directory of the project. Within this file, define the following key value pairs. 
    * `ETHERSCAN_API_KEY`=`<YOUR_ETHERSCAN_API_KEY>` (used to initialize an etherscan api client).
    * `INFURA_API_KEY`=`<YOUR_INFURA_API_KEY>` (used to connect to infura RPC endpoints).

## Initial Setup 

1. Ensure that you have [conda](https://conda.io/projects/conda/en/latest/user-guide/install/index.html) installed on your system. 
2. Execute the following shell commands in order. 
    * `conda create -n flywheel python=3.10.0`
        * Creates the python environment. 
    * `conda activate flywheel`
        * Activates the python environment. 
    * `pip install -r requirements.txt`
        * Installs python dependencies into the environment. 
    * `python -m ipykernel install --user --name=flywheel`
        * Installs our conda kernel into jupyter-lab, so we can use the kernel as an execution 
        environment for our notebooks. 
    
## Development 

This repo houses a number of jupyter notebooks, all stored within the `notebooks` directory.

* To start up the dev environment, run `conda activate flywheel`. 
* To close out the environment, run `conda deactivate`.
* Run `jupyter-lab` to open a jupyter lab session in your web browser. Once this is open, you should be able to run any of the notebooks. Make sure that whenever you run a notebook, you're using the `flywheel` kernel which you can select in the upper right corner of the interface. 
