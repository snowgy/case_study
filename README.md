# Installation

The library I use to display and filter the dataframe is only compatible with python 3.9 right now. If you don't have python3.9, please follow the subsequent procedure to install.

## Install miniconda based on the official document:
https://docs.conda.io/projects/conda/en/latest/user-guide/install/macos.html

```bash
# Create a new conda environment for the case_study:
conda create --name temp python=3.9
conda activate temp
```

## Install Dependencies
```bash 
# Install jupyter notebook by conda:
conda install -c conda-forge notebook
```

```bash
pip install 'ipywidgets~=7.0'

pip install ipython

pip install networkx

pip install pandas
```
### Install QGrid (library to visualize the dataframe)
```bash
pip install qgrid

# enable extensions
jupyter nbextension enable --py --sys-prefix qgrid

jupyter nbextension enable --py --sys-prefix widgetsnbextension
```


## Run the jupyter notebook:
```bash
jupyter notebook
```
