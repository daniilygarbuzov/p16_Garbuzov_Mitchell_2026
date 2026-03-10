Final Project repo for FINM 32900 Full Stack Quantitative Finance creating an open-source Python repo for data table generation in Szymanowska, Marta, Frans De Roon, Theo Nijman, and Rob Van Den Goorbergh. “An anatomy of commodity futures risk premia.” The Journal of Finance 69, no. 1 (2014): 453-482

Quick Start

The quickest way to run code in this repo is to use the following steps. First, you must have the conda package manager installed (e.g., via Anaconda). However, I recommend using mamba, via [miniforge] (https://github.com/conda-forge/miniforge) as it is faster and more lightweight than conda. Second, you must have TexLive (or another LaTeX distribution) installed on your computer and available in your path. You can do this by downloading and installing it from here (windows and mac installers). Having done these things, open a terminal and navigate to the root directory of the project and create a conda environment using the following command:

conda create -n blank python=3.12 conda activate blank and then install the dependencies with pip

pip install -r requirements.txt Finally, copy .env.example to .env, add your WRDS username, then run doit

And that's it!

General Directory Structure

The assets folder is used for things like hand-drawn figures or other pictures that were not generated from code. These things cannot be easily recreated if they are deleted.

The _output folder, on the other hand, contains dataframes and figures that are generated from code. The entire folder should be able to be deleted, because the code can be run again, which would again generate all of the contents.

The data_manual is for data that cannot be easily recreated. This data should be version controlled. Anything in the _data folder or in the _output folder should be able to be recreated by running the code and can safely be deleted.

I'm using the doit Python module as a task runner. It works like make and the associated Makefiles. To rerun the code, install doit (https://pydoit.org/) and execute the command doit from the src directory. Note that doit is very flexible and can be used to run code commands from the command prompt, thus making it suitable for projects that use scripts written in multiple different programming languages.

I'm using the .env file as a container for absolute paths that are private to each collaborator in the project. You can also use it for private credentials, if needed. It should not be tracked in Git.

Data and Output Storage

I'll often use a separate folder for storing data. Any data in the data folder can be deleted and recreated by rerunning the PyDoit command (the pulls are in the dodo.py file). Any data that cannot be automatically recreated should be stored in the "data_manual" folder. Because of the risk of manually-created data getting changed or lost, I prefer to keep it under version control if I can. Thus, data in the "_data" folder is excluded from Git (see the .gitignore file), while the "data_manual" folder is tracked by Git.

Output is stored in the "_output" directory. This includes dataframes, charts, and rendered notebooks. When the output is small enough, I'll keep this under version control. I like this because I can keep track of how dataframes change as my analysis progresses, for example.

Of course, the _data directory and _output directory can be kept elsewhere on the machine. To make this easy, I always include the ability to customize these locations by defining the path to these directories in environment variables, which I intend to be defined in the .env file, though they can also simply be defined on the command line or elsewhere. The settings.py is reponsible for loading these environment variables and doing some like preprocessing on them. The settings.py file is the entry point for all other scripts to these definitions. That is, all code that references these variables and others are loading by importing config.

Dependencies and Virtual Environments

Working with pip requirements

conda allows for a lot of flexibility, but can often be slow. pip, however, is fast for what it does. You can install the requirements for this project using the requirements.txt file specified here. Do this with the following command:

pip install -r requirements.txt
The requirements file can be created like this:

pip list --format=freeze
Working with conda environments

The dependencies used in this environment (along with many other environments commonly used in data science) are stored in the conda environment called blank which is saved in the file called environment.yml. To create the environment from the file (as a prerequisite to loading the environment), use the following command:

conda env create -f environment.yml
Now, to load the environment, use

conda activate blank
Note that an environment file can be created with the following command:

conda env export > environment.yml
However, it's often preferable to create an environment file manually, as was done with the file in this project.

Also, these dependencies are also saved in requirements.txt for those that would rather use pip. Also, GitHub actions work better with pip, so it's nice to also have the dependencies listed here. This file is created with the following command:

pip freeze > requirements.txt
Other helpful conda commands

Create conda environment from file: conda env create -f environment.yml
Activate environment for this project: conda activate blank
Remove conda environment: conda remove --name blank --all
Create blank conda environment: conda create --name myenv --no-default-packages
Create blank conda environment with different version of Python: conda create --name myenv --no-default-packages python Note that the addition of "python" will install the most up-to-date version of Python. Without this, it may use the system version of Python, which will likely have some packages installed already.
mamba and conda performance issues

Since conda has so many performance issues, it's recommended to use mamba instead. I recommend installing the miniforge distribution. See here: https://github.com/conda-forge/miniforge

Formatting

This project uses Ruff for linting and formatting Python code.

# Auto-fix linting issues (e.g., unused imports, undefined names)
ruff check . --fix

# Format code (consistent style, spacing, line length)
ruff format .

# Sort imports, then fix linting issues, then format
ruff format . && ruff check --select I --fix . && ruff check --fix .
ruff check --fix applies safe auto-fixes for linting violations
ruff format formats code similar to Black
--select I targets only import sorting rules (isort-compatible)

