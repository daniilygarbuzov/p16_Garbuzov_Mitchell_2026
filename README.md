Final Project repo for FINM 32900 Full Stack Quantitative Finance creating an open-source Python repo for data table generation in Szymanowska, Marta, Frans De Roon, Theo Nijman, and Rob Van Den Goorbergh. “An anatomy of commodity futures risk premia.” The Journal of Finance 69, no. 1 (2014): 453-482

Quick Start

The quickest way to run code in this repo is to use the following steps. First, you must have the conda package manager installed (e.g., via Anaconda). However, I recommend using mamba, via [miniforge] (https://github.com/conda-forge/miniforge) as it is faster and more lightweight than conda. Second, you must have TexLive (or another LaTeX distribution) installed on your computer and available in your path. You can do this by downloading and installing it from here (windows and mac installers). Having done these things, open a terminal and navigate to the root directory of the project and create a conda environment using the following command:

conda create -n blank python=3.12 conda activate blank and then install the dependencies with pip

pip install -r requirements.txt Finally, copy .env.example to .env, add your WRDS username, then run doit

And that's it!
