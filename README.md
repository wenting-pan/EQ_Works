# EQ Works Data Solution

The solutions for Problems 1 - 4a can be found in the `data` folder within `solution.py`. The solution for problem 4b
can be found in `pipeline_dependency/pipeline_dependency.py`

There is a `Pipfile` in the `data` folder that can be used to install the dependencies used in the project.
Alternatively, you may manually run the following installs where needed:

```
pip install pyspark
pip install geopy
```

#####*Note: `geopy` needs to be installed in both master and worker nodes

`solution.py` can be run via a `spark-submit`:
```
spark-submit --master <spark-master> /tmp/data/solution.py 
``` 
It will assume that the `DataSample.csv` and `POIList.csv` files can be
found in `/tmp/data/`.

An explanation of the mathematical model created for Problem 4a can be found in `solution.md`. The bonus for that
problem can be found in `bonus.txt`.
