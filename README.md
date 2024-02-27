## How to test

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python setup.py bdist_egg

export PYSPARK_PYTHON=$VIRTUAL_ENV/bin/python && \
export PYSPARK_DRIVER_PYTHON=$VIRTUAL_ENV/bin/python && \
spark-submit \
  --master "local[3]" \
  --driver-memory 512m \
  --executor-memory 512m \
  --py-files dist/spark_multithread-0.1.0-py3.10.egg \
  sample.py
```