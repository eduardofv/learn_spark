# Learning Spark & Databricks

**copiar en ./work-dir los archivos de datos para pruebas**

los archivos de datos son jsonl con datos anidados

```
docker run --rm -p 4040:4040 -v ./work-dir:/opt/spark/work-dir --name spark -it apache/spark-py  /opt/spark/bin/pyspark
docker run --rm -p 8889:8888 -p 4040:4040 -v $(pwd)/work-dir:/home/jovyan/work --name sparknb -it jupyter/pyspark-notebook
```

https://github.com/jupyter/docker-stacks/blob/main/pyspark-notebook/Dockerfile

