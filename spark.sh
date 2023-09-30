docker run --rm -p 8889:8888 -p 4040:4040 -v $(pwd)/work-dir:/home/jovyan/work -v /Volumes/shared/harvester/:/home/jovyan/work/harvester --name sparknb -it jupyter/pyspark-notebook\n
