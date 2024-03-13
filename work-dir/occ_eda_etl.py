# # OCC daily EDA Transform for time linear and differential analysis
import pandas as pd
import sys

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

RECORD_VERSION = "0.0.2"
OUTPUT_FILENAME_DEFAULT = \
    f"harvester/occ/occ-timeseries-v{RECORD_VERSION}"


def save(filename, data, append=True):
    import json
    
    mode = "w"
    if append:
        mode = "a+"
    fn = f"{filename}.jsonl"
    with open(fn, mode) as fo:
        fo.write(json.dumps(data)+"\n")


output_filename = OUTPUT_FILENAME_DEFAULT
if len(sys.argv) < 2:
    print(f"Usage:  occ-eda-elt.py YYYYMMDD [output_filename]")
    print(f"  output_filename defaults to {output_filename}")
    exit(1)
date_str = sys.argv[1]
if len(sys.argv) > 2:
    output_filename = sys.argv[2]

spark = SparkSession.builder.appName("occ_eda_etl").getOrCreate()

#Input data: full scraped data and categ/subcateg catalogs
base_dir = f"harvester/occ/{date_str}/"
fn = f"{base_dir}/occ-{date_str}.jsonl.gz"
print(f"Reading {fn}")
df = spark.read.json(fn)
categories = spark.read.json(f"{base_dir}/occ-{date_str}-categories.json")
subcategories = spark.read.json(f"{base_dir}/occ-{date_str}-subcategories.json")


#Output data
data = {}
data['agg'] = {}
data['noagg'] = {}

data['date'] = date_str
data['record_version'] = RECORD_VERSION

dfu = df.dropDuplicates(["id"])
dfu_na = dfu.filter(dfu.redirect.type < 2)
agg_record_count = dfu.count()
na_record_count = dfu_na.count()

data['total_record_count'] = df.count()
data['deduplicated_record_count'] = dfu.count()
data['deduplicated_record_count_noagg'] = dfu_na.count()

#Redirect type
data['count_by_redirect_type'] = dfu.groupby("redirect.type").count().toPandas().to_dict('records')

#by jobType
data['agg']["count_by_jobType"] = dfu.groupby("jobType").count().toPandas().to_dict('records')
data['agg']["count_by_jobType_redirect_type"] = dfu.groupby("jobType").pivot("redirect.type").count().toPandas().to_dict('records')

data['noagg']["count_by_jobType"] = dfu_na.groupby("jobType").count().toPandas().to_dict('records')
data['noagg']["count_by_jobType_redirect_type"] = dfu_na.groupby("jobType").pivot("redirect.type").count().toPandas().to_dict('records')


#Categories:
cat_count = dfu.groupby("category").count().orderBy(F.col("count").desc())
split_col = F.split(cat_count["category.__ref"].cast("String"), ":")
cat_count = cat_count.withColumn("category_id", split_col.getItem(1).cast("INT"))
cat_count = cat_count.join(categories.select("id", "description"), cat_count.category_id == categories.id, how="inner")
cat_count = cat_count.select(["category_id", "description", "count"]).orderBy(F.col("count").desc())
cat_count = cat_count.withColumn("pct", F.col("count") / agg_record_count)
data['agg']['count_by_category'] = cat_count.toPandas().to_dict("records")

cat_count = dfu_na.groupby("category").count().orderBy(F.col("count").desc())
split_col = F.split(cat_count["category.__ref"].cast("String"), ":")
cat_count = cat_count.withColumn("category_id", split_col.getItem(1).cast("INT"))
cat_count = cat_count.join(categories.select("id", "description"), cat_count.category_id == categories.id, how="inner")
cat_count = cat_count.select(["category_id", "description", "count"]).orderBy(F.col("count").desc())
cat_count = cat_count.withColumn("pct", F.col("count") / na_record_count)
data['noagg']['count_by_category'] = cat_count.toPandas().to_dict("records")

## ### Proporciones por tipo de Redir
#
## In[10]:
#
#
#split_col = F.split(dfu["category.__ref"].cast("String"), ":")
#cat_count_redir = dfu.withColumn("category_id", split_col.getItem(1).cast("INT"))
#cat_count_redir = cat_count_redir.groupby("category_id").pivot("redirect.type").count()
#cat_count_redir = cat_count_redir.join(categories.select("id", "description"), cat_count_redir.category_id == categories.id, how="inner")
#cat_count_redir = cat_count_redir.select(["description", "0", "1", "2"]).sort(F.col("0").desc()).toPandas().fillna(0)
#cat_count_redir.columns = ["Category", "NoRedir", "Redir1", "Redir2"]
#cat_count_redir = cat_count_redir.set_index("Category")
#cat_count_redir = cat_count_redir.div(cat_count_redir.sum(axis=1), axis=0)
#data['proportions_by_category_by_redirect_type'] = cat_count_redir.to_dict('index')
#data['proportions_by_category_by_redirect_type']
#
#
## #### Categorías en las que OCC es débil
## 
## Mayor proporción de agregadas
#
## ## Subcategorías
#
## In[11]:
#
#
#subcat_count = dfu.groupby("subcategory").count().orderBy(F.col("count").desc())
#split_col = F.split(subcat_count["subcategory.__ref"].cast("String"), ":")
#subcat_count = subcat_count.withColumn("subcategory_id", split_col.getItem(1).cast("INT"))
#subcat_count = subcat_count.join(subcategories.select("id", "description"), subcat_count.subcategory_id == subcategories.id, how="inner")
#data['count_by_subcategory'] = subcat_count.orderBy(F.col("count").desc()).toPandas().to_dict('records')#.show(25, truncate=False)


# ## Ubicación
# ### Estados
# #### Distribución de vacantes por estado con suma acumulada
dfu_loc = dfu.select("id", F.explode("location.locations").alias("loc_data"))
state_count = dfu_loc.groupby("loc_data.state.description").count().sort(F.col("count").desc())
data['agg']['count_by_state'] = state_count.toPandas().to_dict('records')
#state_count_p = state_count.withColumn("perc", F.col("count") / agg_record_count).orderBy(F.col("perc").desc())
#window = Window.orderBy(F.col("perc").desc()).rowsBetween(Window.unboundedPreceding, Window.currentRow)
#state_count_p = state_count_p.withColumn("cumsum", F.sum(F.col("perc")).over(window))


dfu_loc = dfu_na.select("id", F.explode("location.locations").alias("loc_data"))
state_count = dfu_loc.groupby("loc_data.state.description").count().sort(F.col("count").desc())
data['noagg']['count_by_state'] = state_count.toPandas().to_dict('records')
#state_count_p = state_count.withColumn("perc", F.col("count") / na_record_count).orderBy(F.col("perc").desc())
#window = Window.orderBy(F.col("perc").desc()).rowsBetween(Window.unboundedPreceding, Window.currentRow)
#state_count_p = state_count_p.withColumn("cumsum", F.sum(F.col("perc")).over(window))


# #### Vacantes por estado divididas por tipo Redir, con histograma para Redir2
#state_count = dfu.withColumn("loc_data", F.explode("location.locations"))\
#                .groupby("loc_data.state.description")\
#                .pivot("redirect.type").count()\
#                .withColumn("total", F.col("0") + F.col("1") + F.col("2"))\
#                .withColumn("NoRedirPct", F.col("0") / F.col("total"))\
#                .withColumn("Redir1Pct", F.col("1") / F.col("total"))\
#                .withColumn("Redir2Pct", F.col("2") / F.col("total"))\
#                .sort(F.col("total").desc())


# #### Descripción (granular)
loc_count = dfu.groupby("location.description").count().sort(F.col("count").desc())
loc_count_p = loc_count.withColumn("perc", F.col("count") / agg_record_count).orderBy(F.col("perc").desc())
window = Window.orderBy(F.col("perc").desc()).rowsBetween(Window.unboundedPreceding, Window.currentRow)
loc_count_p = loc_count_p.withColumn("cumsum", F.sum(F.col("perc")).over(window))
data['agg']['count_by_granular_location'] = loc_count_p.toPandas().to_dict('records')

loc_count = dfu_na.groupby("location.description").count().sort(F.col("count").desc())
loc_count_p = loc_count.withColumn("perc", F.col("count") / na_record_count).orderBy(F.col("perc").desc())
window = Window.orderBy(F.col("perc").desc()).rowsBetween(Window.unboundedPreceding, Window.currentRow)
loc_count_p = loc_count_p.withColumn("cumsum", F.sum(F.col("perc")).over(window))
data['noagg']['count_by_granular_location'] = loc_count_p.toPandas().to_dict('records')


# ## Compañias
# Basadas en la url. 
# NULL ==> Confidenciales
company_count = dfu.groupby("company.url").count().sort(F.col("count").desc())
print(f"Número de URLs de compañias: {company_count.count()}")
data['agg']['count_by_company_url'] = company_count.toPandas().to_dict('records')

company_count = dfu_na.groupby("company.url").count().sort(F.col("count").desc())
print(f"Número de URLs de compañias: {company_count.count()}")
data['noagg']['count_by_company_url'] = company_count.toPandas().to_dict('records')


# #### Proporción de vacantes por cia y suma acumulada
#ccp = company_count.withColumn("perc", F.col("count") / record_count).orderBy(F.col("perc").desc())
#window = Window.orderBy(F.col("perc").desc()).rowsBetween(Window.unboundedPreceding, Window.currentRow)
#ccp.withColumn("cumsum", F.sum(F.col("perc")).over(window)).show(30, truncate=False)
# ### Excluyendo confidenciales
#ccp_noconf = company_count.where("url is not null")
#nconf_records = ccp_noconf.agg(F.sum("count")).collect()[0][0]
#ccp_noconf = ccp_noconf.withColumn("perc", F.col("count") / nconf_records).orderBy(F.col("perc").desc())
#window = Window.orderBy(F.col("perc").desc()).rowsBetween(Window.unboundedPreceding, Window.currentRow)
#ccp_noconf.withColumn("cumsum", F.sum(F.col("perc")).over(window)).show(30, truncate=False)


# ### Confidenciales
# La URL null son confidenciales
data['agg']['confidenciales'] = dfu.select(["company.url", "company.name"]).where("company.confidential=TRUE").count()
company_count = dfu.where("redirect.type != 2").groupby("company.url").count().sort(F.col("count").desc())
data['agg']['count_by_company_where_not_redirected'] = company_count.toPandas().to_dict('records')
company_count = dfu.where("redirect.type == 2").groupby("company.url").count().sort(F.col("count").desc())
data['count_by_company_where_redirected'] = company_count.toPandas().to_dict('records')

data['noagg']['confidenciales'] = dfu_na.select(["company.url", "company.name"]).where("company.confidential=TRUE").count()


# ## Salarios
# Casi todas las Redir2 (agregadas) no tienen salario, vs 1/3 de las pagadas:
salary = dfu.select(["salary.from", "salary.to"]).where("salary.from > 0 or salary.to > 0")
salary = salary.withColumn("avg", (F.col("from") + F.col("to")) / 2)
data['agg']['salary_summary'] = salary.summary().toPandas().to_dict('records')#.show()

salary = dfu_na.select(["salary.from", "salary.to"]).where("salary.from > 0 or salary.to > 0")
salary = salary.withColumn("avg", (F.col("from") + F.col("to")) / 2)
data['noagg']['salary_summary'] = salary.summary().toPandas().to_dict('records')#.show()

# ## Ids unicos para poder comparar entre estados

data['agg']['jobids'] = sorted(list(dfu.select("id").toPandas()['id']))
data['noagg']['jobids'] = sorted(list(dfu_na.select("id").toPandas()['id']))

#Save output
print(f"Saving {output_filename}")
save(output_filename, data)

#data
