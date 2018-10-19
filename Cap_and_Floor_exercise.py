import random
import numpy as np
import pyspark
from pyspark import SparkContext, SparkConf

# Declaracion de la libreria HiveContext para manejo de datos distribuidos

from pyspark.sql import HiveContext
from pyspark.sql import SQLContext 
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, DoubleType
import pandas as pd
import re

## Funcion principal, esta funcion practicamente no fue modificada, solo fue importante considerar
## la libreria de Hive para la creacion de las tablas temporales (linea de codigo 42)

def proc_cap_flo(df,input_floor,input_cap,l_exclusiones,sqlContexto,pipeline,lib_write,rnd_nbr):
    
    if pipeline == 1:
        str_rnd_nbr = str(rnd_nbr)
        df = drop_write_aux(lib_write,str_rnd_nbr,"aux_cap_flo_",sqlContexto)(df)
    df.createOrReplaceTempView("temp_cap_flo_aux")
    cols = df.columns
    col_string = [item[0] for item in df.dtypes if item[1].startswith(tuple(['string','boolean','date','bool']))]
    col_string=col_string+l_exclusiones
    S=list(range(len(col_string)))

    col_num=[x for x in cols if x not in col_string]
    N =  list(range(len(col_num)))
    
    cap_flo = "select " + ','.join(["percentile_approx(" + col_num[j] + ", " \
    + "array(" + str(input_cap) + ","+str(input_floor) + ")) as " + col_num[j] for j in N]) \
    + " from temp_cap_flo_aux"
    df.show()
    print cap_flo
    result_cf = sqlContext.sql(cap_flo)
    result_cf.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
    result_cf.createOrReplaceTempView("cap_floor")

    crea = "create table " + lib_write + ".result_cf_" + str(rnd_nbr) + " as select * from cap_floor"
    print crea
    sqlContext.sql(crea)
    result_cf.unpersist()
    result_cf=sqlContext.sql("select * from " + lib_write + ".result_cf_" + str(rnd_nbr))
    result_cf=result_cf.collect()
    sqlContext.sql("drop table "+lib_write+".result_cf_"+str(rnd_nbr) +" purge")
  
    #Cambia None por 0
    result_cf_2=[result_cf[0][k] is None and [0,0] or result_cf[0][k] for k in N]
  
    #Cambia nullos por 0
    rst_c = [(result_cf_2[j][0] or 0) for j in N]
    rst_f = [(result_cf_2[j][1] or 0) for j in N]
  
    excluir = {}
  
    for j in N:
        if result_cf_2[j][0]==0 and result_cf_2[j][1]==0:
            excluir[j]=j
          
    rst_c_r = [(result_cf_2[j][0] or 0) for j in [x for x in N if x not in excluir]]
    rst_f_r = [(result_cf_2[j][1] or 0) for j in [x for x in N if x not in excluir]]
    NR=list(range(len(rst_f_r)))
            
    var_excluir= [col_num[j] for j in excluir if excluir]
    col_num_real=[x for x in col_num if x not in var_excluir]
  
    #F Cap and Floor
    def cap_flo_udf(x, f, c):
        if x>c: return c
        elif x<f: return f
        else: return x

    sqlContext.udf.register("cap_flo_fx", cap_flo_udf)

    app_cap = "select " +( len(col_string)==0 and '' or ','.join([col_string[i] for i in S]))\
           +(len(var_excluir)==0 and '' or ','.join([var_excluir[e] for e in list(range(len(var_excluir)))]))\
           +","+(len(col_num_real)==0 and '' or ','.join(["cap_flo_fx(" + col_num_real[j] + ", " + str(rst_f_r[j]) + ", " + str(rst_c_r[j]) + ")*1 as " \
                                  + col_num_real[j] for j in NR])) + " from temp_cap_flo_aux"
 
    print app_cap
    apply_cf = sqlContext.sql(app_cap)


    return(apply_cf)

## Programa principal

conf = pyspark.SparkConf().setAll([('spark.app.name', 'clientes'),('spark.master', 'local')])

## Configuracion y contexto de ejecucion

sc = SparkContext(conf = conf)
sqlContext = HiveContext(sc)
hiveContext = HiveContext(sc)

## Lectura de la coleccion a partir de un json

dfc = sqlContext.read.json("clientes.json")
dfc.show()

## Creacion de la vista temporal 

dfc.createOrReplaceTempView("clientes")

## Generacion del dataframe

df_f_cap_floor= sqlContext.sql("select * from clientes")

df_f_cap_floor.show()

rand_nbr = random.randint(1,1000000)

## Invocacion de la funcion proc_cap_flo

acf = proc_cap_flo(dfc,0.04,0.60,[], df_f_cap_floor,0,"default", rand_nbr)

acf.show()