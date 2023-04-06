from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext
import pyspark.sql.functions as func
import sys
import csv

def parseCSV(idx, part):
    if idx==0:
        next(part)
    for p in csv.reader(part):
        if ',' in p[1]:
            p[1] = '\"' + p[1]+ '\"'
        yield (p[1].lower(), p[7].lower(), int(p[0][:4]))

def writeToCSV(row):
    return ', '.join(str(item) for item in row)

def main(sc):
    spark = SparkSession(sc)
    sqlContext = SQLContext(sc)
    rows = sc.textFile(sys.argv[1]).mapPartitionsWithIndex(parseCSV)
    df = sqlContext.createDataFrame(rows, ('product', 'company', 'date'))

    dfComplaintsYearly = df.groupby(['product', 'date']).count().sort('product')
    dfComplaintsYearly = dfComplaintsYearly.withColumnRenamed("count", "num_complaints")

    dfCompaniesCount = df.groupby(['product', 'date', 'company']).count()
    dfCompaniesYearly = dfCompaniesCount.groupby(['product', 'date']).count().sort('product')
    dfCompaniesYearly = dfCompaniesYearly.withColumnRenamed("count", "num_companies")

    dfMax = dfCompaniesCount.groupBy(['product', 'date']).max('count')
    dfTotal = dfCompaniesCount.groupBy(['product', 'date']).sum('count')
    dfRatio = dfMax.join(dfTotal, ['product', 'date'], how='inner')
    dfRatio = dfRatio.select('product', 'date', func.round(dfRatio[2]/dfRatio[3]*100).cast('integer').alias('percentage'))

    dfFinal = dfComplaintsYearly.join(dfCompaniesYearly.join(dfRatio, ['product', 'date'], how='inner'), ['product', 'date'], how='inner')
    dfFinal = dfFinal.sort('product', 'date')
    #dfFinal.write.format("csv").save(sys.argv[2])
    dfFinal.rdd.map(writeToCSV).saveAsTextFile(sys.argv[2])

if __name__=="__main__":
    sc = SparkContext()
    main(sc)
