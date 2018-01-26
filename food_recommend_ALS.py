from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel
from pyspark import SparkContext
sc = SparkContext()

mappingRDD = sc.textFile("hdfs://localhost/user/cloudera/eat_ride_easy/raw_data/mapping.txt")
mapping_dict = mappingRDD.map(lambda x:tuple(x.split(","))).collectAsMap()
bc_mapping_dict = sc.broadcast(mapping_dict)

def mapping(x):
    for key in bc_mapping_dict.value:
        if key in x[1]:
            return (x[0],bc_mapping_dict.value[key],1)
        if key in x[2]:
            return (x[0],bc_mapping_dict.value[key],1)
    return (x[0],None,1)


articleRDD = sc.textFile("hdfs://localhost/user/cloudera/eat_ride_easy/raw_data/raw_article.txt")
articleRDD = articleRDD.map(lambda x: x.split(","))
mapping_articleRDD = articleRDD.map(mapping).filter(lambda x:x[1] != None)

model = ALS.train(mapping_articleRDD, rank=5, iterations=10,lambda_=0.01)

model.recommendUsers(50,20)
model.recommendProducts(30,10)

