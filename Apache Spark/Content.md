# Spark context

## Resilient Distributed Datasets (RDDs)

The Resilient Distributed Dataset is a concept at the heart of Spark. It is designed to support in-memory data storage, distributed across a cluster in a manner that is demonstrably both fault-tolerant and efficient. Fault tolerance is achieved, in part, by tracking the lineage of transformations applied to coarse-grained sets of data. Efficiency is achieved through parallelization of processing across multiple nodes in the cluster, and minimization of data replication between those nodes. Once data is loaded into an RDD, two basic types of operation can be carried out: 
	•	Transformations, which create a new RDD by changing the original through processes such as mapping, filtering, and more;  
	•	Actions, such as counts, which measure but do not change the original data. [1]
  
  
  
  
  
  
 ### References: 
  1. Getting Started with Apache Spark, Inception to Production, James A. Scott
