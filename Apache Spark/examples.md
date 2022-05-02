# Word Counting - Scala 

This example counts unique words in any documentation. It saves the output in  a specific directory. 
It has 3  steps: 

1. Note: `"/path/to/text/file/"` have to a directory with a text file. 
`/** map */`

`var map = sc.textFile("/path/to/text/file").flatMap(line => line.split(" ")).map(word => (word,1));`
 
2. 
`/** reduce */`

`var counts = map.reduceByKey(_ + _);`
 
3. Note: `"/path/to/output/"` have to another directory to save output files. 
`/** save the output to file */`

`counts.saveAsTextFile("/path/to/output/")`
