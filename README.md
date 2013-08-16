kaf2es
======
elastic search에 log 데이터를 저장하는 kafka consumer 이다. 

#설치 
mvn assembly:assembly

#실행
java -jar target/Kafka2EsCmd-jar-with-dependencies.jar <OPTIONS>

usage: kafka2es [OPTION]...
 -clustername <boolean>   enable inserting data to elasticsearch
 -elasticsearch <urls>    The connection string for the elasticsearch
                          connection in the form host:port. Multiple URLS
                          can be given to allow fail-over.
 -enable                  enable inserting data to elasticsearch
 -fetchsize <urls>        The amount of data to fetch in a single request.
                          (default: 1048576)
 -group <group name>      REQUIRED: The group id to consume on.
 -topic <topic name>      REQUIRED: The topic id to consume on.
 -zookeeper <urls>        REQUIRED:  The connection string for the
                          zookeeper connection in the form host:port.
                          Multiple URLS can be given to allow fail-over.
                          
                          
