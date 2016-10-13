# Tp hadoop 2


### Installing

Download the pom.xml (it contains the dependencies) )  
Create a jar containing the three classes.  
You can use Maven with Intellij Idea.

```
dependency: hadoop-core-1.2.1
```
```
output jar: tp_hadoopart-1.0-SNAPSHOT.jar
```
```
creating jar on Intellij: maven package (cf maven tool window)
```

## Running 

There is 3 entries in the jar:  
-FirstNameByOrigin  
-NumberFirstNameByOrigin  
-PartitionMF

Execute the jar on hadoop by the command:
```
hadoop jar tp_hadoopart-1.0-SNAPSHOT.jar <jar entry>  < intput ( /user/<username> in hdfs) >   < output (/user/<username> in hdfs) >
```

## Authors

* **Nicolas Tastevin** 
