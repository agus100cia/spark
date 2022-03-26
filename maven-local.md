```xml

mvn install:install-file \
   -Dfile=<path-to-file> \
   -DgroupId=<group-id> \
   -DartifactId=<artifact-id> \
   -Dversion=<version> \
   -Dpackaging=<packaging> \
   -DgeneratePom=true
     
 ``` 
 
 ```xml

mvn install:install-file \
   -Dfile=/Users/agus/Documents/GitHub/consejojudicatura/cj-index-doc/lib/phoenix-5.0.0.3.1.5.0-152-client-cj315.jar \
   -DgroupId=org.apache.phoenix \
   -DartifactId=phoenix-client \
   -Dversion=5.0.0.3.1.5.0-152-cj315 \
   -Dpackaging=jar \
   -DgeneratePom=true

``` 
