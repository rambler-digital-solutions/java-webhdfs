# Java WebHDFS Client

[![](https://jitpack.io/v/rambler-digital-solutions/java-webhdfs.svg)](https://jitpack.io/#rambler-digital-solutions/java-webhdfs)

Lightweight HDFS REST API java client implementation without using fat HADOOP libs.
Based on [Hadoop WebHDFS API](https://hadoop.apache.org/docs/r2.7.7/hadoop-project-dist/hadoop-hdfs/WebHDFS.html).

## Integration tests

For run integration tests with own hadoop - fill `test.properties` and define:

`./gradlew test -Dintegration.test.props=test.properties`

## Usage

```java

WebHdfsClient client = WebHdfsClient.initiate(new URI[] { "http://cluster.local:50070" }, "username");
WebHdfsResource root = client.resource("/path/to/list");

// Non-recursive scan folder
for (Iterator<WebHdfsResource> iter = root.lsResources(false); iter.hasNext();) {
    WebHdfsResource resource = iter.next();
    if (resource.isDir()) {
        System.out.println("Dir: " + resource.getPath().toString());
    } else {
        System.out.println("File: " + resource.getPath().toString());
        byte[] fileData = IOUtils.toByteArray(resource.open());
    }
}

```
