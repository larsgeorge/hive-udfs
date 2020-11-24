# Okera Hive UDFs

This project creates a single JAR with extra/additional UDF functions that can be used with Okera's ODAP.

## Building

Check out the code and run 

```
$ mvn package
```

to build an uberjar with everything you need. 

## Contained UDFs

The following UDFs are part of this project.

### `jq` Function

This function provides access to JSON Query (JQ), which is a common CLI tool to extract information from a given JSON structure using a specific query language.
See https://stedolan.github.io/jq/tutorial/ for examples.
