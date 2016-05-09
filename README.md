# NiFi Processors

## Description

The suite of custom processors for [Apache NiFi](https://nifi.apache.org/).

## Processors

### ConvertLTSVToJSON

This processor converts incoming LTSV string to JSON.

### ConvertToJSONWithRegex

Converts coming string to JSON with the named regex you specified.

```
Input: "id:0019 time:[2016-05-07 10:13:21]      level:WARN      method:PUT      uri:/api/v1/textdata    reqtime:2.9952765391217318      foobar:F2SyydsV"
Regex: "^id:(?<id>[0-9]+) time:\[(?<time>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).+level:(?<level>\w+).+method:(?<method>\w).+uri:(?<uri>[\/\w\d]+).+reqtime:(?<reqtime>[\d\.]+).+foobar:(?<foobar>[\w\d]+)$"
```
It results
```
{"foobar":"F2SyydsV","method":"P","level":"WARN","reqtime":"2.9952765391217318","id":"0019","time":"2016-05-07 10:13:21","uri":"/api/v1/textdata"}
```