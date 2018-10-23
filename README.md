# Embulk Output Domo output plugin for Embulk
This embulk output plugin sends data to a Domo Dataset using domo Stream API.
Using the [domo jdk](https://github.com/domoinc/domo-java-sdk) we can move data from embulk input to domo Stream API.

## Overview

* **Plugin type**: output
* **Load all or nothing**: yes
* **Resume supported**: no
* **Cleanup supported**: yes


## Install
```
embulk gem install embulk-output-embulk_output_domo 
```

## Configuration

- **clientId**: Domo Client Id (string, required)
- **clientSecret**: Domo Client Secret (string, required)
- **apiHost**: api host address (string, default: `"api.domo.com"`)
- **useHttps**: use https? (boolean, default: `true`)
- **streamName**: name of the Domo Stream (string, required)
- **column_options**: Embulk Column Options (used mainly for timestamps formatting) (object, default: `Check embulk column options`)
- **batchSize**: Number of csv files to be zipped and send for each upload request. Each csv has like 50 records. (integer, default: `1000`)
- **quote**: CSV Quote symbol (See csv embulk plugin for more info) (string, default: `"\""`)
- **quote_policy**: CSV Quote Policy (string, default: `MINIMAL`)
- **escape**: CSV Escape Character (string, default: `null`)
- **newline_in_field**: CSV New Line (string, default: `LF`)


## Example

```yaml
out:
  type: embulk_output_domo
  clientId: 209410f4
  clientSecret: 00
  apiHost: api.domo.com
  useHttps: true
  streamName: Daily Metrics Test
  batchSize: 500
```


## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
