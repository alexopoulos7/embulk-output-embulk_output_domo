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

- **clientId**: description (string, required)
- **clientSecret**: description (string, required)
- **apiHost**: description (string, default: `"api.domo.com"`)
- **useHttps**: description (boolean, default: `true`)
- **streamName**: description (string, required)
- **column_options**: description (object, default: `Check embulk column options`)
- **batchSize**: description (integer, default: `1000000`)
- **quote**: description (string, default: `"\""`)
- **quote_policy**: description (string, default: `MINIMAL`)
- **escape**: description (string, default: `null`)
- **newline_in_field**: description (string, default: `LF`)


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
