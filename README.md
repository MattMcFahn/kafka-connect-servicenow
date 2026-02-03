# Kafka Connect ServiceNow Source Connector

This module is a [Kafka Connect](https://docs.confluent.io/current/connect/index.html) Source Connector for the [ServiceNow Table API](https://developer.servicenow.com/app.do#!/rest_api_doc?v=madrid&id=c_TableAPI).
It provides facilities for polling arbitrary ServiceNow tables via its Table API and publishing
detected changes to a [Kafka topic](https://kafka.apache.org/documentation/#intro_topics).

It forks the original [connector developed by IBM](https://github.com/IBM/kafka-connect-servicenow), and has been updated 
to pull **display values** from ServiceNow, as well as the internal values, in a format that maintains backwards 
compatibility for schemas.

This module is agnostic to the ServiceNow model being used as all the table names, and fields used
are provided via configuration.

Going forward in this readme `source_table` will be used to refer to a table accessible via the
ServiceNow TableAPI and `table_config_key` will be used to refer to an identifier representing a
particular `source_table` within the Kafka Connect ServiceNow Source Connector's configuration.

## Table of Contents
- [Specifying Source Tables](#specifying-source-tables)
- [Configuration](#configuration)
- [Display Value Feature](#display-value-feature)
- [Building](#how-to-build)

---

## Specifying Source Tables

The source table configuration is provided by first specifying a comma-delimited list of `table_config_key(s)`. 
Each `table_config_key` is used to specify `source table` specific configuration properties.

**Example:**
```json
{
  "config": {
    "table.whitelist": "case,changerequest"
  }
}
```

In the above example, the Source Connector is going to look for `source table` specific configurations
using the `table_config_keys` of `case` and `changerequest`.

### Source Table Constraints

The current implementation requires the following constraints to be satisfied in order to poll it.

- There must exist a `sortable last updated at timestamp` column that the polling algorithm can use to manage
its window. This column is specified with the `table.whitelist.<table config id>.timestamp.field.name` configuration
key.
- There must exist a `sortable unique identifier` column that the polling algorithm can use to distinguish
between records in the source table. This column is specified with the `table.whitelist.<table config id>.identifier.field.name`
configuration key.

Entry point for this connector is `com.ibm.ingestion.connect.servicenow.ServiceNowSourceConnector`.

The `ServiceNowSourceConnector` class evaluates the connector configuration and determines how
many `ServiceNowTableAPISourceTask` instances it needs to configure. It returns the initialized
`ServiceNowTableAPISourceTaskConfig` objects to `kafka connect`.

### Destination Kafka Topic Partitioning

This Source Connector supports several destination partitioning types.

`table.whitelist.<table config id>.partition.type`

**Values:**

- `default` - This type forces all messages to be on partition `0`. Useful for testing.
- `round-robin` - This type assigns messages to partitions in a round-robin fashion.
- `field-based` - This type requires a set of `fields` (`table.whitelist.<table config id>.partition.fields`) to use as the `partitioning key`
which determines the destination partition. This is useful if you need to guarantee
messages about a particular entity go to the same partition.

---

## Configuration

### Connector Configuration

Configuration | Default | Notes
:------- | :------- | :-------
table.whitelist | none | A list of source table keys to use in subsequent, table specific, configurations. These keys are also used when calculating the `target kafka topic` for a particular `source table`.
topic.prefix | none | The prefix to use when publishing messages for source tables. For example, if the prefix is `ibm.test.servicenow`, and the `source table key` is `changerequest`, then the calculated topic will be `ibm.test.servicenow.changerequest`. This is a required setting and has no default value provided.

### ServiceNow Table API Authentication Configuration

Configuration | Default | Notes
:------- | :------- | :-------
servicenow.client.oauth.path | `/oauth_token.do` | The path to use when logging into the ServiceNow instance using OAuth.
servicenow.client.oauth.clientid | none | The OAuth Client ID to use when authenticating. This is a required field and no default is provided.
servicenow.client.oauth.clientsecret | none | The OAuth Client Secret to use when authenticating.
servicenow.client.oauth.username | none | The OAuth User Name to use when authenticating.
servicenow.client.oauth.userpassword | none | The OAuth User Password to use when authenticating.

### ServiceNow HTTP Client Configuration

ServiceNow Client Implementation: `com.ibm.ingestion.http.ServiceNowTableApiClient`

Configuration  | Default | Notes
:------------- |:--------| :------------
servicenow.client.connection.timeout.seconds | `30`    | The amount of time in seconds to wait for establishing a connection.
servicenow.client.request.timeout.seconds | `30`    | The overall timeout for any call. This configuration is independent of the read, and write timeouts.
servicenow.client.read.timeout.seconds | `30`     | The amount of time in seconds to wait for a read operation to complete.
servicenow.client.request.retries.max | `none`    | The number of times an HTTP call will be retried for an IO exception. If this setting is excluded, then the task will continuously retry HTTP calls.
servicenow.client.request.retries.backoff.seconds | `30`      | The amount of time delayed between retries.
servicenow.client.connection.pool.max.idle.connections | `2`       | The maximum number of idle connections to hold in the connection pool.
servicenow.client.connection.pool.keep.alive.duration.seconds | `60`      | The amount of time to hold onto idle connections in the connection pool.
servicenow.client.display.value | `false` | (Important) Controls the format of field values returned from ServiceNow. Options: `false` (default, returns sys_id values only), `true` (returns display values only), `all` (returns both sys_id and display values in flattened format). See [Display Value Feature](#display-value-feature) for detailed information.

### Connector Subtask Configuration

Subtask Implementation: `com.ibm.ingestion.connect.servicenow.source.TableAPISubTask`

Configuration  | Default | Notes
:------------- | :------------ | :------------
task.poll.batch.max.size | 20 | The maximum number of records to retrieve from ServiceNow per call. This setting applies to each SubTask individually. For instance, if you have `two tables` being watched, and a `max batch of 20`, then for each polling cycle the connector task will have at most `20 * 2 = 40` records in memory before publishing to the target stream.
task.poll.fast.interval.ms | 500 | The amount of time between HTTP requests to ServiceNow when the most recent call returned data. This setting combined with the max batch size setting directly translates to the maximum possible throughput for this connector.
task.poll.slow.interval.ms | 30000 | The amount of time between HTTP requests to ServiceNow when the most recent call returned no data. This setting directly translates to how long it takes for a change in ServiceNow to be surfaced in the target stream.
timestamp.initial.query.hours.ago | none | When a subtask fires up and does not have an existing offset to start from, this setting determines the date from which it will start pulling records. When this setting is excluded, the task starts with the earliest timestamp available in the `source table`.
through.timestamp.delay.interval.seconds | 0 | The amount of time between when a record in the source table is updated, and when it will be picked up by the connector query. For example, if this setting is 10 seconds, then an updated record will "cool down" for at least 10 seconds before the connector will pick it up. This is useful if one is consuming from multiple tables that have relationships and wants to provide time for any source transactions to complete.
table.whitelist.`<table config id>`.name | none | The name of the source table in ServiceNow TableAPI.
table.whitelist.`<table config id>`.timestamp.field.name | none | The name of the column in the source table pertaining to the last updated time for each record.
table.whitelist.`<table config id>`.identifier.field.name | none | The name of the column in the source table uniquely identifying the record.
table.whitelist.`<table config id>`.fields | none | A comma-delimited list of fields or columns to pull from the source table. By default, all available fields or columns are retrieved.
table.whitelist.`<table config id>`.partition.type | none | The partitioning type to use when selecting destination Kafka topic partitions for records. See the readme section about partitioning types. When this setting is excluded, the `default` partition type is used.
table.whitelist.`<table config id>`.partition.fields | none | Only valid for partitioning-type of `field-based`. This setting determines the fields on the `source table` to use as the partitioning key for selecting destination Kafka topic partitions for records.

---

## Display Value Feature

The `servicenow.client.display.value` parameter controls how ServiceNow returns field values, particularly for reference fields (fields that reference other tables).

### Understanding Display Values

In ServiceNow:
- **Actual values** (sys_ids) are 32-character unique identifiers like `d71f7935c0a8016700802b64c67c11c6`
- **Display values** are human-readable text like "John Doe" or "High Priority"

### Configuration Options

| Value | Behavior | Kafka Message Format | Use Case |
|-------|----------|---------------------|----------|
| `false` (default) | Returns only sys_id values | `{"assigned_to": "d71f7935c0a8..."}` | When you only need internal IDs for lookups |
| `true` | Returns only display values | `{"assigned_to": "John Doe"}` | When you only need human-readable values |
| `all` | **Returns both values (flattened)** | `{"assigned_to": "d71f7935c0a8...", "assigned_to_display_value": "John Doe"}` | **Recommended** - Get both ID and display name |

### How `display_value=all` Works (Flattened Format)

When `display_value=all` is configured, reference fields are **flattened** into two separate fields:

**ServiceNow API Response:**
```json
{
  "assigned_to": {
    "value": "user-12345",
    "display_value": "John Doe"
  }
}
```

**Kafka Message (Flattened):**
```json
{
  "assigned_to": "user-12345",
  "assigned_to_display_value": "John Doe"
}
```

### Configuration Examples

#### Get Both Values (Recommended)
```json
{
  "name": "servicenow-connector",
  "config": {
    "servicenow.client.base.uri": "https://your-instance.service-now.com",
    "servicenow.client.display.value": "all",
    "table.whitelist": "incident",
    "topic.prefix": "servicenow"
  }
}
```

#### Default (Only sys_ids)

```json
{
  "name": "servicenow-connector",
  "config": {
    "servicenow.client.base.uri": "https://your-instance.service-now.com",
    "table.whitelist": "incident",
    "topic.prefix": "servicenow"
  }
}
```

## Complete Configuration Example

```json
{
    "name": "servicenow-incident-connector",
    "config": {
        "servicenow.client.base.uri": "https://yourinstance.service-now.com",
        "servicenow.client.oauth.clientid": "your-client-id",
        "servicenow.client.oauth.clientsecret": "your-client-secret",
        "servicenow.client.oauth.username": "your-username",
        "servicenow.client.oauth.userpassword": "your-password",
        "servicenow.client.display.value": "all",
        "table.whitelist": "incident,change_request",
        "table.whitelist.incident.name": "incident",
        "table.whitelist.incident.timestamp.field.name": "sys_updated_on",
        "table.whitelist.incident.identifier.field.name": "number",
        "table.whitelist.incident.partition.type": "field-based",
        "table.whitelist.incident.partition.fields": "number",
        "table.whitelist.change_request.name": "change_request",
        "table.whitelist.change_request.timestamp.field.name": "sys_updated_on",
        "table.whitelist.change_request.identifier.field.name": "number",
        "table.whitelist.change_request.partition.type": "default",
        "topic.prefix": "servicenow",
        "timestamp.initial.query.hours.ago": 720,
        "tasks.max": 1,
        "connector.class": "com.ibm.ingestion.connect.servicenow.ServiceNowSourceConnector"
    }
}
```

---

## How to Build

This connector has a Gradle configuration file. You can create a bundled JAR with the following Gradle command:

```bash
./gradlew shadowJar
```

The shadow JAR will be created in:
```
build/libs/servicenow-connector-1.0-SNAPSHOT-all.jar
```

### Running Tests

```bash
# Run all tests
./gradlew test
```

---

## Troubleshooting

### Issue: Not seeing display values in Kafka

**Check:**
1. Verify `servicenow.client.display.value` is set to `"all"` in your connector configuration
2. Restart the connector after configuration changes
3. Check connector logs for the message: `ServiceNow client initialized with display_value mode: all`

### Issue: Field names have underscores instead of periods

**This is expected behavior.** ServiceNow field names like `parent.incident` are converted to `parent__incident` to comply with Kafka Connect schema naming requirements.

### Issue: Timestamp or identifier fields not working

**Ensure** these fields are specified correctly in your configuration and exist in the ServiceNow table. The connector extracts the "value" from display_value objects for these critical fields.

---
