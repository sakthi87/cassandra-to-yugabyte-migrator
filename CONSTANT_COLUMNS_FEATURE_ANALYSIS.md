# Constant Columns Feature Analysis

## Summary

**Yes, this feature can be added to the cassandra-to-yugabyte-migrator project!** 

The Constant Columns feature from `cassandra-data-migrator-main` allows setting default values for target columns that don't exist in the source table. This is useful for audit fields like `created_by`, `migration_date`, `source_system`, etc.

## How It Works in CDM Project

### Configuration

```properties
# Constant Columns Feature - Audit Fields
spark.cdm.feature.constantColumns.names=created_by,migration_date,source_system
spark.cdm.feature.constantColumns.values='CDM_MIGRATION','2024-12-16','CASSANDRA_PROD'
spark.cdm.feature.constantColumns.splitRegex=,
```

### Implementation Flow

1. **Property Loading**: `ConstantColumns.java` reads names and values from properties
2. **Validation**: Validates columns exist in target table and values match data types
3. **Record Creation**: When creating records, constant values are added to target columns
4. **Primary Key Support**: Can also be used for primary key columns if needed

### Key Classes

- **`ConstantColumns.java`**: Feature implementation
- **`PKFactory.java`**: Uses constant columns for primary key default values
- **`YugabyteUpsertStatement.java`**: Adds constant column values to INSERT statements

## How to Add to cassandra-to-yugabyte-migrator

### Architecture Differences

**CDM Project:**
- Uses Java with feature framework
- Uses INSERT statements with PreparedStatements
- Validates types using CQL codecs

**Your Project:**
- Uses Scala with Spark DataFrames
- Uses COPY FROM STDIN (CSV format)
- Transforms rows to CSV in `RowTransformer`

### Implementation Approach

Since your project uses COPY FROM STDIN (CSV), the implementation is simpler:

1. **Add properties to `TableConfig`**:
   ```scala
   case class TableConfig(
     // ... existing fields ...
     constantColumns: Map[String, String] // column name -> value
   )
   ```

2. **Read properties in `TableConfig.fromProperties()`**:
   ```scala
   // Read constant columns
   val constantColumnNames = getProperty("table.constantColumns.names", "").split(",").map(_.trim).filter(_.nonEmpty)
   val constantColumnValues = getProperty("table.constantColumns.values", "").split(",").map(_.trim).filter(_.nonEmpty)
   val constantColumns = constantColumnNames.zip(constantColumnValues).toMap
   ```

3. **Update `SchemaMapper`** to include constant columns in target columns list

4. **Update `RowTransformer`** to append constant values:
   ```scala
   def toCsv(row: Row): Option[String] = {
     // ... existing source column mapping ...
     
     // Append constant column values
     val constantValues = tableConfig.constantColumns.values.map { value =>
       escapeCsvField(value, isNull = false)
     }
     
     Some((values ++ constantValues).mkString(","))
   }
   ```

5. **Update `CopyStatementBuilder`** to include constant columns in COPY statement:
   ```scala
   val allColumns = targetColumns ++ tableConfig.constantColumns.keys
   s"COPY ${schema}.${table} (${allColumns.mkString(", ")}) FROM STDIN WITH (FORMAT csv, ...)"
   ```

### Key Differences from CDM

1. **Simpler Implementation**: No feature framework needed, just properties
2. **CSV Format**: Values are written as strings in CSV (no type validation needed at runtime)
3. **No Type Validation**: Type validation happens when data is inserted into YugabyteDB
4. **Property Format**: Simpler property format (can use comma-separated or JSON)

## Example Configuration

```properties
# Constant Columns (Default Values for Target Columns)
table.constantColumns.names=created_by,migration_date,source_system
table.constantColumns.values='CDM_MIGRATION','2024-12-16','CASSANDRA_PROD'
```

Or with custom delimiter for complex values:

```properties
table.constantColumns.names=migration_tags,migration_metadata
table.constantColumns.values=['source_cassandra','env_prod']|{'migrator':'CDM','version':'4.0'}
table.constantColumns.splitRegex=\\|
```

## Benefits

1. ✅ **Audit Fields**: Track migration metadata (who, when, source)
2. ✅ **Data Lineage**: Track data origin and migration runs
3. ✅ **Backward Compatible**: No changes needed if not used
4. ✅ **Simple Implementation**: Straightforward CSV append
5. ✅ **Flexible**: Works with any data type (validated by YugabyteDB)

## Considerations

1. **Type Validation**: Values are written as strings in CSV, type validation happens in YugabyteDB
2. **Quoting**: String values must be properly quoted/unquoted for CSV
3. **Order**: Constant columns should be added AFTER source columns in COPY statement
4. **Column Existence**: Target table must have these columns (can validate at startup)

## Next Steps

1. Add properties to `TableConfig`
2. Update `SchemaMapper` to include constant columns
3. Update `RowTransformer` to append constant values
4. Update `CopyStatementBuilder` to include constant columns
5. Add validation (optional - verify columns exist in target table)
6. Add documentation and examples

Would you like me to implement this feature?

