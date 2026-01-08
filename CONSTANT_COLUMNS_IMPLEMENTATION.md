# Constant Columns Feature Implementation

## Summary

✅ **Implemented**: Constant Columns feature allows setting default values for target columns that don't exist in the source table.

## What Was Changed

### 1. `TableConfig.scala`
- Added `constantColumns: Map[String, String]` field
- Added property parsing for `table.constantColumns.names` and `table.constantColumns.values`
- Added support for custom delimiter via `table.constantColumns.splitRegex`

### 2. `SchemaMapper.scala`
- Updated `getTargetColumns()` to append constant column names to the target columns list
- Constant columns are added after source columns

### 3. `RowTransformer.scala`
- Updated `toCsv()` to handle constant columns separately from source columns
- Constant columns get their values from `tableConfig.constantColumns` map
- Values are properly escaped for CSV format

### 4. `CopyStatementBuilder.scala`
- No changes needed - automatically includes constant columns since they're in the target columns list

## Configuration

### Basic Syntax

```properties
# Constant Columns (Default Values for Target Columns)
table.constantColumns.names=created_by,migration_date,source_system
table.constantColumns.values=CDM_MIGRATION,2024-12-16,CASSANDRA_PROD
```

### Parameters

| Parameter | Description | Required |
|-----------|-------------|----------|
| `table.constantColumns.names` | Comma-separated list of target column names | Yes (if using constant columns) |
| `table.constantColumns.values` | Comma-separated list of values (same order as names) | Yes (if using constant columns) |
| `table.constantColumns.splitRegex` | Delimiter for splitting values (default: `,`) | No |

### Value Format

Values are written as strings in CSV. YugabyteDB will validate types on insert:

| Data Type | Format | Example |
|-----------|--------|---------|
| TEXT/VARCHAR | Plain string | `CDM_MIGRATION` |
| INT/BIGINT | Number (no quotes) | `12345` |
| BOOLEAN | true/false (no quotes) | `true` |
| DATE | Date string | `2024-12-16` |
| TIMESTAMP | ISO format string | `2024-12-16T10:30:00Z` |

**Note**: Don't use outer quotes in the properties file. Values will be properly escaped for CSV automatically.

## Examples

### Example 1: Basic Audit Fields

**Target Table (YugabyteDB):**
```sql
CREATE TABLE my_table (
    id UUID PRIMARY KEY,
    name TEXT,
    email TEXT,
    -- Audit fields (not in source)
    created_by TEXT,
    migration_date DATE,
    source_system TEXT
);
```

**Configuration:**
```properties
table.constantColumns.names=created_by,migration_date,source_system
table.constantColumns.values=CDM_MIGRATION,2024-12-16,CASSANDRA_PROD
```

**Result:** Every migrated record will have:
- `created_by = 'CDM_MIGRATION'`
- `migration_date = '2024-12-16'`
- `source_system = 'CASSANDRA_PROD'`

### Example 2: Numeric and Boolean Values

```properties
table.constantColumns.names=migration_run_id,data_version,is_migrated
table.constantColumns.values=1702732800000,1,true
```

### Example 3: Custom Delimiter

If values contain commas, use a custom delimiter:

```properties
table.constantColumns.names=migration_tags,migration_metadata
table.constantColumns.values=['source_cassandra','env_prod']|{'migrator':'CDM','version':'4.0'}
table.constantColumns.splitRegex=\\|
```

## How It Works

1. **Property Parsing**: `TableConfig.fromProperties()` reads constant column names and values
2. **Column List**: `SchemaMapper.getTargetColumns()` appends constant column names to target columns
3. **CSV Generation**: `RowTransformer.toCsv()`:
   - Maps source columns to their values from the Spark Row
   - Appends constant column values from `tableConfig.constantColumns`
   - Escapes all values for CSV format
4. **COPY Statement**: `CopyStatementBuilder` includes all columns (source + constant) in the COPY statement
5. **Data Insert**: YugabyteDB receives CSV with all columns and validates types

## Benefits

✅ **Audit Fields**: Track migration metadata (who, when, source)  
✅ **Data Lineage**: Track data origin and migration runs  
✅ **Backward Compatible**: No changes needed if not used  
✅ **Simple Configuration**: Just add properties  
✅ **Type Safe**: YugabyteDB validates types on insert  

## Limitations

1. **Type Validation**: Values are written as strings in CSV; type validation happens in YugabyteDB
2. **Constant Values**: Same value for all rows (not per-row values)
3. **Column Existence**: Target table must have these columns (no runtime validation yet)

## Testing

To test the feature:

1. Create a target table with extra columns (audit fields)
2. Configure constant columns in properties file
3. Run migration
4. Verify that all records have the constant values populated

## Files Changed

1. `src/main/scala/com/company/migration/config/TableConfig.scala`
2. `src/main/scala/com/company/migration/transform/SchemaMapper.scala`
3. `src/main/scala/com/company/migration/transform/RowTransformer.scala`
4. `src/main/resources/migration.properties.example.constantColumns` (new example file)

