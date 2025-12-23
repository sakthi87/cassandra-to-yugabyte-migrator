# Fix: NULL vs Whitespace-Only Strings vs Non-ASCII Characters

## Problem

When a NOT NULL field contains:
1. **Whitespace-only strings** (e.g., `"   "` - spaces only)
2. **Non-ASCII characters**

The COPY approach was treating these as NULL and trying to insert NULL, causing NOT NULL constraint violations. However, JDBC batch inserts handled these correctly.

## Root Cause

### Issue 1: Incorrect NULL Detection
- **Old code**: Used `row.get(fieldIndex)` and checked if result is `null`
- **Problem**: Spark's `row.get()` may return `null` for null fields, but the proper way is `row.isNullAt(fieldIndex)`
- **Impact**: Could misidentify null vs empty string vs whitespace-only string

### Issue 2: CSV Format for Whitespace-Only Strings
- **Old code**: Whitespace-only strings like `"   "` were not always quoted
- **Problem**: PostgreSQL COPY with `NULL ''` (empty string for NULL) treats:
  - Unquoted empty string `""` → NULL
  - Unquoted whitespace `   ` → May be trimmed or treated as empty
  - Quoted whitespace `"   "` → Preserved as-is
- **Impact**: Whitespace-only strings were being lost or treated as NULL

### Issue 3: Empty String vs NULL Confusion
- **Old code**: Both NULL and empty string became `""` in CSV
- **Problem**: PostgreSQL COPY cannot distinguish between:
  - NULL value: `""` (unquoted empty)
  - Empty string value: `""` (should be `""""` - quoted empty)
- **Impact**: Empty strings were being inserted as NULL

### Issue 4: Non-ASCII Characters
- **Old code**: Non-ASCII characters were not always quoted
- **Problem**: Some non-ASCII characters might be misinterpreted or cause encoding issues
- **Impact**: Non-ASCII characters could be corrupted or cause errors

## Solution

### 1. Use Spark's `isNullAt()` for NULL Detection
```scala
// OLD (incorrect):
val value = row.get(fieldIndex)
if (value == null) { ... }

// NEW (correct):
val isNull = row.isNullAt(fieldIndex)
val value = if (isNull) null else row.get(fieldIndex)
```

### 2. Proper CSV Escaping for Different Cases
```scala
private def escapeCsvField(field: String, isNull: Boolean): String = {
  // NULL → empty string (PostgreSQL COPY NULL representation)
  if (isNull) return ""
  
  // Empty string → quoted empty string (distinguish from NULL)
  if (field.isEmpty) return "\"\""
  
  // Whitespace-only → must be quoted to preserve whitespace
  val isWhitespaceOnly = field.trim.isEmpty && field.nonEmpty
  
  // Non-ASCII → must be quoted
  val hasNonASCII = !field.matches("^[\\x20-\\x7E]*$")
  
  val needsQuoting = isWhitespaceOnly || hasNonASCII || ...
  
  if (needsQuoting) {
    val escaped = field.replace("\"", "\"\"")
    s""""$escaped""""
  } else {
    field
  }
}
```

### 3. CSV Format Rules (PostgreSQL COPY)

| Value Type | CSV Representation | Example |
|------------|-------------------|---------|
| NULL | Empty string | `` (nothing) |
| Empty string | Quoted empty | `""` |
| Whitespace-only | Quoted with spaces | `"   "` |
| String with spaces | Quoted if leading/trailing | `"value "` |
| Non-ASCII | Quoted | `"café"` |
| Normal string | Unquoted | `value` |

## Changes Made

### Files Modified

1. **`RowTransformer.scala`**:
   - Changed `toCsv()` to use `row.isNullAt()` for proper NULL detection
   - Updated `escapeCsvField()` to accept `isNull` parameter
   - Added logic to quote whitespace-only strings
   - Added logic to quote non-ASCII strings
   - Added logic to quote empty strings (distinguish from NULL)

2. **`DataTypeConverter.scala`**:
   - Added defensive check: throws exception if called with null
   - Added documentation explaining proper null checking

## Testing

### Test Cases

1. **NULL value**:
   - Input: `row.isNullAt(0) == true`
   - Expected CSV: `` (empty string)
   - Expected in DB: `NULL`

2. **Empty string**:
   - Input: `""`
   - Expected CSV: `""`
   - Expected in DB: `""` (empty string, not NULL)

3. **Whitespace-only string**:
   - Input: `"   "` (3 spaces)
   - Expected CSV: `"   "`
   - Expected in DB: `"   "` (3 spaces, not NULL)

4. **String with leading/trailing spaces**:
   - Input: `" value "`
   - Expected CSV: `" value "`
   - Expected in DB: `" value "` (preserved)

5. **Non-ASCII characters**:
   - Input: `"café"` or `"北京"`
   - Expected CSV: `"café"` or `"北京"`
   - Expected in DB: `"café"` or `"北京"` (preserved)

6. **Normal string**:
   - Input: `"value"`
   - Expected CSV: `value` (unquoted)
   - Expected in DB: `"value"`

## Why JDBC Batch Inserts Worked

JDBC batch inserts use parameterized statements:
```java
stmt.setString(1, "   ");  // Preserves whitespace
stmt.setString(1, "");     // Empty string (not NULL)
stmt.setNull(1, Types.VARCHAR);  // Explicit NULL
```

JDBC correctly distinguishes:
- `setString()` with empty string → empty string in DB
- `setString()` with spaces → spaces in DB
- `setNull()` → NULL in DB

COPY FROM STDIN requires proper CSV formatting to achieve the same distinction.

## Verification

After this fix:
- ✅ NULL values → NULL in database
- ✅ Empty strings → Empty strings in database (not NULL)
- ✅ Whitespace-only strings → Preserved in database (not NULL)
- ✅ Non-ASCII characters → Preserved correctly
- ✅ NOT NULL constraints → No violations for whitespace-only or non-ASCII values

## Related Configuration

In `migration.properties`:
```properties
yugabyte.csvNull=          # Empty string represents NULL
yugabyte.csvQuote="        # Quote character
yugabyte.csvEscape="       # Escape character (doubled quotes)
```

These settings work together with the fix to ensure proper CSV formatting.

