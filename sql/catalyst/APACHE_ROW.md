# ApacheRow - Alternative Row Format for Spark SQL

## Overview

ApacheRow is a new row format implementation that serves as a portable, object-based alternative to UnsafeRow in Apache Spark SQL. It provides a standardized way to represent row data using Java object arrays, prioritizing compatibility and ease of use over raw performance.

## Key Features

- **Portable Format**: Uses standard Java objects instead of raw memory layout
- **Easy to Use**: Simple array-based storage with intuitive API
- **Compatible**: Works with all Spark SQL data types
- **Flexible**: Avoids off-heap memory usage, suitable for cross-component data exchange
- **Well-Tested**: Comprehensive unit tests for all data types and operations

## When to Use ApacheRow

ApacheRow is ideal for:
- Data exchange between different Spark components
- Scenarios where schema flexibility is important
- Testing and development where readability is valued
- Situations where off-heap memory usage needs to be avoided
- Cases where you need to frequently modify row data

## When to Use UnsafeRow

UnsafeRow remains the best choice for:
- High-performance operations where memory layout matters
- Large-scale data processing where GC overhead is a concern
- Scenarios where rows are mostly read-only
- Cases where you need zero-copy serialization

## Usage Examples

### Creating an ApacheRow

```scala
// Create from values
val row1 = ApacheRow(1, 2L, "hello")

// Create from sequence
val values = Seq(1, 2L, 3.0)
val row2 = ApacheRow.fromSeq(values)

// Create with specific size
val row3 = new ApacheRow(5)
row3.setInt(0, 42)
row3.setLong(1, 100L)
```

### Reading Values

```scala
val row = ApacheRow(1, 2L, 3.0, "test")

val intVal = row.getInt(0)      // 1
val longVal = row.getLong(1)    // 2L
val doubleVal = row.getDouble(2) // 3.0
val strVal = row.getUTF8String(3) // UTF8String("test")
```

### Updating Values

```scala
val row = new ApacheRow(3)

// Using specialized setters
row.setInt(0, 42)
row.setLong(1, 100L)
row.setDouble(2, 3.14)

// Using generic update
row.update(0, 99)

// Setting null values
row.setNullAt(1)
```

### Converting from Other Row Formats

```scala
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.expressions._

// Define schema
val schema = StructType(Seq(
  StructField("id", IntegerType),
  StructField("name", StringType),
  StructField("value", DoubleType)
))

// Convert from GenericInternalRow
val genericRow = new GenericInternalRow(Array[Any](1, UTF8String.fromString("test"), 3.14))
val apacheRow1 = ApacheRow.fromInternalRow(genericRow, schema)

// Convert from UnsafeRow using converter
val unsafeRow: UnsafeRow = ... // obtained from somewhere
val apacheRow2 = ApacheRowConverter.toApacheRow(unsafeRow, schema)
```

### Working with Complex Types

```scala
// Arrays
val row = new ApacheRow(1)
val arrayData = new GenericArrayData(Array(1, 2, 3))
row.update(0, arrayData)
val retrieved = row.getArray(0)

// Nested structs
val innerRow = new ApacheRow(2)
innerRow.setInt(0, 100)
innerRow.update(1, UTF8String.fromString("nested"))

val outerRow = new ApacheRow(1)
outerRow.update(0, innerRow)
val nested = outerRow.getStruct(0, 2)
```

### Copying Rows

```scala
val original = ApacheRow(1, 2L, 3.0)
val copy = original.copy()

// Mutating original doesn't affect copy
original.setInt(0, 100)
assert(copy.getInt(0) == 1)  // Still 1
```

## Implementation Details

### Storage Format

ApacheRow uses a simple `Array[Any]` to store field values:
- Null values are represented as `null` in the array
- Primitive values are boxed Java objects
- Complex types (strings, arrays, structs) are stored as their internal representations

### Inheritance Hierarchy

```
SpecializedGetters (Interface)
  ↓
InternalRow (Abstract Class)
  ↓
BaseGenericInternalRow (Trait)
  ↓
ApacheRow (Class)
```

### Methods Inherited from BaseGenericInternalRow

- All typed getter methods (getInt, getLong, getString, etc.)
- equals() and hashCode() implementations
- toString() representation

### Methods Implemented by ApacheRow

- `genericGet(ordinal: Int)`: Core method to retrieve values
- `numFields`: Returns number of fields
- `setNullAt(i: Int)`: Sets a field to null
- `update(i: Int, value: Any)`: Updates a field value
- `copy()`: Creates a deep copy of the row

## Performance Characteristics

| Operation | ApacheRow | UnsafeRow |
|-----------|-----------|-----------|
| Creation | O(n) | O(n) |
| Read | O(1) with boxing | O(1) zero-copy |
| Update | O(1) | O(1) for mutable fields only |
| Copy | O(n) with deep copy | O(n) memory copy |
| Memory | Heap objects | Contiguous byte array |
| GC Pressure | Higher (object references) | Lower (fewer objects) |

## Testing

ApacheRow includes comprehensive unit tests covering:
- All primitive data types
- Complex types (arrays, maps, structs)
- Null value handling
- Copy operations
- Equality and hashing
- Conversions from other row formats

Run tests with:
```bash
./build/sbt "catalyst/testOnly *ApacheRowSuite"
```

## API Reference

See the scaladoc for complete API documentation:
- `org.apache.spark.sql.catalyst.expressions.ApacheRow`
- `org.apache.spark.sql.catalyst.expressions.ApacheRowConverter`
