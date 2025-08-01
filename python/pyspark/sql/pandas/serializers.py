#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Serializers for PyArrow and pandas conversions. See `pyspark.serializers` for more details.
"""

from decimal import Decimal
from itertools import groupby
from typing import TYPE_CHECKING, Optional

import pyspark
from pyspark.errors import PySparkRuntimeError, PySparkTypeError, PySparkValueError
from pyspark.loose_version import LooseVersion
from pyspark.serializers import (
    Serializer,
    read_int,
    write_int,
    UTF8Deserializer,
    CPickleSerializer,
)
from pyspark.sql import Row
from pyspark.sql.conversion import LocalDataToArrowConversion, ArrowTableToRowsConversion
from pyspark.sql.pandas.types import (
    from_arrow_type,
    is_variant,
    to_arrow_type,
    _create_converter_from_pandas,
    _create_converter_to_pandas,
)
from pyspark.sql.types import (
    DataType,
    StringType,
    StructType,
    BinaryType,
    StructField,
    LongType,
    IntegerType,
)

if TYPE_CHECKING:
    import pandas as pd
    import pyarrow as pa


class SpecialLengths:
    END_OF_DATA_SECTION = -1
    PYTHON_EXCEPTION_THROWN = -2
    TIMING_DATA = -3
    END_OF_STREAM = -4
    NULL = -5
    START_ARROW_STREAM = -6


class ArrowCollectSerializer(Serializer):
    """
    Deserialize a stream of batches followed by batch order information. Used in
    PandasConversionMixin._collect_as_arrow() after invoking Dataset.collectAsArrowToPython()
    in the JVM.
    """

    def __init__(self):
        self.serializer = ArrowStreamSerializer()

    def dump_stream(self, iterator, stream):
        return self.serializer.dump_stream(iterator, stream)

    def load_stream(self, stream):
        """
        Load a stream of un-ordered Arrow RecordBatches, where the last iteration yields
        a list of indices that can be used to put the RecordBatches in the correct order.
        """
        # load the batches
        for batch in self.serializer.load_stream(stream):
            yield batch

        # load the batch order indices or propagate any error that occurred in the JVM
        num = read_int(stream)
        if num == -1:
            error_msg = UTF8Deserializer().loads(stream)
            raise PySparkRuntimeError(
                errorClass="ERROR_OCCURRED_WHILE_CALLING",
                messageParameters={
                    "func_name": "ArrowCollectSerializer.load_stream",
                    "error_msg": error_msg,
                },
            )
        batch_order = []
        for i in range(num):
            index = read_int(stream)
            batch_order.append(index)
        yield batch_order

    def __repr__(self):
        return "ArrowCollectSerializer(%s)" % self.serializer


class ArrowStreamSerializer(Serializer):
    """
    Serializes Arrow record batches as a stream.
    """

    def dump_stream(self, iterator, stream):
        import pyarrow as pa

        writer = None
        try:
            for batch in iterator:
                if writer is None:
                    writer = pa.RecordBatchStreamWriter(stream, batch.schema)
                writer.write_batch(batch)
        finally:
            if writer is not None:
                writer.close()

    def load_stream(self, stream):
        import pyarrow as pa

        reader = pa.ipc.open_stream(stream)
        for batch in reader:
            yield batch

    def __repr__(self):
        return "ArrowStreamSerializer"


class ArrowStreamUDFSerializer(ArrowStreamSerializer):
    """
    Same as :class:`ArrowStreamSerializer` but it flattens the struct to Arrow record batch
    for applying each function with the raw record arrow batch. See also `DataFrame.mapInArrow`.
    """

    def load_stream(self, stream):
        """
        Flatten the struct into Arrow's record batches.
        """
        import pyarrow as pa

        batches = super(ArrowStreamUDFSerializer, self).load_stream(stream)
        for batch in batches:
            struct = batch.column(0)
            yield [pa.RecordBatch.from_arrays(struct.flatten(), schema=pa.schema(struct.type))]

    def dump_stream(self, iterator, stream):
        """
        Override because Pandas UDFs require a START_ARROW_STREAM before the Arrow stream is sent.
        This should be sent after creating the first record batch so in case of an error, it can
        be sent back to the JVM before the Arrow stream starts.
        """
        import pyarrow as pa

        def wrap_and_init_stream():
            should_write_start_length = True
            for batch, _ in iterator:
                assert isinstance(batch, pa.RecordBatch)

                # Wrap the root struct
                if len(batch.columns) == 0:
                    # When batch has no column, it should still create
                    # an empty batch with the number of rows set.
                    struct = pa.array([{}] * batch.num_rows)
                else:
                    struct = pa.StructArray.from_arrays(
                        batch.columns, fields=pa.struct(list(batch.schema))
                    )
                batch = pa.RecordBatch.from_arrays([struct], ["_0"])

                # Write the first record batch with initialization.
                if should_write_start_length:
                    write_int(SpecialLengths.START_ARROW_STREAM, stream)
                    should_write_start_length = False
                yield batch

        return super(ArrowStreamUDFSerializer, self).dump_stream(wrap_and_init_stream(), stream)


class ArrowStreamUDTFSerializer(ArrowStreamUDFSerializer):
    """
    Same as :class:`ArrowStreamUDFSerializer` but it does not flatten when loading batches.
    """

    def load_stream(self, stream):
        return ArrowStreamSerializer.load_stream(self, stream)


class ArrowStreamGroupUDFSerializer(ArrowStreamUDFSerializer):
    """
    Serializes pyarrow.RecordBatch data with Arrow streaming format.

    Loads Arrow record batches as ``[[pyarrow.RecordBatch]]`` (one ``[pyarrow.RecordBatch]`` per
    group) and serializes ``[([pyarrow.RecordBatch], arrow_type)]``.

    Parameters
    ----------
    assign_cols_by_name : bool
        If True, then DataFrames will get columns by name
    """

    def __init__(self, assign_cols_by_name):
        super(ArrowStreamGroupUDFSerializer, self).__init__()
        self._assign_cols_by_name = assign_cols_by_name

    def dump_stream(self, iterator, stream):
        import pyarrow as pa

        # flatten inner list [([pa.RecordBatch], arrow_type)] into [(pa.RecordBatch, arrow_type)]
        # so strip off inner iterator induced by ArrowStreamUDFSerializer.load_stream
        batch_iter = (
            (batch, arrow_type)
            for batches, arrow_type in iterator  # tuple constructed in wrap_grouped_map_arrow_udf
            for batch in batches
        )

        if self._assign_cols_by_name:
            batch_iter = (
                (
                    pa.RecordBatch.from_arrays(
                        [batch.column(field.name) for field in arrow_type],
                        names=[field.name for field in arrow_type],
                    ),
                    arrow_type,
                )
                for batch, arrow_type in batch_iter
            )

        super(ArrowStreamGroupUDFSerializer, self).dump_stream(batch_iter, stream)


class ArrowStreamPandasSerializer(ArrowStreamSerializer):
    """
    Serializes pandas.Series as Arrow data with Arrow streaming format.

    Parameters
    ----------
    timezone : str
        A timezone to respect when handling timestamp values
    safecheck : bool
        If True, conversion from Arrow to Pandas checks for overflow/truncation
    assign_cols_by_name : bool
        If True, then Pandas DataFrames will get columns by name
    int_to_decimal_coercion_enabled : bool
        If True, applies additional coercions in Python before converting to Arrow
        This has performance penalties.
    """

    def __init__(self, timezone, safecheck, int_to_decimal_coercion_enabled):
        super(ArrowStreamPandasSerializer, self).__init__()
        self._timezone = timezone
        self._safecheck = safecheck
        self._int_to_decimal_coercion_enabled = int_to_decimal_coercion_enabled

    @staticmethod
    def _apply_python_coercions(series, arrow_type):
        """
        Apply additional coercions to the series in Python before converting to Arrow:
        - Convert integer series to decimal type.
          When we have a pandas series of integers that needs to be converted to
          pyarrow.decimal128 (with precision < 20), PyArrow fails with precision errors.
          Explicitly cast to Decimal first.

        Parameters
        ----------
        series : pandas.Series
            The series to potentially convert
        arrow_type : pyarrow.DataType
            The target arrow type

        Returns
        -------
        pandas.Series
            The potentially converted pandas series
        """
        import pyarrow.types as types
        import pandas as pd

        # Convert integer series to Decimal objects
        if (
            types.is_decimal(arrow_type)
            and series.dtype.kind in ["i", "u"]  # integer types (signed/unsigned)
            and not series.empty
        ):
            series = series.apply(lambda x: Decimal(x) if pd.notna(x) else None)

        return series

    def arrow_to_pandas(
        self, arrow_column, idx, struct_in_pandas="dict", ndarray_as_list=False, spark_type=None
    ):
        # If the given column is a date type column, creates a series of datetime.date directly
        # instead of creating datetime64[ns] as intermediate data to avoid overflow caused by
        # datetime64[ns] type handling.
        # Cast dates to objects instead of datetime64[ns] dtype to avoid overflow.
        pandas_options = {"date_as_object": True}

        import pyarrow as pa

        if LooseVersion(pa.__version__) >= LooseVersion("13.0.0"):
            # A legacy option to coerce date32, date64, duration, and timestamp
            # time units to nanoseconds when converting to pandas.
            # This option can only be added since 13.0.0.
            pandas_options.update(
                {
                    "coerce_temporal_nanoseconds": True,
                }
            )

        s = arrow_column.to_pandas(**pandas_options)

        # TODO(SPARK-43579): cache the converter for reuse
        converter = _create_converter_to_pandas(
            data_type=spark_type or from_arrow_type(arrow_column.type, prefer_timestamp_ntz=True),
            nullable=True,
            timezone=self._timezone,
            struct_in_pandas=struct_in_pandas,
            error_on_duplicated_field_names=True,
            ndarray_as_list=ndarray_as_list,
        )
        return converter(s)

    def _create_array(self, series, arrow_type, spark_type=None, arrow_cast=False):
        """
        Create an Arrow Array from the given pandas.Series and optional type.

        Parameters
        ----------
        series : pandas.Series
            A single series
        arrow_type : pyarrow.DataType, optional
            If None, pyarrow's inferred type will be used
        spark_type : DataType, optional
            If None, spark type converted from arrow_type will be used
        arrow_cast: bool, optional
            Whether to apply Arrow casting when the user-specified return type mismatches the
            actual return values.

        Returns
        -------
        pyarrow.Array
        """
        import pyarrow as pa
        import pandas as pd

        if isinstance(series.dtype, pd.CategoricalDtype):
            series = series.astype(series.dtypes.categories.dtype)

        if arrow_type is not None:
            dt = spark_type or from_arrow_type(arrow_type, prefer_timestamp_ntz=True)
            # TODO(SPARK-43579): cache the converter for reuse
            conv = _create_converter_from_pandas(
                dt, timezone=self._timezone, error_on_duplicated_field_names=False
            )
            series = conv(series)

            if self._int_to_decimal_coercion_enabled:
                series = self._apply_python_coercions(series, arrow_type)

        if hasattr(series.array, "__arrow_array__"):
            mask = None
        else:
            mask = series.isnull()
        try:
            try:
                return pa.Array.from_pandas(
                    series, mask=mask, type=arrow_type, safe=self._safecheck
                )
            except pa.lib.ArrowInvalid:
                if arrow_cast:
                    return pa.Array.from_pandas(series, mask=mask).cast(
                        target_type=arrow_type, safe=self._safecheck
                    )
                else:
                    raise
        except TypeError as e:
            error_msg = (
                "Exception thrown when converting pandas.Series (%s) "
                "with name '%s' to Arrow Array (%s)."
            )
            raise PySparkTypeError(error_msg % (series.dtype, series.name, arrow_type)) from e
        except ValueError as e:
            error_msg = (
                "Exception thrown when converting pandas.Series (%s) "
                "with name '%s' to Arrow Array (%s)."
            )
            if self._safecheck:
                error_msg = error_msg + (
                    " It can be caused by overflows or other "
                    "unsafe conversions warned by Arrow. Arrow safe type check "
                    "can be disabled by using SQL config "
                    "`spark.sql.execution.pandas.convertToArrowArraySafely`."
                )
            raise PySparkValueError(error_msg % (series.dtype, series.name, arrow_type)) from e

    def _create_batch(self, series):
        """
        Create an Arrow record batch from the given pandas.Series or list of Series,
        with optional type.

        Parameters
        ----------
        series : pandas.Series or list
            A single series, list of series, or list of (series, arrow_type)

        Returns
        -------
        pyarrow.RecordBatch
            Arrow RecordBatch
        """
        import pyarrow as pa

        # Make input conform to
        # [(series1, arrow_type1, spark_type1), (series2, arrow_type2, spark_type2), ...]
        if (
            not isinstance(series, (list, tuple))
            or (len(series) == 2 and isinstance(series[1], pa.DataType))
            or (
                len(series) == 3
                and isinstance(series[1], pa.DataType)
                and isinstance(series[2], DataType)
            )
        ):
            series = [series]
        series = ((s, None) if not isinstance(s, (list, tuple)) else s for s in series)
        series = ((s[0], s[1], None) if len(s) == 2 else s for s in series)

        arrs = [
            self._create_array(s, arrow_type, spark_type) for s, arrow_type, spark_type in series
        ]
        return pa.RecordBatch.from_arrays(arrs, ["_%d" % i for i in range(len(arrs))])

    def dump_stream(self, iterator, stream):
        """
        Make ArrowRecordBatches from Pandas Series and serialize. Input is a single series or
        a list of series accompanied by an optional pyarrow type to coerce the data to.
        """
        batches = (self._create_batch(series) for series in iterator)
        super(ArrowStreamPandasSerializer, self).dump_stream(batches, stream)

    def load_stream(self, stream):
        """
        Deserialize ArrowRecordBatches to an Arrow table and return as a list of pandas.Series.
        """
        batches = super(ArrowStreamPandasSerializer, self).load_stream(stream)
        import pyarrow as pa
        import pandas as pd

        for batch in batches:
            pandas_batches = [
                self.arrow_to_pandas(c, i)
                for i, c in enumerate(pa.Table.from_batches([batch]).itercolumns())
            ]
            if len(pandas_batches) == 0:
                yield [pd.Series([pyspark._NoValue] * batch.num_rows)]
            else:
                yield pandas_batches

    def __repr__(self):
        return "ArrowStreamPandasSerializer"


class ArrowStreamPandasUDFSerializer(ArrowStreamPandasSerializer):
    """
    Serializer used by Python worker to evaluate Pandas UDFs
    """

    def __init__(
        self,
        timezone,
        safecheck,
        assign_cols_by_name,
        df_for_struct=False,
        struct_in_pandas="dict",
        ndarray_as_list=False,
        arrow_cast=False,
        input_types=None,
        int_to_decimal_coercion_enabled=False,
    ):
        super(ArrowStreamPandasUDFSerializer, self).__init__(
            timezone, safecheck, int_to_decimal_coercion_enabled
        )
        self._assign_cols_by_name = assign_cols_by_name
        self._df_for_struct = df_for_struct
        self._struct_in_pandas = struct_in_pandas
        self._ndarray_as_list = ndarray_as_list
        self._arrow_cast = arrow_cast
        self._input_types = input_types

    def arrow_to_pandas(self, arrow_column, idx):
        import pyarrow.types as types

        # If the arrow type is struct, return a pandas dataframe where the fields of the struct
        # correspond to columns in the DataFrame. However, if the arrow struct is actually a
        # Variant, which is an atomic type, treat it as a non-struct arrow type.
        if (
            self._df_for_struct
            and types.is_struct(arrow_column.type)
            and not is_variant(arrow_column.type)
        ):
            import pandas as pd

            series = [
                super(ArrowStreamPandasUDFSerializer, self)
                .arrow_to_pandas(
                    column,
                    i,
                    self._struct_in_pandas,
                    self._ndarray_as_list,
                    spark_type=(
                        self._input_types[idx][i].dataType
                        if self._input_types is not None
                        else None
                    ),
                )
                .rename(field.name)
                for i, (column, field) in enumerate(zip(arrow_column.flatten(), arrow_column.type))
            ]
            s = pd.concat(series, axis=1)
        else:
            s = super(ArrowStreamPandasUDFSerializer, self).arrow_to_pandas(
                arrow_column,
                idx,
                self._struct_in_pandas,
                self._ndarray_as_list,
                spark_type=self._input_types[idx] if self._input_types is not None else None,
            )
        return s

    def _create_struct_array(
        self,
        df: "pd.DataFrame",
        arrow_struct_type: "pa.StructType",
        spark_type: Optional[StructType] = None,
    ):
        """
        Create an Arrow StructArray from the given pandas.DataFrame and arrow struct type.

        Parameters
        ----------
        df : pandas.DataFrame
            A pandas DataFrame
        arrow_struct_type : pyarrow.StructType
            pyarrow struct type

        Returns
        -------
        pyarrow.Array
        """
        import pyarrow as pa

        if len(df.columns) == 0:
            return pa.array([{}] * len(df), arrow_struct_type)
        # Assign result columns by schema name if user labeled with strings
        if self._assign_cols_by_name and any(isinstance(name, str) for name in df.columns):
            struct_arrs = [
                self._create_array(
                    df[field.name],
                    field.type,
                    spark_type=(
                        spark_type[field.name].dataType if spark_type is not None else None
                    ),
                    arrow_cast=self._arrow_cast,
                )
                for field in arrow_struct_type
            ]
        # Assign result columns by position
        else:
            struct_arrs = [
                # the selected series has name '1', so we rename it to field.name
                # as the name is used by _create_array to provide a meaningful error message
                self._create_array(
                    df[df.columns[i]].rename(field.name),
                    field.type,
                    spark_type=spark_type[i].dataType if spark_type is not None else None,
                    arrow_cast=self._arrow_cast,
                )
                for i, field in enumerate(arrow_struct_type)
            ]

        return pa.StructArray.from_arrays(struct_arrs, fields=list(arrow_struct_type))

    def _create_batch(self, series):
        """
        Create an Arrow record batch from the given pandas.Series pandas.DataFrame
        or list of Series or DataFrame, with optional type.

        Parameters
        ----------
        series : pandas.Series or pandas.DataFrame or list
            A single series or dataframe, list of series or dataframe,
            or list of (series or dataframe, arrow_type)

        Returns
        -------
        pyarrow.RecordBatch
            Arrow RecordBatch
        """
        import pandas as pd
        import pyarrow as pa

        # Make input conform to
        # [(series1, arrow_type1, spark_type1), (series2, arrow_type2, spark_type2), ...]
        if (
            not isinstance(series, (list, tuple))
            or (len(series) == 2 and isinstance(series[1], pa.DataType))
            or (
                len(series) == 3
                and isinstance(series[1], pa.DataType)
                and isinstance(series[2], DataType)
            )
        ):
            series = [series]
        series = ((s, None) if not isinstance(s, (list, tuple)) else s for s in series)
        series = ((s[0], s[1], None) if len(s) == 2 else s for s in series)

        arrs = []
        for s, arrow_type, spark_type in series:
            # Variants are represented in arrow as structs with additional metadata (checked by
            # is_variant). If the data type is Variant, return a VariantVal atomic type instead of
            # a dict of two binary values.
            if (
                self._struct_in_pandas == "dict"
                and arrow_type is not None
                and pa.types.is_struct(arrow_type)
                and not is_variant(arrow_type)
            ):
                # A pandas UDF should return pd.DataFrame when the return type is a struct type.
                # If it returns a pd.Series, it should throw an error.
                if not isinstance(s, pd.DataFrame):
                    raise PySparkValueError(
                        "Invalid return type. Please make sure that the UDF returns a "
                        "pandas.DataFrame when the specified return type is StructType."
                    )
                arrs.append(self._create_struct_array(s, arrow_type, spark_type=spark_type))
            else:
                arrs.append(
                    self._create_array(
                        s, arrow_type, spark_type=spark_type, arrow_cast=self._arrow_cast
                    )
                )

        return pa.RecordBatch.from_arrays(arrs, ["_%d" % i for i in range(len(arrs))])

    def dump_stream(self, iterator, stream):
        """
        Override because Pandas UDFs require a START_ARROW_STREAM before the Arrow stream is sent.
        This should be sent after creating the first record batch so in case of an error, it can
        be sent back to the JVM before the Arrow stream starts.
        """

        def init_stream_yield_batches():
            should_write_start_length = True
            for series in iterator:
                batch = self._create_batch(series)
                if should_write_start_length:
                    write_int(SpecialLengths.START_ARROW_STREAM, stream)
                    should_write_start_length = False
                yield batch

        return ArrowStreamSerializer.dump_stream(self, init_stream_yield_batches(), stream)

    def __repr__(self):
        return "ArrowStreamPandasUDFSerializer"


class ArrowStreamArrowUDFSerializer(ArrowStreamSerializer):
    """
    Serializer used by Python worker to evaluate Arrow UDFs
    """

    def __init__(
        self,
        timezone,
        safecheck,
        assign_cols_by_name,
        arrow_cast,
    ):
        super(ArrowStreamArrowUDFSerializer, self).__init__()
        self._timezone = timezone
        self._safecheck = safecheck
        self._assign_cols_by_name = assign_cols_by_name
        self._arrow_cast = arrow_cast

    def _create_array(self, arr, arrow_type, arrow_cast):
        import pyarrow as pa

        assert isinstance(arr, pa.Array)
        assert isinstance(arrow_type, pa.DataType)

        # TODO: should we handle timezone here?

        try:
            return arr
        except pa.lib.ArrowException:
            if arrow_cast:
                return arr.cast(target_type=arrow_type, safe=self._safecheck)
            else:
                raise

    def dump_stream(self, iterator, stream):
        """
        Override because Arrow UDFs require a START_ARROW_STREAM before the Arrow stream is sent.
        This should be sent after creating the first record batch so in case of an error, it can
        be sent back to the JVM before the Arrow stream starts.
        """
        import pyarrow as pa

        def wrap_and_init_stream():
            should_write_start_length = True
            for packed in iterator:
                if len(packed) == 2 and isinstance(packed[1], pa.DataType):
                    # single array UDF in a projection
                    arrs = [self._create_array(packed[0], packed[1], self._arrow_cast)]
                else:
                    # multiple array UDFs in a projection
                    arrs = [self._create_array(t[0], t[1], self._arrow_cast) for t in packed]

                batch = pa.RecordBatch.from_arrays(arrs, ["_%d" % i for i in range(len(arrs))])

                # Write the first record batch with initialization.
                if should_write_start_length:
                    write_int(SpecialLengths.START_ARROW_STREAM, stream)
                    should_write_start_length = False
                yield batch

        return ArrowStreamSerializer.dump_stream(self, wrap_and_init_stream(), stream)

    def __repr__(self):
        return "ArrowStreamArrowUDFSerializer"


class ArrowBatchUDFSerializer(ArrowStreamArrowUDFSerializer):
    """
    Serializer used by Python worker to evaluate Arrow Python UDFs
    when the legacy pandas conversion is disabled
    (instead of legacy ArrowStreamPandasUDFSerializer).

    Parameters
    ----------
    timezone : str
        A timezone to respect when handling timestamp values
    safecheck : bool
        If True, conversion from Arrow to Pandas checks for overflow/truncation
    input_types : bool
        If True, then Pandas DataFrames will get columns by name
    """

    def __init__(
        self,
        timezone,
        safecheck,
        input_types,
    ):
        super().__init__(
            timezone=timezone,
            safecheck=safecheck,
            assign_cols_by_name=False,
            arrow_cast=True,
        )
        self._input_types = input_types

    def load_stream(self, stream):
        """
        Loads a stream of Arrow record batches and converts them to Python values.

        Parameters
        ----------
        stream : object
            Input stream containing Arrow record batches

        Yields
        ------
        list
            List of columns containing list of Python values.
        """
        converters = [
            ArrowTableToRowsConversion._create_converter(dt, none_on_identity=True)
            for dt in self._input_types
        ]

        for batch in super().load_stream(stream):
            columns = [
                [conv(v) for v in column.to_pylist()] if conv is not None else column.to_pylist()
                for column, conv in zip(batch.itercolumns(), converters)
            ]
            if len(columns) == 0:
                yield [[pyspark._NoValue] * batch.num_rows]
            else:
                yield columns

    def dump_stream(self, iterator, stream):
        """
        Dumps an iterator of Python values as a stream of Arrow record batches.

        Parameters
        ----------
        iterator : iterator
            Iterator yielding tuples of (data, arrow_type, spark_type) for single UDF
            or list of tuples for multiple UDFs in a projection
        stream : object
            Output stream to write the Arrow record batches

        Returns
        -------
        object
            Result of writing the Arrow stream via ArrowStreamArrowUDFSerializer dump_stream
        """
        import pyarrow as pa

        def create_array(results, arrow_type, spark_type):
            conv = LocalDataToArrowConversion._create_converter(spark_type, none_on_identity=True)
            converted = [conv(res) for res in results] if conv is not None else results
            try:
                return pa.array(converted, type=arrow_type)
            except pa.lib.ArrowInvalid:
                return pa.array(converted).cast(target_type=arrow_type, safe=self._safecheck)

        def py_to_batch():
            for packed in iterator:
                if len(packed) == 3 and isinstance(packed[1], pa.DataType):
                    # single array UDF in a projection
                    yield create_array(packed[0], packed[1], packed[2]), packed[1]
                else:
                    # multiple array UDFs in a projection
                    yield [(create_array(*t), t[1]) for t in packed]

        return super().dump_stream(py_to_batch(), stream)


class ArrowStreamPandasUDTFSerializer(ArrowStreamPandasUDFSerializer):
    """
    Serializer used by Python worker to evaluate Arrow-optimized Python UDTFs.
    """

    def __init__(self, timezone, safecheck, int_to_decimal_coercion_enabled):
        super(ArrowStreamPandasUDTFSerializer, self).__init__(
            timezone=timezone,
            safecheck=safecheck,
            # The output pandas DataFrame's columns are unnamed.
            assign_cols_by_name=False,
            # Set to 'False' to avoid converting struct type inputs into a pandas DataFrame.
            df_for_struct=False,
            # Defines how struct type inputs are converted. If set to "row", struct type inputs
            # are converted into Rows. Without this setting, a struct type input would be treated
            # as a dictionary. For example, for named_struct('name', 'Alice', 'age', 1),
            # if struct_in_pandas="dict", it becomes {"name": "Alice", "age": 1}
            # if struct_in_pandas="row", it becomes Row(name="Alice", age=1)
            struct_in_pandas="row",
            # When dealing with array type inputs, Arrow converts them into numpy.ndarrays.
            # To ensure consistency across regular and arrow-optimized UDTFs, we further
            # convert these numpy.ndarrays into Python lists.
            ndarray_as_list=True,
            # Enables explicit casting for mismatched return types of Arrow Python UDTFs.
            arrow_cast=True,
            # Enable additional coercions for UDTF serialization
            int_to_decimal_coercion_enabled=int_to_decimal_coercion_enabled,
        )
        self._converter_map = dict()

    def _create_batch(self, series):
        """
        Create an Arrow record batch from the given pandas.Series pandas.DataFrame
        or list of Series or DataFrame, with optional type.

        Parameters
        ----------
        series : pandas.Series or pandas.DataFrame or list
            A single series or dataframe, list of series or dataframe,
            or list of (series or dataframe, arrow_type)

        Returns
        -------
        pyarrow.RecordBatch
            Arrow RecordBatch
        """
        import pandas as pd
        import pyarrow as pa

        # Make input conform to [(series1, type1), (series2, type2), ...]
        if not isinstance(series, (list, tuple)) or (
            len(series) == 2 and isinstance(series[1], pa.DataType)
        ):
            series = [series]
        series = ((s, None) if not isinstance(s, (list, tuple)) else s for s in series)

        arrs = []
        for s, t in series:
            if not isinstance(s, pd.DataFrame):
                raise PySparkValueError(
                    "Output of an arrow-optimized Python UDTFs expects "
                    f"a pandas.DataFrame but got: {type(s)}"
                )

            arrs.append(self._create_struct_array(s, t))

        return pa.RecordBatch.from_arrays(arrs, ["_%d" % i for i in range(len(arrs))])

    def _get_or_create_converter_from_pandas(self, dt):
        if dt not in self._converter_map:
            conv = _create_converter_from_pandas(
                dt,
                timezone=self._timezone,
                error_on_duplicated_field_names=False,
                ignore_unexpected_complex_type_values=True,
            )
            self._converter_map[dt] = conv
        return self._converter_map[dt]

    def _create_array(self, series, arrow_type, spark_type=None, arrow_cast=False):
        """
        Override the `_create_array` method in the superclass to create an Arrow Array
        from a given pandas.Series and an arrow type. The difference here is that we always
        use arrow cast when creating the arrow array. Also, the error messages are specific
        to arrow-optimized Python UDTFs.

        Parameters
        ----------
        series : pandas.Series
            A single series
        arrow_type : pyarrow.DataType, optional
            If None, pyarrow's inferred type will be used
        spark_type : DataType, optional
            If None, spark type converted from arrow_type will be used
        arrow_cast: bool, optional
            Whether to apply Arrow casting when the user-specified return type mismatches the
            actual return values.

        Returns
        -------
        pyarrow.Array
        """
        import pyarrow as pa
        import pandas as pd

        if isinstance(series.dtype, pd.CategoricalDtype):
            series = series.astype(series.dtypes.categories.dtype)

        if arrow_type is not None:
            dt = spark_type or from_arrow_type(arrow_type, prefer_timestamp_ntz=True)
            conv = self._get_or_create_converter_from_pandas(dt)
            series = conv(series)

            if self._int_to_decimal_coercion_enabled:
                series = self._apply_python_coercions(series, arrow_type)

        if hasattr(series.array, "__arrow_array__"):
            mask = None
        else:
            mask = series.isnull()

        try:
            try:
                return pa.Array.from_pandas(
                    series, mask=mask, type=arrow_type, safe=self._safecheck
                )
            except pa.lib.ArrowException:
                if arrow_cast:
                    return pa.Array.from_pandas(series, mask=mask).cast(
                        target_type=arrow_type, safe=self._safecheck
                    )
                else:
                    raise
        except pa.lib.ArrowException:
            # Display the most user-friendly error messages instead of showing
            # arrow's error message. This also works better with Spark Connect
            # where the exception messages are by default truncated.
            raise PySparkRuntimeError(
                errorClass="UDTF_ARROW_TYPE_CAST_ERROR",
                messageParameters={
                    "col_name": series.name,
                    "col_type": str(series.dtype),
                    "arrow_type": arrow_type,
                },
            ) from None

    def __repr__(self):
        return "ArrowStreamPandasUDTFSerializer"


class CogroupArrowUDFSerializer(ArrowStreamGroupUDFSerializer):
    """
    Serializes pyarrow.RecordBatch data with Arrow streaming format.

    Loads Arrow record batches as `[([pa.RecordBatch], [pa.RecordBatch])]` (one tuple per group)
    and serializes `[([pa.RecordBatch], arrow_type)]`.

    Parameters
    ----------
    assign_cols_by_name : bool
        If True, then DataFrames will get columns by name
    """

    def __init__(self, assign_cols_by_name):
        super(CogroupArrowUDFSerializer, self).__init__(assign_cols_by_name)

    def load_stream(self, stream):
        """
        Deserialize Cogrouped ArrowRecordBatches and yield as two `pyarrow.RecordBatch`es.
        """
        dataframes_in_group = None

        while dataframes_in_group is None or dataframes_in_group > 0:
            dataframes_in_group = read_int(stream)

            if dataframes_in_group == 2:
                batches1 = [batch for batch in ArrowStreamSerializer.load_stream(self, stream)]
                batches2 = [batch for batch in ArrowStreamSerializer.load_stream(self, stream)]
                yield batches1, batches2

            elif dataframes_in_group != 0:
                raise PySparkValueError(
                    errorClass="INVALID_NUMBER_OF_DATAFRAMES_IN_GROUP",
                    messageParameters={"dataframes_in_group": str(dataframes_in_group)},
                )


class CogroupPandasUDFSerializer(ArrowStreamPandasUDFSerializer):
    def load_stream(self, stream):
        """
        Deserialize Cogrouped ArrowRecordBatches to a tuple of Arrow tables and yield as two
        lists of pandas.Series.
        """
        import pyarrow as pa

        dataframes_in_group = None

        while dataframes_in_group is None or dataframes_in_group > 0:
            dataframes_in_group = read_int(stream)

            if dataframes_in_group == 2:
                batch1 = [batch for batch in ArrowStreamSerializer.load_stream(self, stream)]
                batch2 = [batch for batch in ArrowStreamSerializer.load_stream(self, stream)]
                yield (
                    [
                        self.arrow_to_pandas(c, i)
                        for i, c in enumerate(pa.Table.from_batches(batch1).itercolumns())
                    ],
                    [
                        self.arrow_to_pandas(c, i)
                        for i, c in enumerate(pa.Table.from_batches(batch2).itercolumns())
                    ],
                )

            elif dataframes_in_group != 0:
                raise PySparkValueError(
                    errorClass="INVALID_NUMBER_OF_DATAFRAMES_IN_GROUP",
                    messageParameters={"dataframes_in_group": str(dataframes_in_group)},
                )


class ApplyInPandasWithStateSerializer(ArrowStreamPandasUDFSerializer):
    """
    Serializer used by Python worker to evaluate UDF for applyInPandasWithState.

    Parameters
    ----------
    timezone : str
        A timezone to respect when handling timestamp values
    safecheck : bool
        If True, conversion from Arrow to Pandas checks for overflow/truncation
    assign_cols_by_name : bool
        If True, then Pandas DataFrames will get columns by name
    state_object_schema : StructType
        The type of state object represented as Spark SQL type
    arrow_max_records_per_batch : int
        Limit of the number of records that can be written to a single ArrowRecordBatch in memory.
    """

    def __init__(
        self,
        timezone,
        safecheck,
        assign_cols_by_name,
        state_object_schema,
        arrow_max_records_per_batch,
        prefers_large_var_types,
        int_to_decimal_coercion_enabled,
    ):
        super(ApplyInPandasWithStateSerializer, self).__init__(
            timezone,
            safecheck,
            assign_cols_by_name,
            int_to_decimal_coercion_enabled=int_to_decimal_coercion_enabled,
        )
        self.pickleSer = CPickleSerializer()
        self.utf8_deserializer = UTF8Deserializer()
        self.state_object_schema = state_object_schema

        self.result_count_df_type = StructType(
            [
                StructField("dataCount", IntegerType()),
                StructField("stateCount", IntegerType()),
            ]
        )

        self.result_count_pdf_arrow_type = to_arrow_type(
            self.result_count_df_type, prefers_large_types=prefers_large_var_types
        )

        self.result_state_df_type = StructType(
            [
                StructField("properties", StringType()),
                StructField("keyRowAsUnsafe", BinaryType()),
                StructField("object", BinaryType()),
                StructField("oldTimeoutTimestamp", LongType()),
            ]
        )

        self.result_state_pdf_arrow_type = to_arrow_type(
            self.result_state_df_type, prefers_large_types=prefers_large_var_types
        )
        self.arrow_max_records_per_batch = arrow_max_records_per_batch

    def load_stream(self, stream):
        """
        Read ArrowRecordBatches from stream, deserialize them to populate a list of pair
        (data chunk, state), and convert the data into a list of pandas.Series.

        Please refer the doc of inner function `gen_data_and_state` for more details how
        this function works in overall.

        In addition, this function further groups the return of `gen_data_and_state` by the state
        instance (same semantic as grouping by grouping key) and produces an iterator of data
        chunks for each group, so that the caller can lazily materialize the data chunk.
        """

        import pyarrow as pa
        import json
        from itertools import groupby
        from pyspark.sql.streaming.state import GroupState

        def construct_state(state_info_col):
            """
            Construct state instance from the value of state information column.
            """

            state_info_col_properties = state_info_col["properties"]
            state_info_col_key_row = state_info_col["keyRowAsUnsafe"]
            state_info_col_object = state_info_col["object"]

            state_properties = json.loads(state_info_col_properties)
            if state_info_col_object:
                state_object = self.pickleSer.loads(state_info_col_object)
            else:
                state_object = None
            state_properties["optionalValue"] = state_object

            return GroupState(
                keyAsUnsafe=state_info_col_key_row,
                valueSchema=self.state_object_schema,
                **state_properties,
            )

        def gen_data_and_state(batches):
            """
            Deserialize ArrowRecordBatches and return a generator of
            `(a list of pandas.Series, state)`.

            The logic on deserialization is following:

            1. Read the entire data part from Arrow RecordBatch.
            2. Read the entire state information part from Arrow RecordBatch.
            3. Loop through each state information:
               3.A. Extract the data out from entire data via the information of data range.
               3.B. Construct a new state instance if the state information is the first occurrence
                    for the current grouping key.
               3.C. Leverage the existing state instance if it is already available for the current
                    grouping key. (Meaning it's not the first occurrence.)
               3.D. Remove the cache of state instance if the state information denotes the data is
                    the last chunk for current grouping key.

            This deserialization logic assumes that Arrow RecordBatches contain the data with the
            ordering that data chunks for same grouping key will appear sequentially.

            This function must avoid materializing multiple Arrow RecordBatches into memory at the
            same time. And data chunks from the same grouping key should appear sequentially, to
            further group them based on state instance (same state instance will be produced for
            same grouping key).
            """

            state_for_current_group = None

            for batch in batches:
                batch_schema = batch.schema
                data_schema = pa.schema([batch_schema[i] for i in range(0, len(batch_schema) - 1)])
                state_schema = pa.schema(
                    [
                        batch_schema[-1],
                    ]
                )

                batch_columns = batch.columns
                data_columns = batch_columns[0:-1]
                state_column = batch_columns[-1]

                data_batch = pa.RecordBatch.from_arrays(data_columns, schema=data_schema)
                state_batch = pa.RecordBatch.from_arrays(
                    [
                        state_column,
                    ],
                    schema=state_schema,
                )

                state_arrow = pa.Table.from_batches([state_batch]).itercolumns()
                state_pandas = [self.arrow_to_pandas(c, i) for i, c in enumerate(state_arrow)][0]

                for state_idx in range(0, len(state_pandas)):
                    state_info_col = state_pandas.iloc[state_idx]

                    if not state_info_col:
                        # no more data with grouping key + state
                        break

                    data_start_offset = state_info_col["startOffset"]
                    num_data_rows = state_info_col["numRows"]
                    is_last_chunk = state_info_col["isLastChunk"]

                    if state_for_current_group:
                        # use the state, we already have state for same group and there should be
                        # some data in same group being processed earlier
                        state = state_for_current_group
                    else:
                        # there is no state being stored for same group, construct one
                        state = construct_state(state_info_col)

                    if is_last_chunk:
                        # discard the state being cached for same group
                        state_for_current_group = None
                    elif not state_for_current_group:
                        # there's no cached state but expected to have additional data in same group
                        # cache the current state
                        state_for_current_group = state

                    data_batch_for_group = data_batch.slice(data_start_offset, num_data_rows)
                    data_arrow = pa.Table.from_batches([data_batch_for_group]).itercolumns()

                    data_pandas = [self.arrow_to_pandas(c, i) for i, c in enumerate(data_arrow)]

                    # state info
                    yield (
                        data_pandas,
                        state,
                    )

        _batches = super(ArrowStreamPandasSerializer, self).load_stream(stream)

        data_state_generator = gen_data_and_state(_batches)

        # state will be same object for same grouping key
        for _state, _data in groupby(data_state_generator, key=lambda x: x[1]):
            yield (
                _data,
                _state,
            )

    def dump_stream(self, iterator, stream):
        """
        Read through an iterator of (iterator of pandas DataFrame, state), serialize them to Arrow
        RecordBatches, and write batches to stream.
        """

        import pandas as pd
        import pyarrow as pa

        def construct_state_pdf(state):
            """
            Construct a pandas DataFrame from the state instance.
            """

            state_properties = state.json().encode("utf-8")
            state_key_row_as_binary = state._keyAsUnsafe
            if state.exists:
                state_object = self.pickleSer.dumps(state._value_schema.toInternal(state._value))
            else:
                state_object = None
            state_old_timeout_timestamp = state.oldTimeoutTimestamp

            state_dict = {
                "properties": [
                    state_properties,
                ],
                "keyRowAsUnsafe": [
                    state_key_row_as_binary,
                ],
                "object": [
                    state_object,
                ],
                "oldTimeoutTimestamp": [
                    state_old_timeout_timestamp,
                ],
            }

            return pd.DataFrame.from_dict(state_dict)

        def construct_record_batch(pdfs, pdf_data_cnt, pdf_schema, state_pdfs, state_data_cnt):
            """
            Construct a new Arrow RecordBatch based on output pandas DataFrames and states. Each
            one matches to the single struct field for Arrow schema. We also need an extra one to
            indicate array length for data and state, so the return value of Arrow RecordBatch will
            have schema with three fields, in `count`, `data`, `state` order.
            (Readers are expected to access the field via position rather than the name. We do
            not guarantee the name of the field.)

            Note that Arrow RecordBatch requires all columns to have all same number of rows,
            hence this function inserts empty data for count/state/data with less elements to
            compensate.
            """

            max_data_cnt = max(1, max(pdf_data_cnt, state_data_cnt))

            # We only use the first row in the count column, and fill other rows to be the same
            # value, hoping it is more friendly for compression, in case it is needed.
            count_dict = {
                "dataCount": [pdf_data_cnt] * max_data_cnt,
                "stateCount": [state_data_cnt] * max_data_cnt,
            }
            count_pdf = pd.DataFrame.from_dict(count_dict)

            empty_row_cnt_in_data = max_data_cnt - pdf_data_cnt
            empty_row_cnt_in_state = max_data_cnt - state_data_cnt

            empty_rows_pdf = pd.DataFrame(
                dict.fromkeys(pa.schema(pdf_schema).names),
                index=[x for x in range(0, empty_row_cnt_in_data)],
            )
            empty_rows_state = pd.DataFrame(
                columns=["properties", "keyRowAsUnsafe", "object", "oldTimeoutTimestamp"],
                index=[x for x in range(0, empty_row_cnt_in_state)],
            )

            pdfs.append(empty_rows_pdf)
            state_pdfs.append(empty_rows_state)

            merged_pdf = pd.concat(pdfs, ignore_index=True)
            merged_state_pdf = pd.concat(state_pdfs, ignore_index=True)

            return self._create_batch(
                [
                    (count_pdf, self.result_count_pdf_arrow_type),
                    (merged_pdf, pdf_schema),
                    (merged_state_pdf, self.result_state_pdf_arrow_type),
                ]
            )

        def serialize_batches():
            """
            Read through an iterator of (iterator of pandas DataFrame, state), and serialize them
            to Arrow RecordBatches.

            This function does batching on constructing the Arrow RecordBatch; a batch will be
            serialized to the Arrow RecordBatch when the total number of records exceeds the
            configured threshold.
            """
            # a set of variables for the state of current batch which will be converted to Arrow
            # RecordBatch.
            pdfs = []
            state_pdfs = []
            pdf_data_cnt = 0
            state_data_cnt = 0

            return_schema = None

            for data in iterator:
                # data represents the result of each call of user function
                packaged_result = data[0]

                # There are two results from the call of user function:
                # 1) iterator of pandas DataFrame (output)
                # 2) updated state instance
                pdf_iter = packaged_result[0][0]
                state = packaged_result[0][1]

                # This is static and won't change across batches.
                return_schema = packaged_result[1]

                for pdf in pdf_iter:
                    # We ignore empty pandas DataFrame.
                    if len(pdf) > 0:
                        pdf_data_cnt += len(pdf)
                        pdfs.append(pdf)

                        # If the total number of records in current batch exceeds the configured
                        # threshold, time to construct the Arrow RecordBatch from the batch.
                        if pdf_data_cnt > self.arrow_max_records_per_batch:
                            batch = construct_record_batch(
                                pdfs, pdf_data_cnt, return_schema, state_pdfs, state_data_cnt
                            )

                            # Reset the variables to start with new batch for further data.
                            pdfs = []
                            state_pdfs = []
                            pdf_data_cnt = 0
                            state_data_cnt = 0

                            yield batch

                # This has to be performed 'after' evaluating all elements in iterator, so that
                # the user function has been completed and the state is guaranteed to be updated.
                state_pdf = construct_state_pdf(state)

                state_pdfs.append(state_pdf)
                state_data_cnt += 1

            # processed all output, but current batch may not be flushed yet.
            if pdf_data_cnt > 0 or state_data_cnt > 0:
                batch = construct_record_batch(
                    pdfs, pdf_data_cnt, return_schema, state_pdfs, state_data_cnt
                )

                yield batch

        def init_stream_yield_batches(batches):
            """
            This function helps to ensure the requirement for Pandas UDFs - Pandas UDFs require a
            START_ARROW_STREAM before the Arrow stream is sent.

            START_ARROW_STREAM should be sent after creating the first record batch so in case of
            an error, it can be sent back to the JVM before the Arrow stream starts.
            """
            should_write_start_length = True

            for batch in batches:
                if should_write_start_length:
                    write_int(SpecialLengths.START_ARROW_STREAM, stream)
                    should_write_start_length = False

                yield batch

        batches_to_write = init_stream_yield_batches(serialize_batches())

        return ArrowStreamSerializer.dump_stream(self, batches_to_write, stream)


class TransformWithStateInPandasSerializer(ArrowStreamPandasUDFSerializer):
    """
    Serializer used by Python worker to evaluate UDF for
    :meth:`pyspark.sql.GroupedData.transformWithStateInPandasSerializer`.

    Parameters
    ----------
    timezone : str
        A timezone to respect when handling timestamp values
    safecheck : bool
        If True, conversion from Arrow to Pandas checks for overflow/truncation
    assign_cols_by_name : bool
        If True, then Pandas DataFrames will get columns by name
    arrow_max_records_per_batch : int
        Limit of the number of records that can be written to a single ArrowRecordBatch in memory.
    """

    def __init__(
        self,
        timezone,
        safecheck,
        assign_cols_by_name,
        arrow_max_records_per_batch,
        int_to_decimal_coercion_enabled,
    ):
        super(TransformWithStateInPandasSerializer, self).__init__(
            timezone,
            safecheck,
            assign_cols_by_name,
            int_to_decimal_coercion_enabled=int_to_decimal_coercion_enabled,
        )
        self.arrow_max_records_per_batch = arrow_max_records_per_batch
        self.key_offsets = None

    def load_stream(self, stream):
        """
        Read ArrowRecordBatches from stream, deserialize them to populate a list of data chunk, and
        convert the data into Rows.

        Please refer the doc of inner function `generate_data_batches` for more details how
        this function works in overall.
        """
        import pyarrow as pa
        from pyspark.sql.streaming.stateful_processor_util import (
            TransformWithStateInPandasFuncMode,
        )

        def generate_data_batches(batches):
            """
            Deserialize ArrowRecordBatches and return a generator of Rows.

            The deserialization logic assumes that Arrow RecordBatches contain the data with the
            ordering that data chunks for same grouping key will appear sequentially.

            This function must avoid materializing multiple Arrow RecordBatches into memory at the
            same time. And data chunks from the same grouping key should appear sequentially.
            """
            for batch in batches:
                data_pandas = [
                    self.arrow_to_pandas(c, i)
                    for i, c in enumerate(pa.Table.from_batches([batch]).itercolumns())
                ]
                key_series = [data_pandas[o] for o in self.key_offsets]
                batch_key = tuple(s[0] for s in key_series)
                yield (batch_key, data_pandas)

        _batches = super(ArrowStreamPandasSerializer, self).load_stream(stream)
        data_batches = generate_data_batches(_batches)

        for k, g in groupby(data_batches, key=lambda x: x[0]):
            yield (TransformWithStateInPandasFuncMode.PROCESS_DATA, k, g)

        yield (TransformWithStateInPandasFuncMode.PROCESS_TIMER, None, None)

        yield (TransformWithStateInPandasFuncMode.COMPLETE, None, None)

    def dump_stream(self, iterator, stream):
        """
        Read through an iterator of (iterator of pandas DataFrame), serialize them to Arrow
        RecordBatches, and write batches to stream.
        """

        def flatten_iterator():
            # iterator: iter[list[(iter[pandas.DataFrame], pdf_type)]]
            for packed in iterator:
                iter_pdf_with_type = packed[0]
                iter_pdf = iter_pdf_with_type[0]
                pdf_type = iter_pdf_with_type[1]
                for pdf in iter_pdf:
                    yield (pdf, pdf_type)

        super().dump_stream(flatten_iterator(), stream)


class TransformWithStateInPandasInitStateSerializer(TransformWithStateInPandasSerializer):
    """
    Serializer used by Python worker to evaluate UDF for
    :meth:`pyspark.sql.GroupedData.transformWithStateInPandasInitStateSerializer`.
    Parameters
    ----------
    Same as input parameters in TransformWithStateInPandasSerializer.
    """

    def __init__(
        self,
        timezone,
        safecheck,
        assign_cols_by_name,
        arrow_max_records_per_batch,
        int_to_decimal_coercion_enabled,
    ):
        super(TransformWithStateInPandasInitStateSerializer, self).__init__(
            timezone,
            safecheck,
            assign_cols_by_name,
            arrow_max_records_per_batch,
            int_to_decimal_coercion_enabled,
        )
        self.init_key_offsets = None

    def load_stream(self, stream):
        import pyarrow as pa
        from pyspark.sql.streaming.stateful_processor_util import (
            TransformWithStateInPandasFuncMode,
        )

        def generate_data_batches(batches):
            """
            Deserialize ArrowRecordBatches and return a generator of pandas.Series list.
            The deserialization logic assumes that Arrow RecordBatches contain the data with the
            ordering that data chunks for same grouping key will appear sequentially.
            See `TransformWithStateInPandasPythonInitialStateRunner` for arrow batch schema sent
             from JVM.
            This function flatten the columns of input rows and initial state rows and feed them
             into the data generator.
            """

            def flatten_columns(cur_batch, col_name):
                state_column = cur_batch.column(cur_batch.schema.get_field_index(col_name))
                state_field_names = [
                    state_column.type[i].name for i in range(state_column.type.num_fields)
                ]
                state_field_arrays = [
                    state_column.field(i) for i in range(state_column.type.num_fields)
                ]
                table_from_fields = pa.Table.from_arrays(
                    state_field_arrays, names=state_field_names
                )
                return table_from_fields

            """
            The arrow batch is written in the schema:
            schema: StructType = new StructType()
                .add("inputData", dataSchema)
                .add("initState", initStateSchema)
            We'll parse batch into Tuples of (key, inputData, initState) and pass into the Python
             data generator. All rows in the same batch have the same grouping key.
            """
            for batch in batches:
                flatten_state_table = flatten_columns(batch, "inputData")
                data_pandas = [
                    self.arrow_to_pandas(c, i)
                    for i, c in enumerate(flatten_state_table.itercolumns())
                ]

                flatten_init_table = flatten_columns(batch, "initState")
                init_data_pandas = [
                    self.arrow_to_pandas(c, i)
                    for i, c in enumerate(flatten_init_table.itercolumns())
                ]
                key_series = [data_pandas[o] for o in self.key_offsets]
                init_key_series = [init_data_pandas[o] for o in self.init_key_offsets]

                if any(s.empty for s in key_series):
                    # If any row is empty, assign batch_key using init_key_series
                    batch_key = tuple(s[0] for s in init_key_series)
                else:
                    # If all rows are non-empty, create batch_key from key_series
                    batch_key = tuple(s[0] for s in key_series)
                yield (batch_key, data_pandas, init_data_pandas)

        _batches = super(ArrowStreamPandasSerializer, self).load_stream(stream)
        data_batches = generate_data_batches(_batches)

        for k, g in groupby(data_batches, key=lambda x: x[0]):
            yield (TransformWithStateInPandasFuncMode.PROCESS_DATA, k, g)

        yield (TransformWithStateInPandasFuncMode.PROCESS_TIMER, None, None)

        yield (TransformWithStateInPandasFuncMode.COMPLETE, None, None)


class TransformWithStateInPySparkRowSerializer(ArrowStreamUDFSerializer):
    """
    Serializer used by Python worker to evaluate UDF for
    :meth:`pyspark.sql.GroupedData.transformWithState`.

    Parameters
    ----------
    arrow_max_records_per_batch : int
        Limit of the number of records that can be written to a single ArrowRecordBatch in memory.
    """

    def __init__(self, arrow_max_records_per_batch):
        super(TransformWithStateInPySparkRowSerializer, self).__init__()
        self.arrow_max_records_per_batch = arrow_max_records_per_batch
        self.key_offsets = None

    def load_stream(self, stream):
        """
        Read ArrowRecordBatches from stream, deserialize them to populate a list of data chunks,
        and convert the data into a list of pandas.Series.

        Please refer the doc of inner function `generate_data_batches` for more details how
        this function works in overall.
        """
        from pyspark.sql.streaming.stateful_processor_util import (
            TransformWithStateInPandasFuncMode,
        )
        import itertools

        def generate_data_batches(batches):
            """
            Deserialize ArrowRecordBatches and return a generator of Row.

            The deserialization logic assumes that Arrow RecordBatches contain the data with the
            ordering that data chunks for same grouping key will appear sequentially.

            This function must avoid materializing multiple Arrow RecordBatches into memory at the
            same time. And data chunks from the same grouping key should appear sequentially.
            """
            for batch in batches:
                DataRow = Row(*(batch.schema.names))

                # This is supposed to be the same.
                batch_key = tuple(batch[o][0].as_py() for o in self.key_offsets)
                for row_idx in range(batch.num_rows):
                    row = DataRow(
                        *(batch.column(i)[row_idx].as_py() for i in range(batch.num_columns))
                    )
                    yield (batch_key, row)

        _batches = super(ArrowStreamUDFSerializer, self).load_stream(stream)
        data_batches = generate_data_batches(_batches)

        for k, g in groupby(data_batches, key=lambda x: x[0]):
            chained = itertools.chain(g)
            chained_values = map(lambda x: x[1], chained)
            yield (TransformWithStateInPandasFuncMode.PROCESS_DATA, k, chained_values)

        yield (TransformWithStateInPandasFuncMode.PROCESS_TIMER, None, None)

        yield (TransformWithStateInPandasFuncMode.COMPLETE, None, None)

    def dump_stream(self, iterator, stream):
        """
        Read through an iterator of (iterator of Row), serialize them to Arrow
        RecordBatches, and write batches to stream.
        """
        import pyarrow as pa

        def flatten_iterator():
            # iterator: iter[list[(iter[Row], pdf_type)]]
            for packed in iterator:
                iter_row_with_type = packed[0]
                iter_row = iter_row_with_type[0]
                pdf_type = iter_row_with_type[1]

                rows_as_dict = []
                for row in iter_row:
                    row_as_dict = row.asDict(True)
                    rows_as_dict.append(row_as_dict)

                pdf_schema = pa.schema(list(pdf_type))
                record_batch = pa.RecordBatch.from_pylist(rows_as_dict, schema=pdf_schema)

                yield (record_batch, pdf_type)

        return ArrowStreamUDFSerializer.dump_stream(self, flatten_iterator(), stream)


class TransformWithStateInPySparkRowInitStateSerializer(TransformWithStateInPySparkRowSerializer):
    """
    Serializer used by Python worker to evaluate UDF for
    :meth:`pyspark.sql.GroupedData.transformWithStateInPySparkRowInitStateSerializer`.
    Parameters
    ----------
    Same as input parameters in TransformWithStateInPySparkRowSerializer.
    """

    def __init__(self, arrow_max_records_per_batch):
        super(TransformWithStateInPySparkRowInitStateSerializer, self).__init__(
            arrow_max_records_per_batch
        )
        self.init_key_offsets = None

    def load_stream(self, stream):
        import itertools
        import pyarrow as pa
        from pyspark.sql.streaming.stateful_processor_util import (
            TransformWithStateInPandasFuncMode,
        )

        def generate_data_batches(batches):
            """
            Deserialize ArrowRecordBatches and return a generator of Row.
            The deserialization logic assumes that Arrow RecordBatches contain the data with the
            ordering that data chunks for same grouping key will appear sequentially.
            See `TransformWithStateInPySparkPythonInitialStateRunner` for arrow batch schema sent
             from JVM.
            This function flattens the columns of input rows and initial state rows and feed them
             into the data generator.
            """

            def extract_rows(cur_batch, col_name, key_offsets):
                data_column = cur_batch.column(cur_batch.schema.get_field_index(col_name))
                data_field_names = [
                    data_column.type[i].name for i in range(data_column.type.num_fields)
                ]
                data_field_arrays = [
                    data_column.field(i) for i in range(data_column.type.num_fields)
                ]

                DataRow = Row(*data_field_names)

                table = pa.Table.from_arrays(data_field_arrays, names=data_field_names)

                if table.num_rows == 0:
                    return (None, iter([]))
                else:
                    batch_key = tuple(table.column(o)[0].as_py() for o in key_offsets)

                    rows = []
                    for row_idx in range(table.num_rows):
                        row = DataRow(
                            *(table.column(i)[row_idx].as_py() for i in range(table.num_columns))
                        )
                        rows.append(row)

                    return (batch_key, iter(rows))

            """
            The arrow batch is written in the schema:
            schema: StructType = new StructType()
                .add("inputData", dataSchema)
                .add("initState", initStateSchema)
            We'll parse batch into Tuples of (key, inputData, initState) and pass into the Python
             data generator. All rows in the same batch have the same grouping key.
            """
            for batch in batches:
                (input_batch_key, input_data_iter) = extract_rows(
                    batch, "inputData", self.key_offsets
                )
                (init_batch_key, init_state_iter) = extract_rows(
                    batch, "initState", self.init_key_offsets
                )

                if input_batch_key is None:
                    batch_key = init_batch_key
                else:
                    batch_key = input_batch_key

                for init_state_row in init_state_iter:
                    yield (batch_key, None, init_state_row)

                for input_data_row in input_data_iter:
                    yield (batch_key, input_data_row, None)

        _batches = super(ArrowStreamUDFSerializer, self).load_stream(stream)
        data_batches = generate_data_batches(_batches)

        for k, g in groupby(data_batches, key=lambda x: x[0]):
            # g: list(batch_key, input_data_iter, init_state_iter)

            # they are sharing the iterator, hence need to copy
            input_values_iter, init_state_iter = itertools.tee(g, 2)

            chained_input_values = itertools.chain(map(lambda x: x[1], input_values_iter))
            chained_init_state_values = itertools.chain(map(lambda x: x[2], init_state_iter))

            chained_input_values_without_none = filter(
                lambda x: x is not None, chained_input_values
            )
            chained_init_state_values_without_none = filter(
                lambda x: x is not None, chained_init_state_values
            )

            ret_tuple = (chained_input_values_without_none, chained_init_state_values_without_none)

            yield (TransformWithStateInPandasFuncMode.PROCESS_DATA, k, ret_tuple)

        yield (TransformWithStateInPandasFuncMode.PROCESS_TIMER, None, None)

        yield (TransformWithStateInPandasFuncMode.COMPLETE, None, None)
