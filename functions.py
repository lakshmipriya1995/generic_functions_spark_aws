import uuid
from pyspark.sql.functions import (
    lit,
    udf,
    coalesce,
    concat_ws,
    trim,
    col,
    when,
    year,
    month,
    row_number,
    lpad,
    regexp_replace,
    split,
    to_date,
)
from pyspark.sql.types import (
    StringType,
    FloatType,
)
from pyspark.sql import Window
from typing import List
from pyspark.sql import DataFrame

NAMESPACE = uuid.UUID("e4971c26-c4e7-43b7-adcd-3d714dec91cd")


def generate_year_month(df, date_column):
    """Create a year and month column from a given date column

    :param PySpark DataFrame df: Source DataFrame
    :param Dict date_column: Date column name as key and new columns name as value
    :return PySpark DataFrame: DataFrame with added columns
    """

    df = df.withColumn(
        "{}_year".format(list(date_column.values())[0]),
        lit(year(list(date_column.keys())[0])),
    ).withColumn(
        "{}_month".format(list(date_column.values())[0]),
        lit(month(list(date_column.keys())[0])),
    )

    return df


def aggregate_duplicate_rows(
        df,
        aggregate_fields,
):
    """
    Aggregate rows that are duplicates save for the fields listed in aggregate_fields

    Parameters
    ----------
    df : Spark Data Frame to be aggregated
    aggregate_fields : List of fields and aggregate method
    """

    columns_list = df.columns

    # List all fields that are not to be aggregated:
    group_by_fields = columns_list - aggregate_fields.keys()
    df_aggregate = df.groupby(*group_by_fields).agg(aggregate_fields)

    for agg in aggregate_fields:
        df_aggregate = df_aggregate.withColumnRenamed(f"{aggregate_fields[agg]}({agg})", agg)

    # Restore column order
    df_aggregate = df_aggregate.select(columns_list)

    return df_aggregate


def filter_data_on_non_zero_and_positive_values(df, non_zero_positive_values_filters):
    """Filter the DataFrame using the configured non-zero and positive filter values

    :param DataFrame df: Input DataFrame to be filtered
    :param Dict non_zero_positive_values_filters: list of columns that need to be filtered on
    :return DataFrame: Filtered DataFrame
    """
    for field in non_zero_positive_values_filters:
        df = df.filter(col(field) > 0)

    return df


def filter_data_on_non_zero_values(df, non_zero_values_filters):
    """Filter the DataFrame using the configured non-zero filter values

    :param DataFrame df: Input DataFrame to be filtered
    :param Dict non_zero_values_filters: list of columns that need to be filtered on
    :return DataFrame: Filtered DataFrame
    """
    for field in non_zero_values_filters:
        df = df.filter(col(field) != 0)

    return df


def filter_outliers(df, outlier_filters):
    """Filter the DataFrame using the configured outlier filter values

    :param DataFrame df: Input DataFrame to be filtered
    :param Dict outlier_filters: list of columns that need to be filtered on
    :return DataFrame: Filtered DataFrame
    """
    for field in outlier_filters:
        df = df.filter(col(field) <= 200000)

    return df


def transform_field_with_condition(df, transformation_config):
    """Use the configuration to dynamically update a field value.
    The objective is to fix specific data issues by updating the data only when it matches
    a set of conditions (Initially needed for GDA-1182).
    Example: Fix an incorrect field for a set of subsidiaries.

    The available transformation logics are described in the dynamic_field_transform function.
    The filtering logic is described in the dynamic_filter function.

    :param PySpark DataFrame df: Source dataframe
    :param Dict transformation_config: Column name as key and transformation logic as value
    :return PySpark DataFrame: Dataframe with fields updated based on the configuration
    """
    # Each field can have a list of logics to be applied
    for field, logics in transformation_config.items():
        # Foreach transformation logic to be applied on the field
        # Make sure that the dynamic_filter are all exclusive to each other
        for logic in logics:
            # Initialize the when function with a condition that is always false for cleaner code
            new_value = when(lit(False), None)

            filters = dynamic_filter(logic["filter"])
            new_value = new_value.when(filters, dynamic_field_transform(logic))

            # Apply the transformation logic dynamically generated previously
            # Keep the initial value for all rows not matching any condition
            if field in df.columns:
                df = df.withColumn(field, new_value.otherwise(col(field)))
            else:
                df = df.withColumn(field, new_value.otherwise(lit(None)))

    return df


def dynamic_filter(filters):
    """Generate a Boolean Column to be used as the first parameter for a "when" spark function.
    The Boolean is generated for a list of columns each being compared to a list of values.

    :param Dict filters: Column name as key, list of values as value
    :return Boolean Spark Column: To be used as filtering condition in a "when" function
    """
    # Set base filter to True
    spark_filter = lit(True)
    # Then keep adding conditions
    for filter_field, filter_values in filters.items():
        spark_filter = spark_filter & (col(filter_field).isin(filter_values))
    return spark_filter


def dynamic_field_transform(transform_logic):
    """Generate a Spark column with transformation applied to it,
    to be used as the second parameter for a "withColumn" spark function.

    Available transformations:
        -replace: Assign the value of the column "field"
            support left padding with "lpad_length" and "lpad_char" parameters
            support replace with "str_replace" parameter
            support replacing date with "replace_date" parameter
            support replace with "no_null_replace" if "no_null_replace" is not null
            support unit conversion by multiplying fields with "conversion_rate"
            support replacement of "replace_with_null" string with null value
        -key: Generate a surrogate key that concatenate the values of a list
            of existing columns "fields"
            support concatenation with a column of constant values
        -rownumber: Generate a row_number based on the columns "key" as partition key
            and the columns "sort" as order by ascending
        -split: Split the value of the column "field" on a given char and concatenates the
            resulting values
            support left padding when concatenating
        -percentage_to_ratio: Change the percentage "field" to a ratio from 0 to 1

    :param Dict transform_logic: logic parameters.
        The parameter "type" define the transformation logic to be used
        and which other parameters are necessary
    :return Spark Column: To be used as the second parameter for a "withColumn" function
    """
    value = None
    if transform_logic["type"] == "replace":
        value = col(transform_logic["field"])
        if "lpad_length" in transform_logic:
            char = transform_logic.get("lpad_char", "")
            length = int(transform_logic["lpad_length"])
            value = lpad(value, length, char)
        if "no_null_replace" in transform_logic:
            fill_col = col(transform_logic["no_null_replace"])
            fill_col = when(fill_col == '', None).otherwise(fill_col)
            value = coalesce(fill_col, value)
        if "str_replace" in transform_logic:
            for tuple in transform_logic["str_replace"]:
                value = regexp_replace(value, tuple[0], tuple[1])
        if "replace_date" in transform_logic:
            value = coalesce(value, col(transform_logic["replace_date"]))
        if "conversion_rate" in transform_logic:
            conversion_rate = float(transform_logic["conversion_rate"])
            value = conversion_rate * value
        if "replace_with_null" in transform_logic:
            replace_str = transform_logic["replace_with_null"]
            value = when(value == replace_str, None).otherwise(value)
    elif transform_logic["type"] == "key":
        if "lit_value" in transform_logic:
            # Set column to constant value
            col_lit_value = lit(transform_logic["lit_value"])
            # Generate an array of Columns casted to string from an array of string
            col_list = [col(c).cast(StringType()) for c in transform_logic["fields"]]
            # Concatenate the values of constant column and the columns in the list
            # with a given separator
            value = concat_ws(transform_logic["separator"], col_lit_value, *col_list)
        else:
            # Generate an array of Columns casted to string from an array of string
            col_list = [col(c).cast(StringType()) for c in transform_logic["fields"]]
            # Concatenate the values of each column in the list with a given separator
            value = concat_ws(transform_logic["separator"], *col_list)
    elif transform_logic["type"] == "rownumber":
        # Define the window that will be used for the row_number
        window = Window.partitionBy(transform_logic["key"]).orderBy(transform_logic["sort"])
        value = row_number().over(window)
    elif transform_logic["type"] == "split":
        if "lpad_length" in transform_logic:
            # Split the column according to a given char into an array of a given length
            split_cols = split(
                col(transform_logic["field"]),
                transform_logic["split_char"],
                transform_logic["split_length"],
            )
            # Left pad the last split element with given char and length
            char = transform_logic.get("lpad_char", "")
            length = int(transform_logic["lpad_length"])
            if transform_logic["split_length"] == 3:
                lpad_col = lpad(split_cols.getItem(2), length, char)
                # Concatenate the first split element with the padded value,
                # using a given separator
                value = concat_ws(transform_logic["separator"], split_cols.getItem(1), lpad_col)
                value = concat_ws(transform_logic["separator"], split_cols.getItem(0), value)
            else:
                lpad_col = lpad(split_cols.getItem(1), length, char)
                # Concatenate the first split element with the padded value,
                # using a given separator
                value = concat_ws(transform_logic["separator"], split_cols.getItem(0), lpad_col)
        else:
            # Split the column according to a given char into an array of a given length
            split_cols = split(
                col(transform_logic["field"]),
                transform_logic["split_char"],
                transform_logic["split_length"],
            )
            # Concatenate the split elements with a given separator
            value = concat_ws(
                transform_logic["separator"], split_cols.getItem(0), split_cols.getItem(1)
            )
    elif transform_logic["type"] == "percentage_to_ratio":
        value = col(transform_logic["field"])
        fill_value = transform_logic["null_fill_value"]
        range_char = transform_logic["range_char"]
        value = when((value == '') | (value.isNull()), fill_value).otherwise(value)
        value = when(value.like(f"%{range_char}%"),
                     _range_to_avg_perc(value, range_char)
                     ).otherwise(value)
        value = when(value.rlike('%'), regexp_replace(value, "%", "")).otherwise(value)
        value = value.cast(FloatType()) / 100.0
    return value


def _range_to_avg_perc(perc_col, range_char):
    """Take string that is a percentage range, convert to average of the two

    :param perc_col: string column that contains a percentage range.
    :param range_char: character that indicates range of percentage e.g. '-'
    :return: value: string that contains the average value of the percentage range.
    """
    low_range = regexp_replace(split(perc_col, range_char).getItem(0), "%", "").cast(FloatType())
    high_range = regexp_replace(split(perc_col, range_char).getItem(1), "%", "").cast(FloatType())
    value = (low_range + high_range) / 2
    value = value.cast(StringType())
    return value


def handle_faulty_dates(df_spark, date_columns):
    """Take a Spark DataFrame and set dates before 1900-01-01 to be 1900-01-01.
    This function helps overcome an issue faced when upgrading to Spark 3.0
    where (wrongly entered) ancient dates could not be read and resulted
    in the glue job crashing. (Originally described in GDA-2135)

    :param df_spark: Spark source DataFrame
    :param date_columns: list of column names that are date dtype
    :return: df_spark: adjusted Spark dataframe with ancient dates overwritten
    """
    for date_column in date_columns:
        df_spark = df_spark.withColumn(
            date_column,
            when(
                (col(date_column) >= "1900-01-01") | (col(date_column).isNull()), col(date_column)
            ).otherwise(to_date(lit("1900-01-01"), "yyyy-MM-dd")),
        )
    return df_spark




def trim_str_cols_spark_df(df_spark_in):
    """
    Strip white spaces around string columns in SparkDataFrame
    :param df_spark_in: input Spark Dataframe to be trimmed
    :return: df_spark_trimmed: trimmed Spark DataFrame
    """
    # Get string columns in DataFrame:
    str_cols = [
        field.name for field in df_spark_in.schema.fields if isinstance(field.dataType, StringType)
    ]

    # Initialize trimmed DataFrame and strip white spaces around string fields and update DataFrame:
    df_spark_trimmed = df_spark_in
    for my_col in str_cols:
        df_spark_trimmed = df_spark_trimmed.withColumn(my_col, trim(df_spark_in[my_col]))

    return df_spark_trimmed


def add_uuid(df: DataFrame, col_name: str, primary_key_columns: List[str]):
    """Add an additional column with value uuid to df with name col_name

    If the primary key is composite, the columns are concatenated based on the following rules:
    - Columns are coalesced based on either their value or "null"
    - Columns are concatenated with "_" separator

    :param df: The dataFrame to add a uuid column to
    :param col_name: Name of the to-be to added uuid column
    :param primary_key_columns: The columns to generate UUID from"""

    # Define the primary key columns based on coalesce rule specified in DOCSTRING
    # and the pkey columns from the configurations.config file:
    pkey_values = [coalesce(df[pkey_value], lit("null")) for pkey_value in primary_key_columns]

    # Add new column as a concatenation of the previously defined pkey_values based on
    # concatenation rule specified in the docstring:
    df = df.withColumn(col_name, concat_ws("_", *pkey_values))

    # A udf (user defined function) that generates a deterministic UUIDv5 based on a static UUIDv4
    # namespace and a string value (the concatted version of the primary keys). This generates
    # stable keys:
    uuid_udf = udf(lambda x: str(uuid.uuid5(NAMESPACE, x)), returnType=StringType())
    df = df.withColumn(col_name, uuid_udf(df[col_name]))
    return df


def union_df_dict(dfs):
    """Union the non empty dataframes stored in a dictionary of Dataframes
    :param Dict of Spark Dataframe dfs: Dictionary of Dataframes to be combined
    :return Spark Dataframe: Combined Dataframe, None if all the Dataframes are empty
    """
    df_union = None
    for df in dfs.values():
        if not df.rdd.isEmpty():
            if df_union is None:
                df_union = df
            else:
                df_union = df_union.union(df)

    return df_union


def replace_empty_string_with_null(df: DataFrame) -> DataFrame:
    """
    This function takes a Spark DataFrame and, only for the String type columns,
    replaces empty string values with NULL.

    :param df: The Spark DataFrame to be modified.
    :return df (Spark DataFrame): The modified DataFrame.
    """
    # Get string columns in DataFrame:
    str_cols = [field.name for field in df.schema.fields if isinstance(field.dataType, StringType)]

    # Initialize trimmed DataFrame and strip white spaces around string fields and update DataFrame:
    for col_name in str_cols:
        df = df.withColumn(col_name,
                           when(trim(col(col_name)) == "", lit(None)).otherwise(col(col_name)))

    return df
    
def check_file_size(min_file_size, file_size_offloaded, unit, log, margin=0.95):
    """Determine if the size of the currently offloaded file is as expected
    :param str file_name: name of the currently evaluated file
    :param float min_file_size: minimum accepted offload size
    :param str file_size_offloaded: size of the offloaded file
    :param str unit: unit in which file size is reported
    :param logging.Logger log: logger to use to log the messages
    :param float margin: theshold margin, default value 95% of min size is accepted"""

    # Transform the size of the offloaded file to the required unit
    if unit == "KB":
        file_size_offloaded = file_size_offloaded / 1024

    elif unit == "MB":
        file_size_offloaded = file_size_offloaded / 1024 ** 2

    elif unit == "GB":
        file_size_offloaded = file_size_offloaded / 1024 ** 3

    # Check if size of the offloaded file is within 10% of the expected size
    log.info(
        f"Minimum file size threshold: {min_file_size} {unit} "
        f"- Size of the offloaded file: {file_size_offloaded} {unit}"
    )
    if file_size_offloaded < margin * min_file_size:
        return False
    else:
        return True
