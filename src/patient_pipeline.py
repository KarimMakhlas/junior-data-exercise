"""
AP-HP Junior Data Exercise
Patient FHIR R4 consolidation pipeline.

Run from the project root:

  spark-submit src/patient_pipeline.py --input resources --output output/patients

Outputs:
  output/patients/patient_jsonl/   -> one FHIR Patient JSON per line
  output/patients/patient_parquet/ -> same data as nested Spark table
  output/patients/sample/          -> small sample for review
"""

import argparse

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


# ---------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------

def read_csv(spark, path):
    """
    Read a CSV file with header and UTF-8 encoding.
    """
    return (
        spark.read
        .option("header", True)
        .option("sep", ",")
        .option("encoding", "UTF-8")
        .option("multiLine", True)
        .option("quote", '"')
        .option("escape", '"')
        .csv(path)
    )


def clean_string(col):
    """
    Trim spaces and convert empty strings to null.
    """
    return F.when(F.trim(col) == "", F.lit(None)).otherwise(F.trim(col))


def upper_clean(col):
    """
    Normalize text:
    - trim
    - uppercase
    - replace multiple spaces with one space
    """
    return F.upper(F.regexp_replace(clean_string(col), r"\s+", " "))


def lower_clean(col):
    """
    Normalize text:
    - trim
    - lowercase
    - replace multiple spaces with one space
    """
    return F.lower(F.regexp_replace(clean_string(col), r"\s+", " "))


def remove_accents(col):
    """
    Simple accent removal for common French characters.
    Useful for values such as DÉPRÉCIÉ or Opposé.
    """
    return F.translate(
        col,
        "ÉÈÊËÀÂÄÙÛÜÎÏÔÖÇéèêëàâäùûüîïôöç",
        "EEEEAAAUUUIIOOCeeeeaaauuuiiooc"
    )


def normalize_status(col):
    """
    Normalize IPP status.

    Examples:
    ACTIF    -> ACTIF
    actif    -> ACTIF
    DÉPRÉCIÉ -> DEPRECIE
    """
    return remove_accents(upper_clean(col))


def parse_date(col_name):
    """
    Parse dates safely with Spark 4.

    Spark 4 is strict with to_date().
    If the first format does not match, it can crash.

    try_to_date() returns NULL instead of crashing.
    Then coalesce() tries the next format.

    Supported examples:
    - 1985-03-12
    - 12/03/1985
    - 28-02-1978
    - 1965/09/30
    """
    return F.coalesce(
        F.expr(f"try_to_date(`{col_name}`, 'yyyy-MM-dd')"),
        F.expr(f"try_to_date(`{col_name}`, 'dd/MM/yyyy')"),
        F.expr(f"try_to_date(`{col_name}`, 'dd-MM-yyyy')"),
        F.expr(f"try_to_date(`{col_name}`, 'yyyy/MM/dd')")
    )


def normalize_gender(col):
    """
    Convert source gender values to FHIR-compatible values.

    FHIR values:
    - male
    - female
    - other
    - unknown
    """
    value = remove_accents(upper_clean(col))

    return (
        F.when(value.isin("M", "H", "HOMME", "MALE", "1"), F.lit("male"))
        .when(value.isin("F", "FEMME", "FEMALE", "2"), F.lit("female"))
        .otherwise(F.lit("unknown"))
    )


def clean_given_names(col):
    """
    The 'prenoms' column is a JSON array encoded as text.

    Example:
      ["Jean"]
      ["Marie", "Claire"]

    We parse it as array<string> and clean each first name.
    """
    parsed = F.from_json(col, T.ArrayType(T.StringType()))

    return F.transform(
        parsed,
        lambda x: F.initcap(F.lower(clean_string(x)))
    )


# ---------------------------------------------------------------------
# Step 1: IPP reconciliation
# ---------------------------------------------------------------------

def build_identifier_mapping(spark, input_dir):
    """
    Build a mapping table:

      ipp -> canonical_ipp

    If an IPP is deprecated and has an ipp_principal,
    we use ipp_principal as the canonical IPP.

    Otherwise, the IPP remains its own canonical IPP.
    """
    identifiers = read_csv(spark, f"{input_dir}/identifiants_ipp.csv")

    id_mapping = (
        identifiers
        .withColumn("ipp", clean_string(F.col("ipp")))
        .withColumn("ipp_principal", clean_string(F.col("ipp_principal")))
        .withColumn("statut_norm", normalize_status(F.col("statut")))
        .withColumn(
            "canonical_ipp",
            F.when(
                (F.col("statut_norm") == "DEPRECIE")
                & F.col("ipp_principal").isNotNull(),
                F.col("ipp_principal")
            ).otherwise(F.col("ipp"))
        )
        .select(
            "ipp",
            "statut_norm",
            "ipp_principal",
            "canonical_ipp"
        )
    )

    return id_mapping


# ---------------------------------------------------------------------
# Step 2: Patient cleaning and deduplication
# ---------------------------------------------------------------------

def build_clean_patients(spark, input_dir):
    """
    Read patients.csv, attach canonical_ipp, and normalize fields.
    """
    patients = read_csv(spark, f"{input_dir}/patients.csv")
    id_mapping = build_identifier_mapping(spark, input_dir)

    patients_with_ipp = (
        patients
        .withColumn("ipp", clean_string(F.col("ipp")))
        .join(id_mapping, on="ipp", how="left")
        .withColumn(
            "canonical_ipp",
            F.coalesce(F.col("canonical_ipp"), F.col("ipp"))
        )
    )

    cleaned_patients = (
        patients_with_ipp
        .withColumn("family_name", upper_clean(F.col("nom_naissance")))
        .withColumn("usual_name", upper_clean(F.col("nom_usuel")))
        .withColumn("given_names", clean_given_names(F.col("prenoms")))
        .withColumn("birth_date", parse_date("date_naissance"))
        .withColumn("gender", normalize_gender(F.col("sexe")))
        .withColumn("deceased_date", parse_date("date_deces"))
        .withColumn("validity_end_date", parse_date("date_fin_validite"))
        .select(
            "ipp",
            "canonical_ipp",
            "family_name",
            "usual_name",
            "given_names",
            "birth_date",
            "gender",
            "deceased_date",
            "validity_end_date"
        )
    )

    return cleaned_patients


def deduplicate_patients(cleaned_patients):
    """
    Keep one patient row per canonical_ipp.

    Strategy:
    1. Prefer records without validity_end_date.
    2. Prefer records with a valid birth_date.
    3. Prefer records with a non-empty family_name.
    """
    window = (
        Window
        .partitionBy("canonical_ipp")
        .orderBy(
            F.col("validity_end_date").isNull().desc(),
            F.col("birth_date").isNotNull().desc(),
            F.col("family_name").isNotNull().desc(),
            F.col("ipp").desc()
        )
    )

    return (
        cleaned_patients
        .withColumn("row_number", F.row_number().over(window))
        .filter(F.col("row_number") == 1)
        .drop("row_number")
    )


# ---------------------------------------------------------------------
# Step 3: Addresses
# ---------------------------------------------------------------------

def normalize_address_use(type_col, end_date_col):
    """
    Convert address type to FHIR address.use.

    FHIR address.use examples:
    - home
    - old
    """
    value = remove_accents(upper_clean(type_col))

    return (
        F.when(end_date_col.isNotNull(), F.lit("old"))
        .when(value.isin("ANCIENNE", "OLD"), F.lit("old"))
        .otherwise(F.lit("home"))
    )


def build_addresses(spark, input_dir):
    """
    Read adresses.csv and aggregate addresses by canonical_ipp.
    """
    addresses = read_csv(spark, f"{input_dir}/adresses.csv")
    id_mapping = build_identifier_mapping(spark, input_dir)

    cleaned_addresses = (
        addresses
        .withColumn("ipp", clean_string(F.col("ipp")))
        # Keep only rows with a numeric IPP.
        # This avoids corrupted rows if a CSV line is broken.
        .filter(F.col("ipp").rlike(r"^[0-9]+$"))
        .join(id_mapping, on="ipp", how="left")
        .withColumn(
            "canonical_ipp",
            F.coalesce(F.col("canonical_ipp"), F.col("ipp"))
        )
        .withColumn("address_line", clean_string(F.col("ligne_adresse")))
        .withColumn("postal_code", clean_string(F.col("code_postal")))
        .withColumn("city", F.initcap(lower_clean(F.col("ville"))))
        .withColumn("country", F.initcap(lower_clean(F.col("pays"))))
        .withColumn("address_start_date", parse_date("date_debut"))
        .withColumn("address_end_date", parse_date("date_fin"))
        .withColumn(
            "address_use",
            normalize_address_use(F.col("type_adresse"), F.col("address_end_date"))
        )
        .filter(F.col("address_line").isNotNull())
    )

    # Deduplicate exact same address for the same patient.
    window = (
        Window
        .partitionBy("canonical_ipp", "address_line", "postal_code", "city")
        .orderBy(F.col("address_start_date").desc_nulls_last())
    )

    deduped_addresses = (
        cleaned_addresses
        .withColumn("row_number", F.row_number().over(window))
        .filter(F.col("row_number") == 1)
        .drop("row_number")
    )

    address_struct = F.struct(
        F.col("address_use").alias("use"),
        F.array(F.col("address_line")).alias("line"),
        F.col("city").alias("city"),
        F.col("postal_code").alias("postalCode"),
        F.col("country").alias("country"),
        F.struct(
            F.date_format(F.col("address_start_date"), "yyyy-MM-dd").alias("start"),
            F.date_format(F.col("address_end_date"), "yyyy-MM-dd").alias("end")
        ).alias("period")
    )

    addresses_by_patient = (
        deduped_addresses
        .groupBy("canonical_ipp")
        .agg(F.collect_list(address_struct).alias("address"))
    )

    return addresses_by_patient


# ---------------------------------------------------------------------
# Step 4: Opposition recherche
# ---------------------------------------------------------------------

def normalize_opposition(col):
    """
    Normalize opposition values to boolean.

    Examples:
    O, oui, true, Opposé -> true
    N, non, false, 0     -> false
    empty/unknown        -> null
    """
    value = remove_accents(upper_clean(col))

    return (
        F.when(value.isin("O", "OUI", "TRUE", "1", "OPPOSE", "OPPOSEE"), F.lit(True))
        .when(value.isin("N", "NON", "FALSE", "0"), F.lit(False))
        .otherwise(F.lit(None).cast("boolean"))
    )


def build_opposition(spark, input_dir):
    """
    Read opposition_recherche.csv and keep the latest value per canonical_ipp.
    """
    opposition = read_csv(spark, f"{input_dir}/opposition_recherche.csv")
    id_mapping = build_identifier_mapping(spark, input_dir)

    cleaned_opposition = (
        opposition
        .withColumn("ipp", clean_string(F.col("ipp")))
        .join(id_mapping, on="ipp", how="left")
        .withColumn(
            "canonical_ipp",
            F.coalesce(F.col("canonical_ipp"), F.col("ipp"))
        )
        .withColumn("research_opposition", normalize_opposition(F.col("opposition")))
        .withColumn("opposition_collection_date", parse_date("date_recueil"))
        .select(
            "canonical_ipp",
            "research_opposition",
            "opposition_collection_date"
        )
    )

    window = (
        Window
        .partitionBy("canonical_ipp")
        .orderBy(
            F.col("research_opposition").isNotNull().desc(),
            F.col("opposition_collection_date").desc_nulls_last()
        )
    )

    latest_opposition = (
        cleaned_opposition
        .withColumn("row_number", F.row_number().over(window))
        .filter(F.col("row_number") == 1)
        .drop("row_number")
    )

    return latest_opposition


# ---------------------------------------------------------------------
# Step 5: Build FHIR Patient resource
# ---------------------------------------------------------------------

def build_fhir_patients(spark, input_dir):
    """
    Build final FHIR Patient resources.
    """
    cleaned_patients = build_clean_patients(spark, input_dir)
    patients = deduplicate_patients(cleaned_patients)

    addresses = build_addresses(spark, input_dir)
    opposition = build_opposition(spark, input_dir)

    enriched = (
        patients
        .join(addresses, on="canonical_ipp", how="left")
        .join(opposition, on="canonical_ipp", how="left")
    )

    active_col = (
        F.col("validity_end_date").isNull()
        | (F.col("validity_end_date") >= F.current_date())
    )

    official_name = F.struct(
        F.lit("official").alias("use"),
        F.col("family_name").alias("family"),
        F.col("given_names").alias("given")
    )

    usual_name = F.struct(
        F.lit("usual").alias("use"),
        F.col("usual_name").alias("family"),
        F.col("given_names").alias("given")
    )

    name_array = (
        F.when(
            F.col("usual_name").isNotNull()
            & (F.col("usual_name") != F.col("family_name")),
            F.array(official_name, usual_name)
        )
        .otherwise(F.array(official_name))
    )

    identifier_array = F.array(
        F.struct(
            F.lit("https://aphp.fr/identifiers/ipp").alias("system"),
            F.col("canonical_ipp").alias("value")
        )
    )

    opposition_extension = (
        F.when(
            F.col("research_opposition").isNotNull(),
            F.array(
                F.struct(
                    F.lit("https://aphp.fr/fhir/StructureDefinition/research-opposition").alias("url"),
                    F.col("research_opposition").alias("valueBoolean")
                )
            )
        )
        .otherwise(
            F.lit(None).cast("array<struct<url:string,valueBoolean:boolean>>")
        )
    )

    patient_struct = F.struct(
        F.lit("Patient").alias("resourceType"),
        F.col("canonical_ipp").alias("id"),
        identifier_array.alias("identifier"),
        active_col.alias("active"),
        name_array.alias("name"),
        F.col("gender").alias("gender"),
        F.date_format(F.col("birth_date"), "yyyy-MM-dd").alias("birthDate"),
        F.date_format(F.col("deceased_date"), "yyyy-MM-dd").alias("deceasedDateTime"),
        F.col("address").alias("address"),
        opposition_extension.alias("extension")
    )

    fhir_patients = (
        enriched
        .withColumn("patient", patient_struct)
        .withColumn(
            "patient_json",
            F.to_json(F.col("patient"), options={"ignoreNullFields": "true"})
        )
        .select(
            "canonical_ipp",
            "patient",
            "patient_json"
        )
    )

    return fhir_patients


# ---------------------------------------------------------------------
# Step 6: Write outputs
# ---------------------------------------------------------------------

def write_outputs(fhir_patients, output_dir):
    """
    Write:
    - JSON Lines for API usage
    - Parquet for table usage
    - small sample
    """
    json_lines = fhir_patients.select(F.col("patient_json").alias("value"))

    json_lines.write.mode("overwrite").text(f"{output_dir}/patient_jsonl")

    fhir_patients.write.mode("overwrite").parquet(f"{output_dir}/patient_parquet")

    json_lines.limit(5).coalesce(1).write.mode("overwrite").text(f"{output_dir}/sample")


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

def main(input_dir, output_dir):
    spark = (
        SparkSession.builder
        .appName("junior-data-exercise-patient-pipeline")
        .getOrCreate()
    )

    fhir_patients = build_fhir_patients(spark, input_dir)

    print("\n=== Final FHIR Patient sample ===")
    fhir_patients.select("patient_json").show(truncate=False)

    print("\nNumber of final patients:")
    print(fhir_patients.count())

    write_outputs(fhir_patients, output_dir)

    print("\nPipeline completed successfully.")
    print(f"JSON Lines output: {output_dir}/patient_jsonl")
    print(f"Parquet output:     {output_dir}/patient_parquet")
    print(f"Sample output:      {output_dir}/sample")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        required=True,
        help="Input directory containing CSV files"
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output directory"
    )

    args = parser.parse_args()

    main(args.input, args.output)