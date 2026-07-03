"""
AP-HP Junior Data Exercise — Patient FHIR consolidation pipeline.

Run:
  spark-submit src/patient_pipeline.py --input resources --output output/patients

Outputs:
  output/patients/patient_jsonl/   JSON Lines, one FHIR Patient per line
  output/patients/patient_parquet/ Spark Parquet table
  output/patients/sample/          Small JSON Lines sample
"""

import argparse

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


# ---------------------------------------------------------------------------
# Generic cleaning helpers
# ---------------------------------------------------------------------------


def read_csv(spark: SparkSession, path: str):
    """Read a CSV file using the same options for all input files."""
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


def clean_string(column):
    """Trim spaces and convert empty strings to null."""
    trimmed = F.trim(column)
    return F.when(trimmed == "", F.lit(None)).otherwise(trimmed)


def remove_accents(column):
    """Remove common French accents for easier normalization."""
    return F.translate(
        column,
        "ÉÈÊËÀÂÄÙÛÜÎÏÔÖÇéèêëàâäùûüîïôöç",
        "EEEEAAAUUUIIOOCeeeeaaauuuiiooc",
    )


def upper_clean(column):
    """Trim, collapse spaces, uppercase, and remove accents."""
    normalized = F.upper(F.regexp_replace(clean_string(column), r"\s+", " "))
    return remove_accents(normalized)


def lower_clean(column):
    """Trim, collapse spaces, lowercase, and remove accents."""
    normalized = F.lower(F.regexp_replace(clean_string(column), r"\s+", " "))
    return remove_accents(normalized)


def parse_date(column_name: str):
    """
    Parse several date formats safely.

    Spark 4 is strict with to_date(). try_to_date() returns NULL instead of
    failing, so coalesce() can try the next known format.
    """
    return F.coalesce(
        F.expr(f"try_to_date(`{column_name}`, 'yyyy-MM-dd')"),
        F.expr(f"try_to_date(`{column_name}`, 'dd/MM/yyyy')"),
        F.expr(f"try_to_date(`{column_name}`, 'dd-MM-yyyy')"),
        F.expr(f"try_to_date(`{column_name}`, 'yyyy/MM/dd')"),
    )


def normalize_gender(column):
    """Map source gender values to FHIR Patient.gender values."""
    value = upper_clean(column)
    return (
        F.when(value.isin("M", "H", "HOMME", "MALE", "1"), F.lit("male"))
        .when(value.isin("F", "FEMME", "FEMALE", "2"), F.lit("female"))
        .otherwise(F.lit("unknown"))
    )


def parse_given_names(column):
    """Parse the prenoms column, which contains a JSON array encoded as text."""
    parsed = F.from_json(column, T.ArrayType(T.StringType()))
    return F.transform(parsed, lambda name: F.initcap(F.lower(clean_string(name))))


# ---------------------------------------------------------------------------
# IPP reconciliation
# ---------------------------------------------------------------------------


def build_ipp_mapping(spark: SparkSession, input_dir: str):
    """
    Build ipp -> canonical_ipp mapping.

    A deprecated IPP is attached to its principal IPP. Otherwise, the IPP is
    kept as its own canonical identifier.
    """
    identifiers = read_csv(spark, f"{input_dir}/identifiants_ipp.csv")

    return (
        identifiers
        .withColumn("ipp", clean_string(F.col("ipp")))
        .withColumn("ipp_principal", clean_string(F.col("ipp_principal")))
        .withColumn("statut_norm", upper_clean(F.col("statut")))
        .withColumn(
            "canonical_ipp",
            F.when(
                (F.col("statut_norm") == "DEPRECIE")
                & F.col("ipp_principal").isNotNull(),
                F.col("ipp_principal"),
            ).otherwise(F.col("ipp")),
        )
        .select("ipp", "canonical_ipp")
        .dropDuplicates(["ipp"])
    )


# ---------------------------------------------------------------------------
# Patients
# ---------------------------------------------------------------------------


def build_clean_patients(spark: SparkSession, input_dir: str, ipp_mapping):
    """Read, normalize, and attach canonical_ipp to patients.csv."""
    patients = read_csv(spark, f"{input_dir}/patients.csv")

    return (
        patients
        .withColumn("ipp", clean_string(F.col("ipp")))
        .join(ipp_mapping, on="ipp", how="left")
        .withColumn("canonical_ipp", F.coalesce(F.col("canonical_ipp"), F.col("ipp")))
        .withColumn("family_name", upper_clean(F.col("nom_naissance")))
        .withColumn("usual_name", upper_clean(F.col("nom_usuel")))
        .withColumn("given_names", parse_given_names(F.col("prenoms")))
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
            "validity_end_date",
        )
    )


def deduplicate_patients(cleaned_patients):
    """Keep one current and complete patient record per canonical_ipp."""
    window = (
        Window
        .partitionBy("canonical_ipp")
        .orderBy(
            F.col("validity_end_date").isNull().desc(),
            F.col("birth_date").isNotNull().desc(),
            F.col("family_name").isNotNull().desc(),
            F.col("ipp").desc(),
        )
    )

    return (
        cleaned_patients
        .withColumn("rank", F.row_number().over(window))
        .filter(F.col("rank") == 1)
        .drop("rank")
    )


# ---------------------------------------------------------------------------
# Addresses
# ---------------------------------------------------------------------------


def normalize_address_use(type_column, end_date_column):
    """Map source address information to FHIR address.use."""
    value = upper_clean(type_column)
    return (
        F.when(end_date_column.isNotNull(), F.lit("old"))
        .when(value.isin("ANCIENNE", "OLD"), F.lit("old"))
        .otherwise(F.lit("home"))
    )


def build_addresses(spark: SparkSession, input_dir: str, ipp_mapping):
    """Clean addresses and group them as an array per canonical_ipp."""
    addresses = read_csv(spark, f"{input_dir}/adresses.csv")

    cleaned = (
        addresses
        .withColumn("ipp", clean_string(F.col("ipp")))
        .filter(F.col("ipp").rlike(r"^[0-9]+$"))
        .join(ipp_mapping, on="ipp", how="left")
        .withColumn("canonical_ipp", F.coalesce(F.col("canonical_ipp"), F.col("ipp")))
        .withColumn("line", clean_string(F.col("ligne_adresse")))
        .withColumn("postal_code", clean_string(F.col("code_postal")))
        .withColumn("city", F.initcap(lower_clean(F.col("ville"))))
        .withColumn("country", F.initcap(lower_clean(F.col("pays"))))
        .withColumn("period_start", parse_date("date_debut"))
        .withColumn("period_end", parse_date("date_fin"))
        .withColumn("address_use", normalize_address_use(F.col("type_adresse"), F.col("period_end")))
        .filter(F.col("line").isNotNull())
    )

    dedupe_window = (
        Window
        .partitionBy("canonical_ipp", "line", "postal_code", "city")
        .orderBy(F.col("period_start").desc_nulls_last())
    )

    deduped = (
        cleaned
        .withColumn("rank", F.row_number().over(dedupe_window))
        .filter(F.col("rank") == 1)
        .drop("rank")
    )

    address = F.struct(
        F.col("address_use").alias("use"),
        F.array(F.col("line")).alias("line"),
        F.col("city").alias("city"),
        F.col("postal_code").alias("postalCode"),
        F.col("country").alias("country"),
        F.struct(
            F.date_format(F.col("period_start"), "yyyy-MM-dd").alias("start"),
            F.date_format(F.col("period_end"), "yyyy-MM-dd").alias("end"),
        ).alias("period"),
    )

    return deduped.groupBy("canonical_ipp").agg(F.collect_list(address).alias("address"))


# ---------------------------------------------------------------------------
# Research opposition
# ---------------------------------------------------------------------------


def normalize_opposition(column):
    """Normalize heterogeneous opposition values to boolean."""
    value = upper_clean(column)
    return (
        F.when(value.isin("O", "OUI", "TRUE", "1", "OPPOSE", "OPPOSEE"), F.lit(True))
        .when(value.isin("N", "NON", "FALSE", "0"), F.lit(False))
        .otherwise(F.lit(None).cast("boolean"))
    )


def build_opposition(spark: SparkSession, input_dir: str, ipp_mapping):
    """Keep the latest research opposition value per canonical_ipp."""
    opposition = read_csv(spark, f"{input_dir}/opposition_recherche.csv")

    cleaned = (
        opposition
        .withColumn("ipp", clean_string(F.col("ipp")))
        .join(ipp_mapping, on="ipp", how="left")
        .withColumn("canonical_ipp", F.coalesce(F.col("canonical_ipp"), F.col("ipp")))
        .withColumn("research_opposition", normalize_opposition(F.col("opposition")))
        .withColumn("collection_date", parse_date("date_recueil"))
        .select("canonical_ipp", "research_opposition", "collection_date")
    )

    window = (
        Window
        .partitionBy("canonical_ipp")
        .orderBy(
            F.col("research_opposition").isNotNull().desc(),
            F.col("collection_date").desc_nulls_last(),
        )
    )

    return (
        cleaned
        .withColumn("rank", F.row_number().over(window))
        .filter(F.col("rank") == 1)
        .drop("rank")
    )


# ---------------------------------------------------------------------------
# FHIR resource construction
# ---------------------------------------------------------------------------


def build_fhir_patients(spark: SparkSession, input_dir: str):
    """Build the final FHIR-like Patient resources."""
    ipp_mapping = build_ipp_mapping(spark, input_dir)

    patients = deduplicate_patients(build_clean_patients(spark, input_dir, ipp_mapping))
    addresses = build_addresses(spark, input_dir, ipp_mapping)
    opposition = build_opposition(spark, input_dir, ipp_mapping)

    enriched = (
        patients
        .join(addresses, on="canonical_ipp", how="left")
        .join(opposition, on="canonical_ipp", how="left")
    )

    identifier = F.array(
        F.struct(
            F.lit("https://aphp.fr/identifiers/ipp").alias("system"),
            F.col("canonical_ipp").alias("value"),
        )
    )

    official_name = F.struct(
        F.lit("official").alias("use"),
        F.col("family_name").alias("family"),
        F.col("given_names").alias("given"),
    )

    usual_name = F.struct(
        F.lit("usual").alias("use"),
        F.col("usual_name").alias("family"),
        F.col("given_names").alias("given"),
    )

    names = F.when(
        F.col("usual_name").isNotNull() & (F.col("usual_name") != F.col("family_name")),
        F.array(official_name, usual_name),
    ).otherwise(F.array(official_name))

    research_extension = F.when(
        F.col("research_opposition").isNotNull(),
        F.array(
            F.struct(
                F.lit("https://aphp.fr/fhir/StructureDefinition/research-opposition").alias("url"),
                F.col("research_opposition").alias("valueBoolean"),
            )
        ),
    ).otherwise(F.lit(None).cast("array<struct<url:string,valueBoolean:boolean>>"))

    active = F.col("validity_end_date").isNull() | (F.col("validity_end_date") >= F.current_date())

    patient = F.struct(
        F.lit("Patient").alias("resourceType"),
        F.col("canonical_ipp").alias("id"),
        identifier.alias("identifier"),
        active.alias("active"),
        names.alias("name"),
        F.col("gender").alias("gender"),
        F.date_format(F.col("birth_date"), "yyyy-MM-dd").alias("birthDate"),
        F.date_format(F.col("deceased_date"), "yyyy-MM-dd").alias("deceasedDateTime"),
        F.col("address").alias("address"),
        research_extension.alias("extension"),
    )

    return (
        enriched
        .withColumn("patient", patient)
        .withColumn("patient_json", F.to_json(F.col("patient"), options={"ignoreNullFields": "true"}))
        .select("canonical_ipp", "patient", "patient_json")
    )


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------


def write_outputs(fhir_patients, output_dir: str):
    """Write API-oriented JSON Lines, Spark Parquet, and a small sample."""
    json_lines = fhir_patients.select(F.col("patient_json").alias("value"))

    json_lines.write.mode("overwrite").text(f"{output_dir}/patient_jsonl")
    fhir_patients.write.mode("overwrite").parquet(f"{output_dir}/patient_parquet")
    json_lines.limit(5).coalesce(1).write.mode("overwrite").text(f"{output_dir}/sample")


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def main(input_dir: str, output_dir: str):
    spark = (
        SparkSession.builder
        .appName("aphp-junior-data-exercise")
        .getOrCreate()
    )

    fhir_patients = build_fhir_patients(spark, input_dir)

    print("\n=== Final FHIR Patient sample ===")
    fhir_patients.select("patient_json").show(5, truncate=False)

    patient_count = fhir_patients.count()
    print(f"\nNumber of final patients: {patient_count}")

    write_outputs(fhir_patients, output_dir)

    print("\nPipeline completed successfully.")
    print(f"JSON Lines output: {output_dir}/patient_jsonl")
    print(f"Parquet output:     {output_dir}/patient_parquet")
    print(f"Sample output:      {output_dir}/sample")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build FHIR Patient resources from CSV files.")
    parser.add_argument("--input", required=True, help="Input directory containing CSV files")
    parser.add_argument("--output", required=True, help="Output directory")
    args = parser.parse_args()

    main(args.input, args.output)