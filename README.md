## Objective

The input data is split across several CSV files:

```text
resources/
  patients.csv
  identifiants_ipp.csv
  adresses.csv
  opposition_recherche.csv
```

The pipeline reads these files, cleans the data, reconciles patient identifiers, deduplicates patients, enriches them with addresses and research opposition information, and produces one FHIR-like `Patient` JSON resource per real patient.

## Project structure

```text
junior-data-exercise/
  resources/
    patients.csv
    identifiants_ipp.csv
    adresses.csv
    opposition_recherche.csv

  src/
    patient_pipeline.py

  output/
    sample/
      patient_sample.jsonl

  README.md
  NOTES.md
  requirements.txt
```

## Prerequisites

If you use a Python virtual environment:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## How to run the pipeline

From the project root:

```bash
spark-submit src/patient_pipeline.py --input resources --output output/patients
```
Alternatively, run:

```bash
./run.sh

The pipeline creates the following folders:

```text
output/patients/
  patient_jsonl/
  patient_parquet/
  sample/
```

### JSON Lines output

```text
output/patients/patient_jsonl/
```

This folder contains one FHIR `Patient` JSON resource per line.

To display the generated patients:

```bash
cat output/patients/patient_jsonl/part-*.txt
```

### Parquet output

```text
output/patients/patient_parquet/
```

This folder contains the same patient data as a Spark Parquet table.

### Sample output

```text
output/sample/patient_sample.jsonl
```

This file contains a small sample of generated FHIR `Patient` resources for review.

## Main processing steps

The pipeline follows these steps:

```text
1. Read CSV files with Spark
2. Build an IPP mapping table
3. Resolve deprecated IPPs into a canonical IPP
4. Clean patient administrative data
5. Normalize dates, gender, names, and first names
6. Deduplicate patients using canonical_ipp
7. Clean and attach addresses
8. Clean and attach research opposition
9. Build a FHIR-like Patient resource
10. Export JSON Lines and Parquet outputs
```

## Important concept: canonical_ipp

Some IPPs can be deprecated and attached to a principal IPP.

To avoid creating duplicate patients, I created a `canonical_ipp` column.

Example:

```text
IPP 700000045 -> deprecated -> principal IPP 800000123
```

In the final output, the patient is identified by:

```text
800000123
```

This ensures that each real patient appears only once in the final result.

## FHIR Patient output example

Example of generated output:

```json
{
  "resourceType": "Patient",
  "id": "800000123",
  "identifier": [
    {
      "system": "https://aphp.fr/identifiers/ipp",
      "value": "800000123"
    }
  ],
  "active": true,
  "name": [
    {
      "use": "official",
      "family": "MARTIN",
      "given": ["Jean"]
    }
  ],
  "gender": "male",
  "birthDate": "1985-03-12"
}
```
