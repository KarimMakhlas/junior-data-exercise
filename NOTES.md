# Notes — Patient FHIR Consolidation Exercise

## 1. General approach

The input consists of several CSV files containing patient identity data, IPP identifiers, addresses, and research opposition information.

The output is a consolidated set of FHIR-like `Patient` resources, with one resource per real patient.

## 2. Main assumption

The most important assumption is that the real patient identity is represented by a canonical IPP.

Some IPPs are active, while others are deprecated and point to a principal IPP.

I created a column named `canonical_ipp`.

If an IPP is deprecated and has an `ipp_principal`, I use the principal IPP as the canonical identifier.

Otherwise, I keep the original IPP.

Example:

```text
ipp        statut      ipp_principal   canonical_ipp
700000045  DEPRECIE    800000123       800000123
800000123  ACTIF       null            800000123
```

This avoids producing duplicate patients.

## 3. Patient cleaning

The patient data contains several inconsistencies.

I normalized:

```text
family names
usual names
first names
birth dates
death dates
gender
validity end dates
```

### Names

Family names and usual names are trimmed and converted to uppercase.

Example:

```text
Martin -> MARTIN
```

### First names

The `prenoms` column contains a JSON array encoded as text.

Example:

```text
["Jean"]
["Marie", "Claire"]
```

I parse this column as an array of strings and normalize the casing.

### Dates

The source data contains several date formats.

Supported examples:

```text
1985-03-12
12/03/1985
28-02-1978
1965/09/30
```

I use a safe date parsing strategy with `try_to_date`.

This is important because Spark 4 is strict with date parsing. If a date does not match the first attempted format, a classic `to_date` can fail the job. With `try_to_date`, invalid parsing attempts return `NULL`, and the pipeline can try the next known format.

### Gender

I mapped source gender values to FHIR-compatible values.

Examples:

```text
M, H, Homme, male, 1 -> male
F, Femme, female, 2  -> female
other values         -> unknown
```

FHIR-compatible values used:

```text
male
female
unknown
```

## 4. Patient deduplication

After resolving the canonical IPP, I deduplicate patients using a Spark window function.

The window is partitioned by:

```text
canonical_ipp
```

The ranking strategy is:

```text
1. Prefer records without date_fin_validite
2. Prefer records with a valid birth date
3. Prefer records with a non-empty family name
```

This strategy is intentionally simple and explainable.

The goal is to keep the current and most complete record for each real patient.

## 5. Address handling

Addresses are attached to the canonical IPP.

I clean:

```text
address line
postal code
city
country
start date
end date
address type
```

If an address has an end date, I consider it an old address.

Otherwise, I consider it a home address.

Example mapping:

```text
date_fin is not null -> old
otherwise            -> home
```

Then I group addresses by patient.

## 6. Research opposition

The opposition file contains values with different formats.

I normalized them to boolean values.

Examples:

```text
O, oui, true, 1, Opposé -> true
N, non, false, 0        -> false
empty or unknown        -> null
```

FHIR `Patient` does not have a direct standard field for this specific information in this exercise, so I modeled it as a custom FHIR extension.

Example:

```json
{
  "url": "https://aphp.fr/fhir/StructureDefinition/research-opposition",
  "valueBoolean": true
}
```

## 7. Output format

The pipeline writes two main outputs.

### JSON Lines

```text
output/patients/patient_jsonl/
```

This is the API-oriented output.

Each line contains one FHIR-like `Patient` JSON resource.

### Parquet

```text
output/patients/patient_parquet/
```

This is the Spark/table-oriented output.

It can be useful for downstream analytics or debugging.

## 8. Limitations

Known limitations:

```text
The IPP resolution handles one level of deprecated IPP mapping.
The FHIR resource is FHIR-like but not fully validated against an official FHIR validator.
The research opposition extension uses a custom URL.
The deduplication strategy is rule-based and could be improved with stronger business rules.
Address quality checks are basic.
No automated unit tests are included in this first version.
```

## 9. Possible improvements

With more time, I would add:

```text
unit tests for cleaning functions
data quality reports
FHIR schema validation
recursive IPP resolution if deprecated IPPs can point to other deprecated IPPs
better address validation
logging instead of print statements
CI pipeline with automatic execution
```

## 10. Summary

The main idea of my solution is:

```text
Read messy CSV files
Resolve deprecated IPPs
Create a canonical patient identifier
Clean administrative data
Deduplicate patients
Attach addresses and opposition information
Build one FHIR Patient JSON per real patient
```
