#!/bin/bash

set -e

echo "Running Patient FHIR consolidation pipeline..."

spark-submit src/patient_pipeline.py \
  --input resources \
  --output output/patients

echo "Pipeline completed."
echo "Sample output:"
cat output/patients/sample/part-*.txt