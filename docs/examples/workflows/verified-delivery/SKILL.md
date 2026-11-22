---
name: verified-delivery
description: Deliver a requested artifact to a known path and verify the saved result against observable criteria.
---

# Verified delivery

1. Identify the required output path, format, and acceptance checks from the request. Treat the input files and paths beyond the assigned output as read-only.
2. Inspect only the inputs needed to produce the output. Save the first usable result at the required path. Retain it while trying a permissible alternative.
3. Check the saved artifact itself. Compare its format and contents with the checks in the request. For a derived result, independently check the source calculation against the saved output.
4. Repair a failing check within scope and rerun the affected checks. Once the output passes, report its path and the evidence from verification.
