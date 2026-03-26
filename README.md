# FDA-CRL-ANALYSIS - Currently in Progress
FDA CRL Public Safety Risk Index — Ingests FDA Complete Response Letters via the openFDA API, applies an AI-assisted scoring rubric across deficiency severity, drug type, facility inspection, and outcome severity, and visualizes aggregated facility-level public safety risk scores on an interactive geographic map.

#### About the Public Safety Risk Score
This tool visualizes a proprietary Public Safety Risk Score derived from FDA Complete Response Letters (CRLs) — official correspondence issued when the FDA determines a drug or biologic application cannot be approved in its current form. Each letter was analyzed using an AI-assisted rubric designed to quantify the potential public safety impact of the deficiencies cited.
How the Score is Calculated
Each CRL is evaluated across four dimensions:

####  Deficiency Severity 
— Measures the seriousness of the cited manufacturing, safety, or efficacy deficiencies. This score is weighted by a Drug Type Multiplier, which reflects the relative risk profile of the drug category. Together, these produce a Deficiency Severity Score ranging from 0 (no concern) to -50 (critical concern).
#### Facility Inspection 
— Reflects the findings and resolution status of any facility inspections cited in the letter, contributing up to -25 points.
#### Outcome Severity 
— Captures the potential patient harm associated with the deficiencies identified, contributing up to -25 points.

#### Roll-up Logic
These three components are summed and subtracted from a baseline of 100, yielding a per-letter score ranging from 0 to 100, where a higher score indicates lower public safety risk and a higher score indicates greater concern.
Where a facility has received multiple CRLs, scores are averaged across all letters. For each additional letter beyond the first, the aggregated "Roll-Up" score is penalized by 10 points, reflecting the elevated risk associated with repeat regulatory action. The Roll-Up score is floor-capped at 0.
Interpreting the Score
Score RangeInterpretation91 – 100Minimal public safety concern71 – 90Low public safety concern51 – 70Moderate public safety concern36 – 50Elevated public safety concern16 – 35High public safety concern0 – 15Critical public safety concern
Limitations
This score is derived from AI-assisted analysis of redacted CRL text and should be interpreted with the following caveats in mind. The underlying CRL dataset is a growing but incomplete archive — not all CRLs issued by the FDA are currently published. Redactions in source letters may obscure the full scope of cited deficiencies. The rubric reflects a structured but interpretive framework; scores represent an assessed risk signal, not an official FDA determination.
