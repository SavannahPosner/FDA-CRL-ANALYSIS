import pandas as pd


combined_risk_score_df = pd.DataFrame(columns=[
    "application_number",
    "risk_score_category_id"
    "type",
    "deficiency_severity_ps",
    "deficiency_severity_ps_source",
    "drug_type_multiplier_ps",
    "drug_type_multiplier_ps_source",
    "facility_inspection_ps",
    "facility_inspection_ps_source",
    "outcome_severity_ps",
    "outcome_severity_ps_source",
    "ps_summary",
    "drug_essentiality_sc",
    "drug_essentiality_sc_source",
    "facility_inspection_impact_sc",
    "facility_inspection_impact_sc_source",
    "outcome_consequences_sc",
    "outcome_consequences_sc_source",
    "product_quality_deficiencies_sc",
    "product_quality_deficiencies_sc_source",
    "sc_summary"
])

supply_chain_ouput_df = pd.DataFrame(columns=[
    "application_number",
    "type",
    "risk_score_category_id",
    "drug_essentiality_sc",
    "drug_essentiality_sc_source",
    "facility_inspection_impact_sc",
    "facility_inspection_impact_sc_source",
    "outcome_consequences_sc",
    "outcome_consequences_sc_source",
    "product_quality_deficiencies_sc",
    "product_quality_deficiencies_sc_source",
    "sc_summary"
])

public_safety_output_format = {
    "type": "json_schema",
    "schema": {
        "type": "object",
        "properties": {
            "results": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "letter_id": {"type": "string"},
                        "type": {"type": "string"},
                        "deficiency_severity_ps": {"type": "string"},
                        "deficiency_severity_ps_source": {"type": "string"},
                        "drug_type_multiplier_ps": {"type": "string"},
                        "drug_type_multiplier_ps_source": {"type": "string"},
                        "facility_inspection_ps": {"type": "string"},
                        "facility_inspection_ps_source": {"type": "string"},
                        "outcome_severity_ps": {"type": "string"},
                        "outcome_severity_ps_source": {"type": "string"},
                        "ps_summary": {"type": "string"}
                    },
                    "required": ["application_number", "type", "deficiency_severity_ps", "deficiency_severity_ps_source", "drug_type_multiplier_ps", "drug_type_multiplier_ps_source", "facility_inspection_ps", "facility_inspection_ps_source", "outcome_severity_ps", "outcome_severity_ps_source", "ps_summary"],
                    "additionalProperties": False
                }
            }
        },
        "required": ["results"],
        "additionalProperties": False
    }
}

def public_safety_prompt(public_safety_grading_ruberic, public_safety_output_format, input_records):
    prompt = f"""
    You will receive 10 letters describing US FDA inspections and here is the grading rubric for public safety impacts for each letter. 
    
    {public_safety_grading_ruberic}

    These will be the columns to will return: 
    "letter_id",
    "type",
    "deficiency_severity_ps",
    "deficiency_severity_ps_source",
    "drug_type_multiplier_ps",
    "drug_type_multiplier_ps_source",
    "facility_inspection_ps",
    "facility_inspection_ps_source",
    "outcome_severity_ps",
    "outcome_severity_ps_source",
    "ps_summary"

    You will use this ruberic judge how the letter describes the status of each category: Deficiency Severity, Drug Type Multiplier, Facility Inspection, Outcome Severity. For the values, deficiency_severity_ps, drug_type_multiplier_ps, facility_inspection_ps, and outcome_severity_ps, you will return the id (provided in the ruberic) that represents the subcategory associated with each category that describes this aspect of the letter. Always return an id. If there are multiple ids applicable to the category, choose the most severe subcategory (the lowest base_deduction). 
    If you are unsure about a product/drug type/ or whatever, look it up.

    For the columns deficiency_severity_ps_source, drug_type_multiplier_ps_source, facility_inspection_ps_source, outcome_severity_ps_source, provide the minimum amount of text EXACTLY AS IS in the document that led you to make this decision. Most responses should be a clause, not even a full sentence. Use the text you need to prove you are correct, but no more. If there are multiple clauses, provide the text that provides the most direct/clear reason why you made the decision. If text is redacted show by "[REDACTED]" This will be used to check you. 

    You will also return the type - "Public Safety" is the answer every time. 

    You will also write a 2-4 short sentence summary of the letter describing the public safety concerns resulting from the observations and the steps the offender should take to address them. 

    None of these values should be null when you return them. Here's the input: {input_records}

    """
    return prompt, public_safety_output_format