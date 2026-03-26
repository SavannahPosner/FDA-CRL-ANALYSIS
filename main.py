from time import sleep
from functions import *

# get needed secrets
headers, ai_key, table_access = get_secret()
#tables 
letters_df = get_table("fda_risk.raw.letters", table_access)
letters_df['application_number'] = letters_df['application_number'].apply(lambda x: x[0].replace(" ", "") if x is not None and len(x) > 0 else None).astype(str)
letters_df["letter_date"] = pd.to_datetime(letters_df["letter_date"], format='mixed').dt.strftime("%Y%m%d").astype(str)
letters_df["letter_id"] = letters_df['application_number'] + letters_df["letter_date"]
public_safety_df = get_table("fda_risk.transformed.public_safety_risk_reference",table_access)
supply_chain_df = get_table("fda_risk.transformed.supply_chain",table_access)
#get public safety grading ruberic and format for ai input
public_safety_grading_ruberic = public_safety_df[["id","category","subcategory","description"]].to_json(orient="records")
#set up variables to loop through the pandas 
#dataframe, send chuncks to Claude, 
#Union the results into a single
start = 0
next = 10
total = len(letters_df)

while start < total:
    try:
        chunked_input = letters_df[["letter_id","text"]].iloc[start:next].to_json(orient="records")
        prompt, public_safety_output_format = public_safety_prompt(public_safety_grading_ruberic,public_safety_output_format, chunked_input)
        chunked_df = ai_task(prompt, ai_key, public_safety_output_format)
        if start == 0:
            full_df = chunked_df
        else:
            full_df = pd.concat([full_df, chunked_df], ignore_index=True)
        print(f"Processed {min(next, total)} / {total} letters")
        start += 10
        next += 10
        sleep(1)
    except Exception as e:
        print(f"Error processing letters {start} to {next}: {e}")
        start += 10
        next += 10
        sleep(1)
        continue 

#transformations needed to create visuals and upload to DB
base_deductions_dict, multiplier_dict, letters_df, locations_df,columns_dict = get_score_references(table_access)
scores_added_df = gather_scores(full_df,columns_dict, base_deductions_dict, multiplier_dict)
final_df =  rollup_scores(scores_added_df,locations_df)

final_df.to_csv('full_df.csv', index=False)
table_name = "fda_risk.transformed.public_safety_risk_scores"
create_table_and_upload_to_DB(final_df, table_name, table_access)