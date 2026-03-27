from time import sleep
from functions import *

# get needed secrets
headers, ai_key, table_access,google_api_key = get_secret()
#tables 
letters_df = get_table("fda_risk.raw.letters", table_access)


current_letter_ids = letters_df['letter_id'].tolist()
complete_letters_df, complete_letter_id_list = get_all_crl_records()

new_ids = [letter_id for letter_id in complete_letter_id_list if letter_id not in current_letter_ids]
print(new_ids)
public_safety_df = get_table("fda_risk.transformed.public_safety_risk_reference",table_access)
# supply_chain_df = get_table("fda_risk.transformed.supply_chain",table_access)
# #get public safety grading ruberic and format for ai input
public_safety_grading_ruberic = public_safety_df[["id","category","subcategory","description"]].to_json(orient="records")
# #set up variables to loop through the pandas 
# #dataframe, send chuncks to Claude, 
# #Union the results into a single
start = 0
next = 10
total = len(new_ids)
if len(new_ids) > 0:
    print("New ids to add.")
    #add new letters to the raw letters table
    new_letters_df = complete_letters_df[complete_letters_df['letter_id'].isin(new_ids)]
    new_letters_df['application_number'] = new_letters_df['application_number'].apply(lambda x: [x] if not isinstance(x, list) else x)
    insert_table(new_letters_df, "fda_risk.raw.letters", table_access)
    #add any new locations that aren't stored in the reference table 
    locations_ref = get_table("fda_risk.transformed.locations_ref",table_access)
    loc_letter_ref = locations_ref["letter_id"].to_list()
    #check to make sure I'm only added new locations based on letter_id
    p_new_locations_df = new_letters_df
    new_loc_ids = list(set(new_ids) - set(loc_letter_ref))
    p_new_locations_df[["Lat","Lon"]] = p_new_locations_df.apply(lambda row: get_lat_lon(row, google_api_key), axis=1).tolist()
    new_locations_df = p_new_locations_df[p_new_locations_df['letter_id'].isin(new_loc_ids)]
    new_locations_df = new_locations_df[["letter_id","Lat","Lon"]]

    if len(new_locations_df)>0:
        print("New location(s) to add")
        insert_table(new_locations_df, "fda_risk.transformed.locations_ref", table_access)

    #For any new letters, send to Claude with public risk health scoring ruberic and determine scores 
    while start < total:
        try:
            chunked_input = new_letters_df[["letter_id","text"]].iloc[start:next].to_json(orient="records")
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
    
    print("Claude completed analysis!")
    #transformations needed to create visuals and upload to DB
    if len(full_df)>0:
        base_deductions_dict, multiplier_dict, locations_df,columns_dict = get_score_references(table_access)
        scores_added_df = gather_scores(full_df,columns_dict, base_deductions_dict, multiplier_dict)
        insert_table(scores_added_df, "fda_risk.transformed.individual_public_safety_risk_scores", table_access)
        

        raw_letters_df = get_table("fda_risk.raw.letters", table_access)
        new_letters_df = get_table("fda_risk.transformed.individual_public_safety_risk_scores", table_access)
        new_letters_df = new_letters_df.merge(raw_letters_df[["letter_id","letter_date","company_name","company_address"]], on="letter_id", how="left")
        locations_df = get_table("fda_risk.transformed.locations_ref", table_access)
        final_df = rollup_scores(new_letters_df, locations_df)
        #I'll need to append the new value to letters_df and then rerun the scores on the newly updated table
        replace_table(final_df, "fda_risk.transformed.rolled_up_public_safety_risk_scores", table_access)
        print(final_df.columns.tolist())
        print("Updated all tables, no issues.")
    else: 
        print("No new CRLs to process.")