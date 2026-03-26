
import pandas as pd
import boto3
from botocore.exceptions import ClientError
import json
import requests
import datetime
import anthropic
from prompts import *
from databricks import sql


def get_color(score):
    color_mapping = {
        range(0, 16): "#D62E11",
        range(16, 36): "#FF8170",
        range(36, 51): "#FFB070",
        range(51, 71): "#FFE7AD",
        range(71, 91): "#C9FF8F",
        range(91, 101): "#5CA65B",
    }
    for k, v in color_mapping.items():
        if int(score) in k:
            return v

def get_score_references(table_access):
    path_1 = "fda_risk.transformed.public_safety_risk_reference"
    path_2 = "fda_risk.raw.letters"
    path_3 = "fda_risk.transformed.locations_ref"
    connection = sql.connect(
        server_hostname = "dbc-9ec227be-73c5.cloud.databricks.com",
        http_path = "/sql/1.0/warehouses/823db32bd2b9a836",
        access_token = table_access)

    cursor = connection.cursor()
    cursor.execute(f"""SELECT * FROM {path_1}""")
    df = cursor.fetchall_arrow().to_pandas()

    cursor.execute(f"""SELECT * FROM {path_2}""")
    letters_df = cursor.fetchall_arrow().to_pandas()
    letters_df['application_number'] = letters_df['application_number'].apply(lambda x: x[0].replace(" ", "") if x is not None and len(x) > 0 else None).astype(str)
    letters_df["letter_date"] = pd.to_datetime(letters_df["letter_date"], format='mixed').dt.strftime("%Y%m%d").astype(str)
    letters_df["letter_id"] = letters_df['application_number'] + letters_df["letter_date"]
    letters_df["company_address"] = letters_df["company_address"].str.replace('\n', ' ', regex=False)

    cursor.execute(f"""SELECT * FROM {path_3}""")
    locations_df = cursor.fetchall_arrow().to_pandas()

    cursor.close()
    connection.close()
    base_deductions_dict = df[["id","base_deduction"]].dropna(subset=['base_deduction']).set_index('id')['base_deduction'].to_dict()
    multiplier_dict = df[["id","multiplier"]].dropna(subset=['multiplier']).set_index('id')['multiplier'].to_dict()
    columns_dict = {"deficiency_severity_ps":"baseline_value",
                         "facility_inspection_ps":"baseline_value",
                         "outcome_severity_ps":"baseline_value",
                         "drug_type_multiplier_ps":"multiplier_value"
                         }
    return base_deductions_dict, multiplier_dict, letters_df, locations_df,columns_dict

def gather_scores(df,columns_dict, base_deductions_dict, multiplier_dict):
    df = df.dropna()
    for key,value in columns_dict.items():
        if value == "baseline_value":
            new_column_name = key+"_score_value"
            df[new_column_name] = df[key].map(base_deductions_dict)
        if value == "multiplier_value":
            new_column_name = key+"_score_value"
            df[new_column_name] = df[key].map(multiplier_dict)
    df["ps_total_deficiency_severity_score_value"] = df["deficiency_severity_ps_score_value"] * df["drug_type_multiplier_ps_score_value"]
    df["final_public_safety_risk_score"] = round(100 + df["ps_total_deficiency_severity_score_value"] + df["facility_inspection_ps_score_value"] + df["outcome_severity_ps_score_value"])
    df["final_public_safety_risk_score"] = df["final_public_safety_risk_score"].astype('int64')
    
    return df


# df = pd.read_csv(r"C:\Users\Savan\OneDrive\Desktop\Projects_VSCODE\Personal_Growth_Projects\FDA_FEI_LOOKUP\final_df_1.csv")
def rollup_scores(df,locations_df):
    df["letter_date"] = pd.to_datetime(df["letter_date"], format="%Y%m%d")
    df = df.merge(locations_df, how="inner", on="letter_id")
    address_ref_df = df.groupby(["company_name","company_address"])['final_public_safety_risk_score'].mean().reset_index(name='final_public_safety_risk_score_mean')
   
    address_ref_df = address_ref_df.merge(
        df.groupby(["company_name", "company_address"]).apply(
            lambda g: pd.Series({
                "letter_ids": g["letter_id"].tolist(),
                "_sorted_g": g.sort_values("letter_date")
            })
        ).reset_index(),
        on=["company_name", "company_address"]
    )

    address_ref_df['letter_count'] = address_ref_df['letter_ids'].apply(len)
    address_ref_df['letter_count_detractor'] = (address_ref_df['letter_count'] - 1) * 10
    address_ref_df['agg_final_public_safety_risk_score'] = (address_ref_df['final_public_safety_risk_score_mean'] - address_ref_df['letter_count_detractor']).clip(lower=0)
    address_ref_df["color"] = address_ref_df["agg_final_public_safety_risk_score"].apply(get_color)

    address_ref_df["hover_data"] = address_ref_df.apply(
        lambda row: 
            f"Rolled Up Score: {row['agg_final_public_safety_risk_score']:.2f}<br>" +
            "<br>".join(
                f"* {r['letter_date'].strftime('%Y-%m-%d')} Score: {r['final_public_safety_risk_score']}"
                for _, r in row["_sorted_g"].iterrows()
            ),
        axis=1
    )

    final_df = address_ref_df.drop(columns=["_sorted_g"])
    # final_df = address_ref_df.merge(locations_df, how="inner", on="letter_id")
    return final_df

def get_table(path,table_access):
    connection = sql.connect(
        server_hostname = "dbc-9ec227be-73c5.cloud.databricks.com",
        http_path = "/sql/1.0/warehouses/823db32bd2b9a836",
        access_token = table_access
    )

    cursor = connection.cursor()
    cursor.execute(f"""SELECT * FROM {path}""")
    df = cursor.fetchall_arrow().to_pandas()
    cursor.close()
    connection.close()
    return df

def create_table_and_upload_to_DB(df,table_access,table_name):
    connection = sql.connect(
        server_hostname = "dbc-9ec227be-73c5.cloud.databricks.com",
        http_path = "/sql/1.0/warehouses/823db32bd2b9a836",
        access_token = table_access
    )

    cursor = connection.cursor()
    cursor.copy(
        table_name,
        df
    )
    cursor.close()
    connection.close()

# secret_list = ["fda_fei_api", "anthropic","databricks"]
def get_secret():
    secret_list = ["fda_fei_api", "anthropic","databricks-fda-project"]
    for item in secret_list:
        secret_name = item
        region_name = "us-east-2"

        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name
        )

        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name
            )
        except ClientError as e:
            # For a list of exceptions thrown, see
            # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
            raise e

        secret = get_secret_value_response['SecretString']
        secret_dict = json.loads(secret)
        if item == "fda_fei_api":
            user = secret_dict['authorization-user']
            key = secret_dict['key']
            content_type = secret_dict['content-type']
            headers = {
            'Content-Type': content_type ,
            'Authorization-User': user,
            'Authorization-Key': key,
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
            }
        if item == "anthropic":
            ai_key = secret_dict['key']
        if item == "databricks-fda-project":
            table_access = secret_dict['table_access']

            
    return headers, ai_key, table_access

def fda_fei_request(data,headers):
    signature = str(int(datetime.datetime.now().timestamp()))
    url = 'https://www.accessdata.fda.gov/rest/feiapi/firmdata/feidata/?signature='+signature
    response = requests.post(url, headers=headers, data=data)
    json = response.json()
    print(json)
    row = {}
    address = ""
    LEGALNAME = json["RESULT"]['LEGALNAME']
    row['LEGALNAME'] = LEGALNAME
    FEINUMBER = json["RESULT"]['FEINUMBER']
    row['FEINUMBER'] = FEINUMBER
    FIRMADDRESS1 = json["RESULT"]["MAILINGADDRESS"]["FIRMADDRESS1"]
    address += FIRMADDRESS1+" "
    try: 
        FIRMADDRESS2 = json["RESULT"]["MAILINGADDRESS"]['FIRMADDRESS2']
        address = address+FIRMADDRESS2+" "
    except:
        pass
    try:
        FIRMCITY = json["RESULT"]["MAILINGADDRESS"]['FIRMCITY']
        address = address+FIRMCITY+" "

    except:
        pass
    try:
        FIRMSTATE = json["RESULT"]["MAILINGADDRESS"]['FIRMSTATE']
        address = address+FIRMSTATE+" "
        
    except:
        pass
    try: 
        FIRMZIP = json["RESULT"]["MAILINGADDRESS"]['FIRMZIP']
        address = address+FIRMZIP
        
    except:
        pass
    row["json"] = {"Name": LEGALNAME, "Address": address, "FEINUMBER": FEINUMBER}
    # print(row)
    return row

def get_params_by_round(row):
    Params = []
    All_Addresses_Params = {
        "search":f'''company_name:"{row["Name"]}"+AND+COMPANY_ADDRESS:"{row["Address"]}"'''
    }
    Address_Specific_Params = {
        "search":f'''company_name:"{row["Name"]}"''' 
    }
    Params.append(All_Addresses_Params)
    Params.append(Address_Specific_Params)
    return Params   


def request_transparency(row):
    BASE_URL = "https://api.fda.gov/transparency/crl.json"
    name_only_results = []
    name_address_results = []
    Params = get_params_by_round(row["json"])
    for item in Params:
        skip = 0  # reset for each param set
        while True:
            item['limit'] = 1000
            item['skip'] = skip
            try:
                response = requests.get(BASE_URL, params=item)
                data = response.json()
                if 'error' in data:
                    raise ValueError(f"API error: {data['error']}")
                total = data['meta']['results']['total']
                if 'Address' in item:
                    name_address_results.extend(data.get('results', []))
                else:
                    name_only_results.extend(data.get('results', []))
                skip += 1000
                if skip >= total:
                    break
            except Exception as e:
                # TODO: handle failed requests
                break
    return name_only_results, name_address_results


def ai_task(input, ai_key, output_format):
    client = anthropic.Anthropic(api_key=ai_key)
    response = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=4096,
        messages=[{"role": "user", "content": input}],
        output_config={"format": output_format},
    )
    result = json.loads(response.content[0].text)
    return pd.DataFrame(result["results"])

# headers, ai_key, table_access = get_secret()
# payload = {"FIRMNAME": "Hospira Inc.", "FIRMADDRESS1": "1776 Centennial Dr", "FIRMADDRESS2": "", "FIRMCITY": "McPherson", "FIRMSTATE": "KS", "FIRMCOUNTRY": "US", "FIRMPROVINCE": "", "FIRMZIP": "67460"}
# data = 'payload=' + json.dumps(payload)
# output = fda_fei_request(data,headers)
# print(output)
# headers, ai_key, table_access = get_secret()
# row = fda_fei_request(data, headers)
# name_only_results, name_address_results = request_transparency(row)
# df_name_only_df = pd.DataFrame(name_only_results)
# df_name_only_df['ai_input'] = df_name_only_df["text"].apply(summary_prompt)
# df_name_only_df["ai_summary"] = df_name_only_df["ai_input"].apply(lambda x: ai_task(x, ai_key))
# df_name_only_df.to_csv('filename.csv', index=False)
