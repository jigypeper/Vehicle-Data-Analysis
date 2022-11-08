import pandas as pd

# read base_data to data frame, rename columns to match for join, fill in NaN with 0's
base_data = pd.read_csv("base_data.csv").rename(columns={"Model_Text": "Model", "Options_Code": "Option_Code"}).fillna("0")

# read options_data to data frame, drop Option_Desc column to reduce duplicate columns
options_data = pd.read_csv("options_data.csv").drop(columns=["Option_Desc"])  #["Model", "Option_Desc"]

# add column to options_data for aggregate function
options_data["Car_Type"] = options_data["Model"].astype(str).str[0]

# read vehicle_line_mapping to data frame
vehicle_line_mapping = pd.read_csv("vehicle_line_mapping.csv")

# get average costs by vehicle type
options_averages = options_data.groupby("Car_Type").mean()


options_costs = {
    options_data["Option_Code"][i]: options_data["Material_Cost"][i] for i in range(0, len(options_data)) 
}



# list comprehension to generate model names list and force unique values
models = list(set([vehicle_line_mapping["nameplate_code"][i] for i in range(0, len(vehicle_line_mapping)) ]))

options = list(set([options_data["Option_Code"][i] for i in range(0, len(options_data)) ]))

# replace model_text with model from vehicle line mapping where possible
for model in models:
    for row in base_data.itertuples():
        check = base_data.at[row.Index, "Model"]
        if model in check:
            base_data.at[row.Index, "Model"] = model

# join options table on option code and model
joined_data = base_data.merge(options_data, how="left", on=["Option_Code", "Model"])

# logic for population zeroes and negatives
for row in joined_data.itertuples():
    check_zeroes_negatives = int(joined_data.at[row.Index, "Sales_Price"])
    if check_zeroes_negatives == 0 or check_zeroes_negatives < 0:
        joined_data.at[row.Index, "Material_Cost"] = 0
        

print(len(joined_data_2), len(base_data))
print(joined_data_2)
print(options_averages)


