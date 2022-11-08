import pandas as pd
import sqlite3

conn = sqlite3.connect("v_data.db")


# read base_data to data frame, rename columns to match for join, fill in NaN with 0's
base_data = pd.read_csv("base_data.csv").rename(columns={"Model_Text": "Model", "Options_Code": "Option_Code"}).fillna("0")
base_data.to_sql(name="base_data", con=conn, if_exists="replace", index=False)

# read options_data to data frame, drop Option_Desc column to reduce duplicate columns
options_data = pd.read_csv("options_data.csv").drop(columns=["Option_Desc"])  #["Model", "Option_Desc"]

# add column to options_data for aggregate function
options_data["Car_Type"] = options_data["Model"].astype(str).str[0]
options_data.to_sql(name="options_data", con=conn, if_exists="replace", index=False)

# read vehicle_line_mapping to data frame
vehicle_line_mapping = pd.read_csv("vehicle_line_mapping.csv")
vehicle_line_mapping.to_sql(name="vehicle_line_mapping", con=conn, if_exists="replace", index=False)

conn.commit()
conn.close()

# Handler for database operations
def database_handeler(query, *args):
    try:
        conn =sqlite3.connect("v_data.db")
        cursor = conn.cursor()
        cursor.execute(query, (*args,))
        conn.commit()
        print("Record Updated successfully ")
        cursor.close()
    except sqlite3.Error as error:
        print("Failed to update sqlite table", error)
    finally:
        if conn:
            conn.close()
            print("The SQLite connection is closed")

# Dataframe initializations
conn =sqlite3.connect("v_data.db")

# read options_data to data frame
options_data = pd.read_sql("SELECT * FROM options_data", conn)

# read vehicle_line_mapping to data frame
vehicle_line_mapping = pd.read_sql("SELECT * FROM vehicle_line_mapping", conn)

# get average costs by vehicle type
options_averages = options_data.groupby("Car_Type").mean().reset_index()

# dictionary comprehension to generate option data and averages for vehicle type
options_costs = {
    options_data["Option_Code"][i]: options_data["Material_Cost"][i] for i in range(0, len(options_data)) 
}

options_averages_dict = {
    options_averages["Car_Type"][i]: options_averages["Material_Cost"][i] for i in range(0, len(options_averages)) 
}

# list comprehension to generate model names list and force unique values
models = list(set([vehicle_line_mapping["nameplate_code"][i] for i in range(0, len(vehicle_line_mapping)) ]))

# Close connection
conn.close

# Update base_data
model_query = ''' UPDATE base_data SET Model = ? WHERE Model LIKE ? '''

for model in models:
    database_handeler(model_query, model, '%'+model+'%')

# Re-open connection
conn = sqlite3.connect("v_data.db")

# read base_data to data frame
base_data = pd.read_sql("SELECT * FROM base_data", conn)

# join options table on option code and model
joined_data = base_data.merge(options_data, how="left", on=["Option_Code", "Model"]).fillna("0")
joined_data.to_sql(name="joined_data", con=conn, if_exists="replace", index=False)

# commit and close
conn.commit()
conn.close()

# Update zeroes and negatives
cost = 0
price = 0
less_than_zero = 0
zeroes_query = ''' UPDATE joined_data SET Material_Cost= @cost WHERE Sales_Price= @price OR Sales_Price < @less_than_zero '''

database_handeler(zeroes_query, cost, price, less_than_zero)

# update car types for jaguar
car_type_x = 'X'
like_x = '%X%'
c_type_j = '%Jaguar%'
car_type_query_x = ''' UPDATE joined_data SET Car_Type= @car_type_x WHERE Model LIKE @like_x OR Model LIKE @c_type_j '''

database_handeler(car_type_query_x, car_type_x, like_x, c_type_j)

# update car types for landrover
car_type_y = 'L'
like_y = '%L%'
c_type_l = '%Land%'
c_type_l_2 = '%Range%'
car_type_query_l = ''' UPDATE joined_data SET Car_Type= @car_type_y WHERE Model LIKE @like_y OR Model LIKE @c_type_l OR Model LIKE @c_type_l_2 '''

database_handeler(car_type_query_l, car_type_y, like_y, c_type_l, c_type_l_2)

# populate material cost
for types in ["X", "L"]:
    car_type_x_y = types
    avg_for_vehicle_type = options_averages_dict[car_type_x_y]
    mat_price_query = ''' UPDATE joined_data SET Material_Cost= @avg_for_vehicle_type WHERE Car_Type= @car_type_x_y AND Material_Cost= 0 AND Sales_Price > 1 '''
    database_handeler(mat_price_query, avg_for_vehicle_type, car_type_x_y)
    