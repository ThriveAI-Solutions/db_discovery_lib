import pandas as pd 
import db_discovery_lib.postgres_utils as pg
from importlib.resources import files

def read_files_to_df(DB_NAME, USER, PASSWORD, HOST, PORT):
    path_to_raw_data = files("db_discovery_lib.data")
    
    def read_titanic():
        df = pd.read_csv(path_to_raw_data / "titanic.csv")    # Read the CSV file into a pandas DataFrame
        df.columns = df.columns.str.replace(' ', '_').str.lower()
        df_titanic = df  
        df_titanic["pclass"] = df_titanic["pclass"].fillna("Unknown pclass")
        df_titanic["cabin"] = df_titanic["cabin"].fillna("Unknown cabin")
        df_titanic["sex"] = df_titanic["sex"].fillna("Unknown sex") 
        return df_titanic
        
    def read_penguin():
        df = pd.read_csv(path_to_raw_data / "penguins.csv")    # Read the CSV file into a pandas DataFrame
        df.columns = df.columns.str.replace(' ', '_').str.lower()  
        df.columns = df.columns.str.replace('(', '_')
        df.columns = df.columns.str.replace(')', '_')   
        df.columns = df.columns.str.replace('/', '_')      
        df_penguins = df 
        return df_penguins    
            
    def read_heart():
        df = pd.read_csv(path_to_raw_data / "heart.csv")    # Read the CSV file into a pandas DataFrame
        df.columns = df.columns.str.replace(' ', '_').str.lower()
        df_heart = df    
        df_heart["patient_id"] = df_heart.index
        return df_heart 
        
    def read_diabetes():
        df = pd.read_csv(path_to_raw_data / "diabetes.csv")    # Read the CSV file into a pandas DataFrame
        df.columns = df.columns.str.replace(' ', '_').str.lower()
        df_diabetes = df 
        df_diabetes["patient_id"] = df_diabetes.index
        return df_diabetes
            
    def read_healthcare():
        df = pd.read_csv(path_to_raw_data / "healthcare_dataset.csv")    # Read the CSV file into a pandas DataFrame
        df.columns = df.columns.str.replace(' ', '_')
        return df

    def read_chronic_disease():
        df = pd.read_csv(path_to_raw_data / "chronic_disease_indicators.csv")    # Read the CSV file into a pandas DataFrame
        df.columns = df.columns.str.replace(' ', '_')
        return df  
        
    def read_hel():
        df = pd.read_excel(path_to_raw_data / "HeWNY Sample File.xlsx")    # Read the CSV file into a pandas DataFrame
        df.columns = df.columns.str.replace(' ', '_')  
        df = df.rename(columns={'BCS': 'Breast_Cancer_Screening'})
        df = df.rename(columns={'CCS': 'Cervical_Cancer_Screening'})
        df = df.rename(columns={'COL': 'Colorectal_Cancer_Screening'})
        return df

    df_titanic = read_titanic()
    print(f'The data contains {df_titanic.shape[0]} rows and {df_titanic.shape[1]} columns of titanic data' )
    df_penguin = read_penguin()
    print(f'The data contains {df_penguin.shape[0]} rows and {df_penguin.shape[1]} columns of penguin data')
    df_health_care = read_healthcare()
    print(f'The data contains {df_health_care.shape[0]} rows and {df_health_care.shape[1]} columns of health_care data')
    df_diabetes = read_diabetes()
    print(f'The data contains {df_diabetes.shape[0]} rows and {df_diabetes.shape[1]} columns of diabetes data')
    df_heart = read_heart()
    print(f'The data contains {df_heart.shape[0]} rows and {df_heart.shape[1]} columns of heart data')
    df_chronic_disease = read_chronic_disease()
    print(f'The data contains {df_chronic_disease.shape[0]} rows and {df_chronic_disease.shape[1]} columns of chronic_disease data')
    df_hel = read_hel()
    print(f'The data contains {df_hel.shape[0]} rows and {df_hel.shape[1]} columns of WNY Healthelink - wny_health data')

    df_pretend_employee = pg.create_pretend_employee_df()
    print(f'The data contains {df_pretend_employee.shape[0]} rows and {df_pretend_employee.shape[1]} columns of pretend employee data')

    connection = pg.connect_to_postgresql(DB_NAME, USER, PASSWORD, HOST, PORT) 

    pg.drop_table(connection, "titanic") 
    pg.drop_table(connection, "wny_health") 
    pg.drop_table(connection, "penguin") 
    pg.drop_table(connection, "heart") 
    pg.drop_table(connection, "diabetes") 
    pg.drop_table(connection, "chronic_disease") 
    pg.drop_table(connection, "pretend_employees") 
    pg.drop_table(connection, "health_care") 

    def load_dataframes_to_sql():
        pg.create_table_from_dataframe(df_titanic, "titanic", DB_NAME, USER, PASSWORD, HOST, PORT) 
        pg.create_table_from_dataframe(df_penguin, "penguin", DB_NAME, USER, PASSWORD, HOST, PORT) 
        pg.create_table_from_dataframe(df_heart, "heart", DB_NAME, USER, PASSWORD, HOST, PORT)  
        pg.create_table_from_dataframe(df_diabetes, "diabetes", DB_NAME, USER, PASSWORD, HOST, PORT)       
        pg.create_table_from_dataframe(df_health_care, "health_care", DB_NAME, USER, PASSWORD, HOST, PORT)     
        pg.create_table_from_dataframe(df_chronic_disease, "chronic_disease", DB_NAME, USER, PASSWORD, HOST, PORT)    
        pg.create_table_from_dataframe(df_hel, "wny_health", DB_NAME, USER, PASSWORD, HOST, PORT)    
        pg.create_table_from_dataframe(df_pretend_employee, "pretend_employees", DB_NAME, USER, PASSWORD, HOST, PORT)      

    load_dataframes_to_sql()

if __name__ == "__main__":
    read_files_to_df("postgres", "postgres", "postgres", "localhost", "5469")