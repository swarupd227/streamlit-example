import snowflake
from snowflake.snowpark.session import Session
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import avg, sum, col,lit
import streamlit as st
from st_aggrid import AgGrid, JsCode,GridOptionsBuilder,DataReturnMode,AgGridTheme
import pandas as pd
from snowflake.snowpark.context import get_active_session
import snowflake.connector
from datetime import datetime
import streamlit_authenticator as stauth

import yaml
from yaml.loader import SafeLoader

with open('./pass.yaml') as file:
    config = yaml.load(file, Loader=SafeLoader)

authenticator = stauth.Authenticate(
    config['credentials'],
    config['cookie']['name'],
    config['cookie']['key'],
    config['cookie']['expiry_days'],
    )


name, authentication_status, username = authenticator.login("Login", "main")

if authentication_status == False:
    st.error("Username/password is incorrect")

if authentication_status == None:
    st.warning("Please enter your username and password")

if authentication_status: 
    user_email = authenticator.credentials['usernames'][username]['email'] 
    print("################################",user_email)
    @st.cache_resource
    def create_session():
        return Session.builder.configs(st.secrets.snowflake).create()

    session = create_session()
    st.success("Connected to Snowflake!")


    @st.cache_data
    def load_data(table_name):
        st.write(f"Here's some example data from `{table_name}`:")
        table = session.table(table_name)    
        table = table.limit(100)
        table = table.collect()
        return table
    table_name = "MARVEL_TEST.PUBLIC.MFG_MARVEL_DEMO"

    def create_grid(df):

        gb = GridOptionsBuilder.from_dataframe(df)
        gb.configure_default_column(editable=True, filter=True, resizable=True, sortable=True, value=True, enableRowGroup=True,
                                    enablePivot=True, enableValue=True, floatingFilter=True, aggFunc='sum', flex=1, minWidth=150, width=150, maxWidth=200)
        gb.configure_selection(selection_mode='multiple', use_checkbox=True)
        gridOptions = gb.build()
        grid = AgGrid(
            df,
            gridOptions=gridOptions,
            data_return_mode=DataReturnMode.AS_INPUT,
            update_on='MANUAL',
            fit_columns_on_grid_load=True,
            theme=AgGridTheme.STREAMLIT,  
            enable_enterprise_modules=True,
            height=600,
            width='100%',
        )

        return grid

    def delete_row(df, grid):
        selected_rows = grid['selected_rows']
        selectedRecord = selected_rows[0]
        selectedOrderID = selectedRecord.get('ORDERID')
        session.sql(f'''DELETE  from MFG_MARVEL_DEMO WHERE ORDERID = {selectedOrderID}''').collect()
        if selected_rows:            
            selected_indices = [i['_selectedRowNodeInfo']
                                ['nodeRowIndex'] for i in selected_rows]
            df_indices = st.session_state.df_for_grid.index[selected_indices]
            print("####################",selected_indices)
            print("####################",df_indices )
            
            df = df.drop(df_indices)
            
        return df

    def update_row(df,grid):
        selected_rows = grid['selected_rows']
        selectedRecord = selected_rows[0]
        selectedOrderID = selectedRecord.get('ORDERID')
        updatedCustomer = selectedRecord.get('CUSTOMER')
        updatedWeight = selectedRecord.get('WEIGHT')
        updatedRate = selectedRecord.get('RATE')
        updatedCreatedDt = selectedRecord.get('CREATEDDT')
        updatedModifiedDt = selectedRecord.get('MODIFIEDDT')
        updatedCreatedBy = selectedRecord.get('CREATEDBY')
        updatedLastModifiedBy = selectedRecord.get('LASTMODIFIEDBY')
        
        
        session.sql(f'''UPDATE MFG_MARVEL_DEMO SET CUSTOMER = '{updatedCustomer}',WEIGHT= '{updatedWeight}',RATE='{updatedRate}',CREATEDDT='{updatedCreatedDt}',MODIFIEDDT='{updatedModifiedDt}',CREATEDBY='{updatedCreatedBy}',LASTMODIFIEDBY='{updatedCreatedBy}' WHERE ORDERID = {selectedOrderID}''').collect()
        return df

    def loadInferAndPersist(file) -> snowpark.DataFrame:
        file_df = pd.read_excel(file, parse_dates=['CREATEDDT'])
        file_df['CREATEDDT']=file_df['CREATEDDT'].dt.tz_localize(None)
        session.sql("USE SCHEMA MFG_MARVEL_DEMO")
        snowparkDf=session.write_pandas(df=file_df,schema="PUBLIC",table_name="MFG_MARVEL_DEMO",overwrite=True) 
        return snowparkDf

        
    def main():
        
        options = st.sidebar.selectbox("Select Operations",("Create","Read","Upload"))
        authenticator.logout('Logout', 'main', key='unique_key')

        st.title("CRUD Application for Table")
        today = datetime.today()
        date_in_str=today.strftime("%Y%m%d_%H%M%S")

        if options == "Create":
            st.subheader("Create a Record")
            with st.form("data_editor_form"):            
                st.caption("Create a new Record")
                orderid = st.number_input("ID")
                weight_value=st.number_input("WEIGHT")
                rate_value = st.number_input("RATE")
                submit_button = st.form_submit_button("Create")
            if submit_button:            
                try:
                    print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^",user_email)
                    print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^",orderid)
                    print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^",weight_value)
                    print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^",rate_value)  
                    todayDate=session.sql(f'''SELECT CONVERT(varchar, getdate(), 23)''').collect()
                    print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^",todayDate)                 
                    session.sql(f"""INSERT INTO MFG_MARVEL_DEMO (orderid, customer,weight,rate, createddt, modifieddt,createdby,lastmodifiedby) 
        VALUES ('{orderid}', '{name}','{weight_value}','{rate_value}', '{todayDate}','{date_in_str}','{user_email}','{user_email}')""").collect()
                    st.success('Success!', icon="✅")
                    df = load_data(table_name)
                    df=pd.DataFrame(df)
                    AgGrid(df)
                except:
                    st.warning("Error updating table")
    
                    

        elif options == "Read":
            df =session.sql(f'''SELECT * FROM MFG_MARVEL_DEMO ''').collect()
            delete_row_button = st.button("Delete")
            update_record_button = st.button("Update")
            df = pd.DataFrame(df)
            grid = create_grid(df)
            if delete_row_button:            
                df = delete_row(df,grid)
            if update_record_button:
                df = update_row(df,grid)

        elif options == "Upload":
            file = st.file_uploader("Drop your CSV here",type=["xlsx"])
            if file is not None:
                df= loadInferAndPersist(file)
                st.success("Great, your data has been uploaded to Snowflake!")
                st.dataframe(df)
            
            

        
            
            

    if __name__ == "__main__":
        main()
