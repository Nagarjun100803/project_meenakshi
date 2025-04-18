"""
    Author: Nagarjun R
    Date: Thu Oct 24, 10:10 am
"""

#importing necessary libraries

import time
from typing import Literal
import numpy as np
import streamlit as st 
import pandas as pd
from PIL import Image
from main import (
    get_all_items, get_inventory, 
    # get_all_contribution, 
    get_particular_contribution, 
    add_new_items,
    get_allocations, 
    create_new_bill_record, is_bill_exists, 
    settings
)


inventory_data = get_inventory()
# contributions = get_all_contribution()
items = get_all_items()

allocations = get_allocations()

image = Image.open('./meenakshi_thirukalyanam.jpeg')
def main() -> None:

    st.set_page_config(
        page_title = 'Meenakshi Donation Record',
        layout = 'centered'
    )

    st.title('Meenakshi Thirukalyanam')

    with st.sidebar:
        st.title("Shivaya Namah")
        st.image(
            image, caption = 'Thirukalyanam'
        )

    (
        items_tab, 
        inventory_tab, 
        # transaction_tab, 
        particular_contribution_tab,
        add_item_tab,
        allocations_tab,
        bill_entry
        
    ) = st.tabs(
            [
                'Items', 'Inventory', 
                # 'Transactions', 
                'Search', 'Add Item',
                'Allocations',
                "Bill Entry"
            ]
    )

    with items_tab:

        st.text('This are the list of Items')

        st.dataframe(
            data = items.fillna(' ').set_index('item_id'),
            use_container_width = True
        )
    
    with inventory_tab:

        st.text('These are inventory data')
        inventory_df = inventory_data.set_index("item_id")
        st.dataframe(
            data = inventory_df.fillna(' '),
            use_container_width = True
        )

    # with transaction_tab:

    #     st.text('Transaction data')

    #     st.dataframe(
    #         data = contributions.fillna(' '),
    #         use_container_width = True
    #     )

    with particular_contribution_tab:
        
        col1, col2 = st.columns(2)
        bill_book_code = col1.selectbox('Bill Book Code', options = [f"B{x}" for x in np.arange(1, 101)])
        bill_id = col2.number_input("Bill Id", min_value = 1, step = 1)

        donar_name, contribution = get_particular_contribution(
            bill_book_code =  bill_book_code,
            bill_id = bill_id
        )

        if contribution is not None :
            st.text(f'The followings are contributed by {donar_name}')
            st.dataframe(
                data = contribution,
                use_container_width = True,
                hide_index = True
            )
            # st.table(data = contribution.set_index('item'))

        else:
            # st.text(contribution)
            st.error('No records found with this Bill')
            

    with add_item_tab:

        @st.dialog("Admin Credentials")
        def add_new_item_with_cred(
                item_name: str, 
                unit_of_measurement: Literal["Kg", "L", "Nos"]
        ):
            with st.form("Form", border = False):
                username: str = st.text_input("Admin Username")
                password: str = st.text_input("Admin Password", type = "password")

                if st.form_submit_button("Continue and Add Item"):
                    if username and password:
                        if username == settings.admin_username and password == settings.admin_password:
                            result = add_new_items(item_name = item_name, unit_of_measurement = unit_of_measurement)
                            if not result:
                                st.error(f"{item_name} already exists")
                            else:
                                st.info("Item inserted successfully", icon = "😍")
                                time.sleep(0.25)
                                st.rerun()
                                
                        else:
                            st.error("Invalid Credentials, Cannot add a new item.")

        st.text("Add a new item")
        
        with st.form('item', border = True, clear_on_submit = False):
            col1, col2 = st.columns(2)
            item_name: str = col1.text_input("Item name")
            unit_of_measurement: str = col2.selectbox(
                "Unit of Measurement", 
                options = ['Kg', 'L', 'Nos']
            )
            button: bool = st.form_submit_button('Submit', use_container_width = True)
        
        if button:
            
            if not all([item_name, unit_of_measurement]):
                st.toast("Item name should not be empty", icon = "😢")
                st.error("All fields required")
            
            else:
            
                add_new_item_with_cred(item_name = item_name, unit_of_measurement = unit_of_measurement)

                
    
    with allocations_tab:
        
        st.markdown("<b><p style='text-align: center;'>Allocations details</p></b>", unsafe_allow_html = True) 
        col1, col2, col3 = st.columns(3)
                
        options: list[str] = ['ALL'] + list(allocations['supervisor_name'].unique())
        supervisor_name: str = col3.selectbox('Supervisor', options)

        data: pd.DataFrame = allocations if supervisor_name == 'ALL' else allocations[allocations['supervisor_name'] == supervisor_name].reset_index(drop = True)
        grouped_data = data.groupby('item_id')['quantity'].sum().reset_index()
        final_grouped_data = pd.merge(items, grouped_data, how = 'inner', on = 'item_id')
        final_grouped_data = final_grouped_data[['item', 'quantity', 'unit_of_measurement']].sort_values('quantity', ascending=False).reset_index(drop = True)

        # remove the unnecessary columns
        data.drop(['cooking_team_id', 'supervisor_name', 'item_id'], axis = 1, inplace = True)
        
        col3, col4 = st.columns(2, vertical_alignment = 'bottom')
        col3.write('**Total Allocation:**')
        with col4.popover('graph'):
            st.bar_chart(
                final_grouped_data, x = 'item', y = 'quantity',
                horizontal = True, width = 400, height = 250  
            )
        st.dataframe(
            final_grouped_data, use_container_width = True,
            hide_index = True
        )      
        st.divider()

        st.write('**Individual Allocations:**')
        st.dataframe(
            data, use_container_width = True,
            hide_index = True
        )

    with bill_entry:
        st.markdown("<p style='text-align: center;'><b>Record Contribution</b></p>", unsafe_allow_html = True)

        with st.form("bill", clear_on_submit = False):
            col1, col2 = st.columns(2, vertical_alignment = "bottom", gap = "medium")

            bill_book_code = col1.selectbox("Bill Book Code", options = [f"B{x}" for x in np.arange(1, 101)])
            bill_id = col2.number_input("Bill Id", step = 1, min_value = 1)
            donar_name = col1.text_input("Contributor Name")
            donar_phone_num = col2.text_input("Contributor Number")

            items_df =  get_all_items()
            items_options = items_df["item_id"].astype(str) + " - " +  items_df["item"] + " - " + items_df["unit_of_measurement"] 
            
            st.markdown("<p style='text-align: center;'><b>Choose Items </b></p>", unsafe_allow_html = True)
            df = pd.DataFrame(columns = ["items", "quantity"])
            
            contribution_df = st.data_editor(
                df, num_rows = "dynamic", 
                use_container_width = True,
                column_config = {
                    "items": st.column_config.SelectboxColumn(
                        "Choose Item", help = "Name of an item",
                        width = "medium", 
                        options = items_options, 
                        required = True
                    ), 
                    "quantity": st.column_config.NumberColumn(
                        "Quantity (in Numbers/Decimal values)", 
                        help = "The quantity that we received, like 1, 2, 2.4, 8.6.",
                        min_value = 0.1,
                        required = True
                    )
                }

            )


            if st.form_submit_button("Submit", use_container_width = True):
                # st.table(contribution_df)
                if contribution_df.empty:
                    st.error("No items have chosen.")
                    st.stop()

                bill_exists: bool = is_bill_exists(bill_book_code, bill_id)

                @st.dialog("Bill Exists")
                def ask_update_or_cancel(
                    bill_book_code: str, bill_id: int,
                    donar_name: str,
                    donar_phone_num: str,
                    contribution_df: pd.DataFrame
                ):
                    st.markdown("<h2 style='text-align:center; color:red;'>Warning</h2>", unsafe_allow_html = True)
                    st.text(f"The bill book code {bill_book_code}, bill id {bill_id} already exists")
                    col1, col2 = st.columns(2)
                    
                    if col1.button("Cancel", use_container_width = True):
                        st.info("Transaction Cancelled")
                        time.sleep(0.5)
                        st.rerun()

                    if col2.button("Update", use_container_width = True):
                        create_new_bill_record(
                            bill_book_code, bill_id, donar_name,
                            donar_phone_num, 
                            contribution_df
                        )
                        st.info("Transaction Success")
                        time.sleep(0.5)
                        st.rerun()
                    

                if bill_exists:
                    ask_update_or_cancel(bill_book_code, bill_id, donar_name,
                        donar_phone_num, 
                        contribution_df)
                else:
                    create_new_bill_record(
                        bill_book_code, bill_id, donar_name,
                        donar_phone_num, 
                        contribution_df
                    )
                    st.toast("Success")
                    time.sleep(0.5)
                    st.rerun()

                
                
                

        



if __name__ == '__main__':
    main()



