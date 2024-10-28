"""
    Author: Nagarjun R
    Date: Thu Oct 24, 10:10 am
"""

#importing necessary libraries

import streamlit as st 
import pandas as pd
from main import (
    get_all_items, get_inventory, 
    # get_all_contribution, 
    get_particular_contribution, 
    add_new_items
)

unique_key: str = '123'

inventory_data = get_inventory()
# contributions = get_all_contribution()
items = get_all_items()



def main() -> None:

    st.set_page_config(
        page_title = 'Meenakshi Donation Record',
        layout = 'centered'
    )

    st.title('Thirukalyanam Donations')

    with st.sidebar:
        st.title("Shivaya namah")

    (
        items_tab, 
        inventory_tab, 
        # transaction_tab, 
        particular_contribution_tab,
        add_item_tab
        
    ) = st.tabs(
            [
                'Items', 'Inventory', 
                # 'Transactions', 
                'Search', 'Add Item'
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
        bill_book_code = col1.selectbox('Bill Book Code', options = ['B1', 'B2', 'B3'])
        bill_id = col2.number_input('Bill Id', min_value = 1, step = 1)

        donar_name, contribution = get_particular_contribution(
            bill_book_code =  bill_book_code,
            bill_id = bill_id
        )

        if (donar_name) and (contribution is not None) :
            st.text(f'The following are contributed by {donar_name}')
            st.dataframe(
                data = contribution.set_index('item'),
                use_container_width = True
            )
            # st.table(data = contribution.set_index('item'))

        else:
            st.error('No records found with this Bill')
            

    with add_item_tab:

        st.text("Add a new item")
        
        with st.form('item', border = True, clear_on_submit = True):
            col1, col2 = st.columns(2)
            item_name: str = col1.text_input('Enter the Item name')
            unit_of_measurement: str = col2.selectbox(
                'Enter unit of measurement', 
                options = ['Kg', 'L', 'Nos']
            )
            button: bool = st.form_submit_button('Submit', use_container_width = True)
        
        if button:
            
            if not all([item_name, unit_of_measurement]):
                st.toast("Item name should not be empty", icon = "😢")
                st.error("All fields required")
            
            else:
                # Need to add the logic to insert a new item
                result = add_new_items(
                    item_name = item_name, unit_of_measurement = unit_of_measurement
                )
                st.toast("Item inserted successfully", icon = "😍")

        

if __name__ == '__main__':
    main()



