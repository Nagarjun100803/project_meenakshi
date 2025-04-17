"""
    Author: Nagarjun R
    Date: Mon Oct 21, 9:10 am
"""

from decimal import Decimal
from typing import Literal
import psycopg2 as pg 
from psycopg2 import pool
import pandas as pd 
from psycopg2.extras import execute_values
from psycopg2.errors import UniqueViolation
from psycopg2.extras import RealDictCursor


#Database Setup

#connection parameters
connection_params: dict[str, str] = {

        'host': 'localhost',
        'database': 'meenakshi',
        'password': 'arju@123',
        'user': 'postgres',
        "cursor_factory": RealDictCursor
    }

db: pool.SimpleConnectionPool = pool.SimpleConnectionPool(
    minconn = 4, maxconn = 12,
    **connection_params
)


def execute_sql_select_query(
        sql_statement: str, 
        vars: tuple | dict | None = None
) -> (pd.DataFrame | None):

    "Return the select sql statement as pandas dataframe object"

    conn: pg.extensions.connection = db.getconn()
    cur: pg.extensions.cursor = conn.cursor()

    # executing the sql statement
    cur.execute(sql_statement, vars = vars) # stores the value in cursor object

    db.putconn(conn) # releasing the connection back to the pool.

    #fetching the data from the cursor
    data: RealDictCursor | None = cur.fetchall()

    if not data:
        # if the result is empty list then we return None
        return None
    
    df = pd.DataFrame(data)
    df = df.map(lambda x: float(x) if isinstance(x, Decimal) else x)

    #closing the cursor object
    cur.close()
    
    return df

def execute_sql_statements(
        sql: str, vars: tuple | dict | None = None, 
        # fetch: Literal["all", "one", "no_fetch"] = "no_fetch"
) -> list[tuple] : 

    """
        This function is used to execute the sql statements specially Insert clause except Select clause

        Args
        -----
            sql: str 
                sql statement.
            vars: tuple 
                A parameter which need to insert.
    """

    # creating a new connection.
    conn: pg.extensions.connection = db.getconn()
    # creating a cursor object to execute sql statement.
    cur: pg.extensions.cursor = conn.cursor()

    # executing the statement.
    cur.execute(sql, vars = vars)

    #save the changes in the db.
    conn.commit()
    db.putconn(conn) # releasing the connection back to the pool.

    result: list[RealDictCursor] = cur.fetchall() # fetching results from a cur obj.
    cur.close()
    
    return result
  
  
def get_all_items() -> (pd.DataFrame | None) : 

    "This function returns all the items list"

    sql: str = """
        select 
            i.id as item_id,
            initcap(i.name) as item,
            i.unit_of_measurement
        from
            items as i
        order by 
            i.id
    """    
    items_df = execute_sql_select_query(sql)
    
    return items_df


def get_all_contribution() -> (pd.DataFrame | None):

    "This returns the contribution of all donar"

    sql: str = """

        select 
            b.*,
            i.name as item,
            t.quantity,
            i.unit_of_measurement,
            t.donated_at::date
        from 
            transactions as t 
        join 
            items as i 
        on 
            i.id = t.item_id
        join 
            bill_books as b
        on 
            (b.bill_book_code = t.bill_book_code and b.bill_id = t.bill_id)
        ;

    """

    contribution_df = execute_sql_select_query(sql)
    
    return contribution_df


 
def get_particular_contribution(
        bill_book_code: str, 
        bill_id: int
) -> (tuple[str, pd.DataFrame] | tuple[None, None]): 
    
    """
    This function returns the items contributed in a particular bill.

    Parameter
    ----------
    bill_book_code: str 
        Unique code represents particular bill_book eg.B1, B2 etc.   
    bill_id: int 
        Unique id/page represents the particular bill entered in the bill book.
        
    Returns
    --------
        tuple[str, pd.DataFrame] 
            which is the donar name and the contribution if bill found with bill_id and bill_book_code.
        tuple[None, None] 
            if no bill is found with bill_id and bill_book_code.

    """

    sql: str = """

        select
            initcap(i.name) as item,
            x.quantity,
            i.unit_of_measurement,
            x.donated_at::date as donated_on, 
            to_char(x.donated_at, 'HH12 : MI : SS AM') as donated_at,
            b.donar_name
        from(
            select 
                *
            from 
                transactions as t 
            where 
                t.bill_book_code = %(bill_book_code)s and t.bill_id = %(bill_id)s
        )x
        join 
            bill_books as b
        on 
            b.bill_book_code = x.bill_book_code and b.bill_id = x.bill_id
        join 
            items as i 
        on 
            i.id = x.item_id
        ;
    """

    contribution_df = execute_sql_select_query(sql, vars = {
        'bill_book_code': bill_book_code, 
        'bill_id': bill_id
    })
    
    if contribution_df is None:
        print("The contribution df is ", contribution_df)
        return (None, None)
    
    donar_column: str = 'donar_name'
    donar_name: str = contribution_df[donar_column].iloc[0]

    final_df = contribution_df.drop(donar_column, axis=1)

    return (donar_name, final_df)




def add_new_items(
        item_name: str, 
        unit_of_measurement: Literal['Kg', 'L', 'Nos']
) -> tuple | None:

    """
        This function is used to add a new item in the items table
    """

    if not item_name:
        print('Item name must include')
        return None
    
    # Check the item is already exists in our database.

    existance_check_sql: str = """
        select 
            1
        from 
            items as i 
        where 
            i.name like lower(%(item_name)s)
        ;
    """

    previous_record = execute_sql_select_query(existance_check_sql, vars = {"item_name": item_name})

    if previous_record is not None:
        print(previous_record)
        return None
    
    item_name = item_name.strip().lower() # standardizing the item names for backend convenience.
    
    sql: str = """
        
        insert into items
            (name, unit_of_measurement)
        values
            (%(item_name)s, %(unit_of_measurement)s)
        returning *
        ;

    """
    result: tuple = execute_sql_statements(
        sql, 
        vars = {
            'item_name': item_name,
            'unit_of_measurement': unit_of_measurement
        }
    )

    print(f"{item_name} added successfully.")
    
    return result 

def add_new_cooking_team(
        supervisor_name: str, 
        supervisor_phone_num: str | None = None,         
) -> (tuple | None):
    # need to fix the empty strings ' '
    if not supervisor_name:
        print('Supervisor name must include')
        return None

    sql: str = """
        
        insert into cooking_teams
            (supervisor_name, supervisor_phone_num)
        values
            (%(supervisor_name)s, %(supervisor_phone_num)s)
        returning *
        ;
    """
    result = None 
    try:
        result = execute_sql_statements(
            sql, 
            vars = {
                "supervisor_name": supervisor_name,
                "supervisor_phone_num": supervisor_phone_num
            }
        )
    except UniqueViolation:
        print("Error occuring in inserting the data: Cooking team already found with this supervisor name")
        return result
    
    return result

def get_inventory() -> (pd.DataFrame | None) :

    sql : str = """

        with cte1 as (
            select 
                t.item_id,
                sum(quantity) as total_quantity
            from 
                transactions as t
            group by 
                t.item_id
        ), 
        cte2 as (
            select 
                a.item_id,
                sum(quantity) as total_quantity
            from 
                allocations as a
            group by 
                a.item_id
        )

        select 
            cte1.item_id,
            initcap(i.name) as item,
            (cte1.total_quantity - coalesce(cte2.total_quantity, 0)) as available_quantity, 
            i.unit_of_measurement
            -- cte1.total_quantity as total_quantity_taken,
            -- cte2.total_quantity as total_quantity_given
        from 
            cte1
        left join 
            cte2
        on 
            cte1.item_id = cte2.item_id
        left join
            items as i
        on 
            i.id = cte1.item_id
        order by 
            cte1.item_id
        ;

    """

    inventory_df = execute_sql_select_query(sql)

    return inventory_df


def check_for_item_availability(
        item_ids: list[int],
        quantites: list[int | float]
) -> (pd.DataFrame):
    
    """
        Returns the items that are requested greater than available quantity.

        Parameters
        ---------
        item_ids: list[int]
            List of item ids requesting to allocate.
        quantities: list[int | float]
            List of quantities that maps the item.
        
        Returns
        -------
        availability_df: pd.DataFrame
            items which are requested greater than available quantity.
    """
    
    inventory_df: pd.DataFrame = get_inventory() # get the inventory data for all items.
    required_columns: list[str] = ['item_id', 'item', 'unit_of_measurement', 'available_quantity']
    required_items_df: pd.DataFrame = inventory_df[required_columns]

    requested_items_df: pd.DataFrame = pd.DataFrame({'item_id': item_ids, 'requested_quantity': quantites}) # create temporary dataframe with input item_ids and quantites for further operations(join).

    final_df: pd.DataFrame = pd.merge(required_items_df, requested_items_df, how ='inner', on ='item_id') # join(inner) the two dataframe to get necessary details of required items.

    avaialability_df: pd.DataFrame = final_df[final_df['available_quantity'] < final_df['requested_quantity']] # filter the items that are requested greater than the available quantity.

    return avaialability_df
    

def allocate_items_to_cooking_team(
    cooking_team_id: int,
    item_ids: list[int],
    quantities: list[int | float]
) -> (list[tuple] | None):

    if len(item_ids) != len(quantities):
        print("Size of Item and Quantity must be same.")
        return None
    
    # check whether the cooking team exists or not.
    sql: str = """
        select 
            *
        from 
            cooking_teams
        where 
            cooking_teams.id = %(cooking_team_id)s
        ;
    """     
    cooking_team: list[tuple] | None = execute_sql_select_query(sql, vars = {'cooking_team_id': cooking_team_id})

    if cooking_team is None:
        # if no cooking team is found with the id, we stop the operation and return None.
        print(f"No cooking team found with this id : {cooking_team_id}.\nPlease create the team to allocate items.")
        return None
    
    # check for the availability of each items to allocate.
    availability_df: pd.DataFrame = check_for_item_availability(item_ids, quantities)

    if not availability_df.empty:
        # If any item requested greater than available quantity, we are not perform this operation further, So we return None.
        # Items requested the quantity greater than available quantity.
        for _, row in availability_df.iterrows():
            row: tuple = tuple(row)
            item_id, item, unit, available_quantity, requested_quantity = row 
            
            print(f'You are requesting the item {item} of item_id {item_id} for {requested_quantity} {unit} but the available quantity is {available_quantity} {unit}.')
        print('\nNote: Please enter the items within the available quantity.')

        return None
    
    # If we have all items, Then allocate that to cooking team.
    conn: pg.extensions.connection = db.getconn() # get a db connection. 
    cur: pg.extensions.cursor = conn.cursor() # create a cursor object to interact with database.

    cooking_team_ids: list[int] = [cooking_team_id] * len(item_ids) # make the cooking_team_id into list of ids to insert into table. 
    
    sql: str = """
        insert into allocations
            (cooking_team_id, item_id, quantity)
        values
            %s
        returning *
    """
    values: list[tuple] = [
            (team_id, item_id, quantity) \
        for team_id, item_id, quantity in zip(
            cooking_team_ids, item_ids, quantities, strict = True
        )
    ]

    try:
        execute_values(cur, sql, argslist = values) # insert the records
        conn.commit() # save the data into the database.
        result: list[tuple] = cur.fetchall() # fetch the inserted record.
        return result
    
    except Exception as e:
        print(f'Error occur during inserting the record: {str(e)}')
        return None
    
    finally:
        cur.close() # close the cursor object.
        db.putconn(conn) # release the back to the pool. 

def get_allocations() -> pd.DataFrame:

    sql: str = """
        select 
            a.cooking_team_id,
            upper(c.supervisor_name) as supervisor_name,
            --c.supervisor_phone_num,
            initcap(i.name) as item,
            a.quantity,
            i.id as item_id,
            i.unit_of_measurement,
            a.allocated_at::date as alloacted_on,
            to_char(a.allocated_at, 'HH12 : MI : SS AM') as allocated_at
        from 
            allocations as a
        join 
            cooking_teams as c
        on 
            a.cooking_team_id = c.id
        join 
            items as i 
        on 
            a.item_id = i.id
        order by 
            a.allocated_at
        ;
    """
    alloactions = execute_sql_select_query(sql)

    return alloactions    


def is_bill_exists(
    bill_book_code: str, 
    bill_id: int, 
    cur: pg.extensions.cursor | None = None
) -> bool: 
    
    """
        Helper function used to check the bill is already created.
    """
    sql: str = """
        select 
            1 as bill_exist
        from 
            bill_books
        where 
            bill_book_code = %(bill_book_code)s and 
            bill_id = %(bill_id)s
        ;
    """
    
    if not cur: 
        conn: pg.extensions.connection = db.getconn()
        cur: pg.extensions.cursor = conn.cursor()
        cur.execute(sql, vars = {"bill_book_code": bill_book_code, "bill_id": bill_id})
        db.putconn(conn)
    else:
        cur.execute(sql, vars = {"bill_book_code": bill_book_code, "bill_id": bill_id})

    result = cur.fetchone()

    return bool(result)    


def create_new_bill_record(
    bill_book_code: str, 
    bill_id: int, 
    contributor_name: str, 
    contributor_phone_num: str, 
    contribution_df: pd.DataFrame
):

    conn: pg.extensions.connection = db.getconn()
    cur: pg.extensions.cursor = conn.cursor()

    try:

        previous_bill_record = is_bill_exists(bill_book_code, bill_id, cur)

        if not previous_bill_record:
            # Create the bill record. 
            create_bill_entry_sql: str = """
                insert into bill_books(
                    bill_book_code, bill_id, donar_name, donar_phone_num
                )
                values(
                    %(bill_book_code)s, %(bill_id)s, %(donar_name)s, %(donar_phone_num)s
                );
            """
            new_record = cur.execute(
                create_bill_entry_sql, {
                    "bill_book_code": bill_book_code, "bill_id": bill_id, 
                    "donar_name": contributor_name, "donar_phone_num": contributor_phone_num
                }
            )

        # # Create transactions.
        contribution_df["item_ids"] = contribution_df["items"].str.split("-").str[0].astype(int)
        contribution_df["bill_book_code"] = bill_book_code
        contribution_df["bill_id"] = bill_id
        contribution_df.drop("items", axis = 1, inplace = True)

        # print(contribution_df)
        # return contribution_df.to_dict("records")

        create_transactions_sql: str = """
            insert into 
                transactions(
                    bill_book_code, bill_id, item_id, quantity
                )
            values
                %s
            ;
        """
        execute_values(
            cur, create_transactions_sql, 
            argslist = contribution_df.to_dict("records"),
            template = "(%(bill_book_code)s, %(bill_id)s, %(item_ids)s, %(quantity)s)"
        )

        conn.commit()

        print("All records inserted")

    except Exception as e: 
        conn.rollback()
        print(str(e))
        raise e

    finally:
        cur.close()
        db.putconn(conn)





def main() -> None :
    pass


if __name__ == '__main__':
    "Checking if the main file is executed directly"
    main()
