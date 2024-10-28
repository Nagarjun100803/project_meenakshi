"""
    Author: Nagarjun R
    Date: Mon Oct 21, 9:10 am
"""

from typing import Literal
import psycopg2 as pg 
from psycopg2 import pool
import pandas as pd 
from psycopg2.extras import execute_values
from psycopg2.errors import UniqueViolation


#Database Setup

#connection parameters
connection_params: dict[str, str] = {

        'host': 'localhost',
        'database': 'meenakshi',
        'password': 'arju@123',
        'user': 'postgres'
    }

db: pool.SimpleConnectionPool = pool.SimpleConnectionPool(
    minconn = 4, maxconn = 12,
    **connection_params
)


def execute_sql_select_query(
        sql_statemt: str, 
        vars: tuple | dict | None = None
) -> (pd.DataFrame | None):

    "Return the select sql statement as pandas dataframe object"

    conn: pg.extensions.connection = db.getconn()
    cur: pg.extensions.cursor = conn.cursor()

    # executing the sql statement
    cur.execute(sql_statemt, vars = vars) # stores the value in cursor object

    db.putconn(conn) # releasing the connection back to the pool.

    #fetching the data from the cursor
    data: list[tuple] | None = cur.fetchall()

    if not data:
        # if the result is empty list then we return None
        return None
    
    #fetching all the columns from the cursor object 
    descriptions: list[pg.extensions.Column] = cur.description

    column_names: list[str] = [description.name for description in descriptions]

    #creating the final result as pandas DataFrame object
    df: pd.DataFrame = pd.DataFrame(
        data = data,
        columns = column_names
    )
    #closing the cursor object
    cur.close()
    
    return df

def execute_sql_statements(sql: str, vars: tuple | dict | None = None) -> list[tuple] : 

    """
        This function is used to execute the sql statements specially Insert clause except Select clause

        Args:
        ----
            sql: str is a sql statement.
            vars: tuple is a parameter which need to insert.
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

    result: list[tuple] = cur.fetchall() # fetching results from a cur obj.
    cur.close()
    
    return result
  
  
def get_all_items() -> pd.DataFrame | None : 

    "This function returns all the items list"

    sql: str = """
        select 
            i.id as item_id,
            i.name,
            i.unit_of_measurement
        from
            items as i
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
    This returns the contribution of a particular bill with a donar_name
    
    Returns
    -------
        Donar Name : str
        Contribuion: pd.DataFrame
    """

    sql: str = """

        select
            i.name as item,
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
        return (None, None)
    
    donar_column: str = 'donar_name'
    donar_name: str = contribution_df[donar_column].iloc[0]

    final_df = contribution_df.drop(donar_column, axis=1)

    return (donar_name, final_df)


def make_transactions(
        bill_book_code: str,
        bill_id: int,
        item_ids: list[int],
        quanties: list[int | float]
) -> (list[tuple] | None):
    
    """
        This function is used to insert all the transaction made in a single bill.

        Parameters
        -----------
        bill_book_code: str is the code of the bill book.   
        bill_id: int is the unique id/page of particular bill book.
        item_ids: list[int] is the list of integers that represent the list of item contributed.
        quantities: list[int | float] is the quantites of each items provided by the donar.
        
        Returns
        --------
            It returns list[tuple[Any]]
    """
    if len(item_ids) != len(quanties):
        # check the number of item id is equal to number of quantites that map each other.
        print('The item and quanitity size should match')
        return None
    
    # get a database connection from the pool
    conn: pg.extensions.connection = db.getconn()
    # cursor object to insert records
    cur: pg.extensions.cursor = conn.cursor()

    sql: str = """
        
        insert into transactions
            (bill_book_code, bill_id, item_id, quantity)
        ialues
            %s
        returning *
        ;

    """
    bill_book_code = [bill_book_code] * len(item_ids)
    bill_id = [bill_id] * len(item_ids)

    values: list[tuple] = [
        (bill_book_code, bill_id, item_id, quantity) \
            for bill_book_code, bill_id, item_id, quantity \
        in zip(
            bill_book_code, bill_id, item_ids, quanties, strict = True
        )
    ]
    try :
        # inserting all the transactions
        execute_values(
            cur,
            sql,
            argslist = values
        )

        conn.commit() # save the records into the database.

    except UniqueViolation:

        print("Error occur during inserting the record: The item is already exist in this bill.")
        
        return None
    

    result: list[tuple] = cur.fetchall() # fetching the inserted record.
    
    cur.close() # close the cursor.
    db.putconn(conn) # releasing the connection back to the pool.

    return result



def insert_bill_records(
        bill_book_code: str,
        bill_id: int,
        donar_name: str | None = None,
        donar_phone_num: str | None  = None,
) -> (tuple[str, int] | tuple[None, None]): 
    

    sql: str = """

        insert into bill_books 
            (bill_book_code, bill_id, donar_name, donar_phone_num)
        values 
            (%(bill_book_code)s, %(bill_id)s, %(donar_name)s, %(donar_phone_num)s)
        returning 
            bill_book_code, bill_id
        ;
    """
    vars: dict[str, str | int | None] = {
            'bill_book_code': bill_book_code,
            'bill_id': bill_id,
            'donar_name': donar_name,
            'donar_phone_num': donar_phone_num
        }

    result = (None, None)

    try :
        # inserting the record
        result = execute_sql_statements(sql, vars = vars)
        print("Data is inserted successfully.")

    except UniqueViolation as e:
        print(f"Error occur during inserting the record: The bill is already exist")

    return result


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

def get_inventory() -> pd.DataFrame :

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
            i.name as item,
            (cte1.total_quantity - coalesce(cte2.total_quantity, 0)) as available_quantity, 
            i.unit_of_measurement
            --cte1.total_quantity as total_quantity_taken,
            --cte2.total_quantity as total_quantity_given
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




def main() -> None :

    # result = get_inventory()
    # print('Available Products are : \n')
    # print(result)
    
    # item = add_new_items('godhumai', 'Kg')

    # print(item)
    # items = get_all_items()
    # print(items)
    pass


if __name__ == '__main__':
    "Checking if the main file is executed directly"
    main()
