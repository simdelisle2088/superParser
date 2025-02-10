from sqlalchemy.exc import IntegrityError, DBAPIError
import pandas as pd
import logging
import json
from apscheduler.schedulers.background import BackgroundScheduler
import pytz
import re
from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, Boolean, func, select
from sqlalchemy.ext.declarative import declarative_base

def keep_alive():
    logging.info("Keeping the application alive...")

# Create the scheduler
scheduler = BackgroundScheduler()
scheduler.add_job(keep_alive, 'interval', minutes=20)
scheduler.start()
Base = declarative_base()

# =========== Processor functions ==========================

def process_and_store_dataframe(df: pd.DataFrame, session_factory) -> None:
    try:
        logging.debug(f"Number of rows in initial DataFrame: {len(df)}")

        # Parse the data
        df = parse_data(df)

        # Check for duplicates in the DataFrame
        duplicate_orders = df[df.duplicated(subset='order_number', keep=False)]
        if not duplicate_orders.empty:
            logging.warning(f"Found duplicate order numbers in DataFrame: {duplicate_orders['order_number'].tolist()}")

        # Process and categorize the order
        if len(df) == 0:
            logging.info("No orders to process after parsing. Exiting function.")
            return

        logging.debug(f"Number of rows in processed DataFrame: {len(df)}")

        # Insert each order into the database
        insert_orders(df, session_factory)
    except Exception as e:
        logging.error(f"Unexpected error occurred during process_and_store_dataframe: {e}")

def parse_data(df: pd.DataFrame) -> pd.DataFrame:
    df["state"] = df.apply(
        lambda x: f"missing {x['Qty']}" if x["QtyOnHand"] < 0 else False, axis=1
    )
    df["order_info"] = df.apply(
        lambda row: {
            "item": row["LineCode"] + " " + row["ItemNumber"],
            "description": row["Description"],
            "units": row["Qty"],
            "state": row["state"],
            "upc": row["upc"],
        },
        axis=1,
    )

    grouped_df = (
        df.groupby("DocNo")
        .agg(
            {
                "Customer": "first",
                "CrName": "first",
                "PhoneNumber": "first",
                "Store": "first",
                "Address1": "first",
                "Address2": "first",
                "Address3": "first",
                "ShipAddr1": "first",
                "ShipAddr2": "first",
                "ShipAddr3": "first",
                "Clerk": "first",
                "TotalAtCost": "first",
                "JobNumber": "first",
                "order_info": lambda x: list(x),
            }
        )
        .rename(
            columns={
                "Customer": "customer",
                "CrName": "client_name",
                "PhoneNumber": "phone_number",
                "Store": "store",
                "Address1": "address1",
                "Address2": "address2",
                "Address3": "address3",
                "ShipAddr1": "ship_addr1",
                "ShipAddr2": "ship_addr2",
                "ShipAddr3": "ship_addr3",
                "Clerk": "clerk_name",
                "TotalAtCost": "price",
                "JobNumber": "job",
            }
        )
        .reset_index()
        .rename(columns={"DocNo": "order_number"})
    )

    logging.debug(f"Parsed DataFrame:\n{grouped_df}")

    return grouped_df

def find_location(session, upc, item, store):
    # Query to find all locations based on UPC from InvLocations where is_archived is False
    query = select(InvLocations).where(InvLocations.upc.in_(upc), InvLocations.store == store, InvLocations.is_archived == False)
    loc_info_list = session.execute(query).fetchall()

    # Use a set to avoid duplicate locations
    unique_locations = set()

    # Add locations from InvLocations, only if they are not archived
    if loc_info_list:
        # loc_info_list contains tuples, extract the first element (the object)
        unique_locations.update([loc[0].full_location for loc in loc_info_list if loc[0].full_location])
        logging.info(f"Found {len(loc_info_list)} non-archived locations in InvLocations: {[loc[0].full_location for loc in loc_info_list]}")
        
        # If we found locations in InvLocations, return them and skip PalletLocations
        return list(unique_locations)

    # If no locations found in InvLocations, check PalletLocations
    loc_pallet_info_list = session.query(PalletLocations).filter(
        func.replace(PalletLocations.item, '-', '') == item, 
        PalletLocations.store == store, 
    ).all()

    if loc_pallet_info_list:
        for loc_pallet_info in loc_pallet_info_list:
            if loc_pallet_info.loc == "0":
                loc_pallet_info.loc = "1"
            # Format and add location string
            pallet_location = f'0{int(loc_pallet_info.store)}{int(loc_pallet_info.loc)}0000000'
            unique_locations.add(pallet_location)
            logging.info(f"Returning location from PalletLocations: {pallet_location}")

    # If no locations were found in both tables, return a default location
    if not unique_locations:
        default_location = f'0{store}10000000'
        logging.warning(f"No location found for item {item} in store {store}. Using default location: {default_location}")
        unique_locations.add(default_location)
    
    # Return a list of unique, non-archived locations
    return list(unique_locations)

def insert_orders(df: pd.DataFrame, session_factory) -> None:
    session = session_factory()
    try:
        for _, order in df.iterrows():
            # Check if the order already exists in the database
            existing_order = session.query(FullOrder).filter_by(order_number=order["order_number"],store=order["store"]).first()
            if existing_order:
                logging.warning(f"Order with number {order['order_number']} already exists. Skipping insertion.")
                continue

            full_order = FullOrder(
                order_number=order["order_number"],
                customer=order["customer"],
                client_name=order.get("client_name", None),
                phone_number=order.get("phone_number", None),
                order_info=json.dumps(order.get("order_info", None)),
                pickers=order.get("pickers", False),
                dispatch=order.get("dispatch", False),
                drivers=order.get("drivers", False),
                store=order["store"],
                address1=order.get("address1", None),
                address2=order.get("address2", None),
                address3=order.get("address3", None),
                ship_addr1=order.get("ship_addr1", None),
                ship_addr2=order.get("ship_addr2", None),
                ship_addr3=order.get("ship_addr3", None),
                clerk_name=order.get("clerk_name", None),
                price=order.get("price", None),
                job=order.get("job", None),
            )
            session.add(full_order)
            session.flush()

            logging.info(f"Successfully inserted {order['order_number']} into the database.")

            # Insert each item from order_info into the items table
            for item_data in order["order_info"]:
                # Verify if it has a localization in locations
                locs = find_location(session, item_data["upc"], item_data["item"], order["store"])
                
                if not locs:
                    logging.error(f"Failed to find location for item {item_data['item']} in order {order['order_number']}")
                    continue  # Skip if no valid location is found

                # Join the locations into a comma-separated string for storage
                loc_str = ','.join(locs)
                
                # Add item to database
                db_item = Items(
                    store=order['store'],
                    order_number=order["order_number"],
                    item=item_data["item"],
                    description=item_data["description"],
                    units=item_data["units"],
                    state=item_data["state"],
                    loc=loc_str,
                    upc=str(item_data["upc"]),
                )
                session.add(db_item)

        session.commit()

    except IntegrityError as e:
        session.rollback()
        logging.error(f"Integrity error occurred during insertion of order: {e}")

    except DBAPIError as e:
        session.rollback()
        logging.error(f"Database error occurred during insertion of order: {e}")

    except Exception as e:
        session.rollback()
        logging.error(f"Unexpected error during insertion of order: {e}")

    finally:
        session.close()

def get_eastern_time():
    return datetime.now(pytz.timezone('America/New_York'))

class FullOrder(Base):
    __tablename__ = "full_orders"
    id = Column(Integer, primary_key=True, autoincrement=True)
    order_number = Column(String(80), nullable=False, unique=True)
    customer = Column(String(255), nullable=False)
    client_name = Column(String(255), nullable=True)
    phone_number = Column(String(64), nullable=True)
    order_info = Column(String(8096), nullable=True)
    pickers = Column(Boolean, nullable=False)
    dispatch = Column(Boolean, nullable=False)
    drivers = Column(Boolean, nullable=False)
    store = Column(Integer, nullable=False)
    address1 = Column(String(255), nullable=True)
    address2 = Column(String(255), nullable=True)
    address3 = Column(String(255), nullable=True)
    ship_addr1 = Column(String(255), nullable=True)
    ship_addr2 = Column(String(255), nullable=True)
    ship_addr3 = Column(String(255), nullable=True)
    clerk_name = Column(String(255), nullable=True)
    price = Column(String(255), nullable=True)
    job = Column(String(64), nullable=False)


class Items(Base):
    __tablename__ = "order_info"
    id = Column(Integer, primary_key=True, autoincrement=True)
    store = Column(Integer, nullable=False)
    order_number = Column(String(80), nullable=False)
    item = Column(String(255), nullable = False,)
    description= Column(String(255), nullable = False,)
    units= Column(Integer, nullable = False,)
    state = Column(String(255), nullable=False)
    updated_by = Column(String(255), nullable=True)
    loc = Column(String(9), nullable=False)
    is_reserved = Column(Boolean, default=False) 
    is_archived = Column(Boolean, default=False)
    is_missing = Column(Boolean, default=False)
    upc = Column(String(64), nullable=True)
    picked_by = Column(String(64), nullable=True)
    
class InvLocations(Base):
    __tablename__ = "locations"

    id = Column(Integer, primary_key=True, autoincrement=True)
    upc = Column(String)
    name = Column(String)
    store = Column(String)
    level = Column(String)
    row = Column(String)
    side = Column(String)
    column = Column(String)
    shelf = Column(String)
    full_location = Column(String)
    updated_by = Column(String)
    updated_at = Column(DateTime, default=get_eastern_time, onupdate=get_eastern_time)
    created_at = Column(DateTime, default=get_eastern_time)
    is_archived = Column(Boolean, default=False)
    

class PalletLocations(Base):
    __tablename__ = "loc_pallet"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    store =  Column(Integer)
    item =  Column(String)
    loc =  Column(String)