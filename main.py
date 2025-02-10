import asyncio
import base64
from contextlib import asynccontextmanager
from typing import List
from pydantic import BaseModel
from sqlalchemy.ext.declarative import declarative_base
from fastapi.responses import ORJSONResponse
from sqlalchemy.orm import sessionmaker
from cryptography.fernet import Fernet
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from dotenv import load_dotenv
import pandas as pd
import logging
import hmac
import os
import io
from balancer import check_server_status
from processor import find_location, process_and_store_dataframe
from fastapi import Body, FastAPI, Header, UploadFile, File, status

# Load the .env file
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

# Get the environment variables
PARSER_KEY = os.getenv("PARSER_KEY")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
ENCRYPTION_SUITE = Fernet(os.getenv("ENCRYPTION_KEY").encode())

# Create the base class for the ORM
Base = declarative_base()

# Create the database URL
DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Create the database engine
engine = create_engine(DATABASE_URL, poolclass=NullPool)

# Create a session factory
SessionFactory = sessionmaker(autocommit=False, autoflush=False, bind=engine)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start up
    monitor_task = asyncio.create_task(check_server_status())
    yield
    # Shutdown
    monitor_task.cancel()
    try:
        await monitor_task
    except asyncio.CancelledError:
        logging.info("Monitoring task cancelled")

# Declare the FastAPI app
app = FastAPI(debug=False, docs_url=None, redoc_url=None, lifespan=lifespan)

# ===========================================
# Configure the logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%H:%M:%S",
)

# Set the log level for 'multipart.multipart' to 'WARNING' to suppress debug messages
logging.getLogger("multipart.multipart").setLevel(logging.WARNING)

logging.info("=================================")
logging.info("---------- Superparser ----------")
logging.info("=================================\n")

class LocationRequest(BaseModel):
    upc: List[str]
    item: str
    store: str

# Route to upload Pickle data and queue it
@app.post("/")
async def upload_data(
    token: str = Header(None, alias="PARSER_KEY"),
    file: UploadFile = File(...),
) -> ORJSONResponse:
    try:
        # Handle authentication
        if not hmac.compare_digest(token, PARSER_KEY):
            return ORJSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={"message": "Unauthorized"},
            )

        # Read the file, decrypt it, and load it as a DataFrame
        df = pd.read_pickle(io.BytesIO(ENCRYPTION_SUITE.decrypt(await file.read())))

        # Check if the DataFrame is empty
        if df.empty:
            return ORJSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={"message": "Unauthorized"},
            )

        # Process the data
        process_and_store_dataframe(df, SessionFactory)

        # Return the response
        return ORJSONResponse(
            status_code=status.HTTP_200_OK,
            content={"message": "Success to insert the data"},
        )
    except Exception as _:
        logging.error(f"Error processing data: {_}")
        return ORJSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"message": "Failed - General error"},
        )

@app.post("/find-location")
async def find_location_route(
    request: LocationRequest = Body(...),
) -> ORJSONResponse:
    try:
        # Call the find_location function using the request data
        location = find_location(SessionFactory(), request.upc, request.item, request.store)

        if location:
            return ORJSONResponse(
                status_code=200,
                content={"location": location},
            )
        else:
            return ORJSONResponse(
                status_code=404,
                content={"message": "Location not found"},
            )
    except Exception as e:
        logging.error(f"Error finding location: {e}")
        return ORJSONResponse(
            status_code=500,
            content={"message": "Failed - General error"},
        )

@app.post("/inv")
async def upload_inv(
    token: str = Header(None, alias="PARSER_KEY"),
    body: dict[str, bytes] = {}
) -> ORJSONResponse:
    try:
        if not hmac.compare_digest(token, PARSER_KEY):
            return ORJSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={"message": "Unauthorized"},
            )

        if 'data' not in body or not body['data']:
            return ORJSONResponse(
                status_code=status.HTTP_204_NO_CONTENT,
                content={"message": "No data Received."},
            )
        # Decode the Base64-encoded string back to bytes
        result = base64.b64decode(body['data'])

        # Converting data from DataFrame to CSV to allow the compression
        try:
            logging.info("Decrypting..")
            unencrypted_data = ENCRYPTION_SUITE.decrypt(result)
            logging.info("Decompressing..")
            uncompressed_data = unencrypted_data.decode('utf-8')
        except Exception as e:
            logging.error(f"Decompression failed: {e}")
            return ORJSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content={"message": "processing failed"},
            )
        #Sending to database
        logging.info("Data processing complete. Sending Data to Database..")
        tableRows = [
            "upc",
            "sku",
            "item",
            "description",
            "package_quantity",
        ]
        csv_file = io.StringIO(uncompressed_data)
        df = pd.read_csv(csv_file)
        filtered_data = df[tableRows]

        filtered_data['upc'] = filtered_data['upc'].str.strip()
        print(str(filtered_data))
        filtered_data.to_sql(name='inventory', con=engine, if_exists='replace', index=True, chunksize=2500, index_label='id')
        return ORJSONResponse(
            status_code=status.HTTP_200_OK,
            content={"message": "Inventory saved Successfully."}
        )
    except Exception as e:
        logging.error(f"Error processing data: {e}")
        return ORJSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"message": "Failed - General error"},
        )

@app.post("/locPallets")
async def get_palettes_inv() -> ORJSONResponse:

    logging.info("Data processing complete. Sending Data to Database..")
    
    try:
        # Define the columns to filter
        tableColumns = ["Store Number", "Part Number", "Location 2"]

        # Read the CSV file with low_memory=False
        df = pd.read_csv("./csv/SORTIRSTOCK.csv", low_memory=False)

        # Check if df is loaded correctly
        if df is None or not isinstance(df, pd.DataFrame):
            raise ValueError("Failed to read the CSV file or the DataFrame is None.")

        # Ensure columns exist in the DataFrame
        for col in tableColumns:
            if col not in df.columns:
                raise ValueError(f"Column '{col}' not found in CSV file.")

        # Filter the dataframe to the specified columns
        filtered_data = df[tableColumns].copy()

        # Check if filtered_data is a valid DataFrame
        if filtered_data is None or not isinstance(filtered_data, pd.DataFrame):
            raise ValueError("Filtered data is None or not a DataFrame.")

        # Rename columns
        filtered_data.rename(columns={
            "Store Number": "store",
            "Part Number": "item",
            "Location 2": "loc"
        }, inplace=True)

        # Remove rows with any NULL (NaN) values
        filtered_data.dropna(inplace=True)

        # Remove rows that contain 'ES1', 'ET2', or 0
        filtered_data = filtered_data[~filtered_data.isin(['ES1', 'ET2', 0]).any(axis=1)]

        # Check if filtering caused any issues
        if filtered_data is None or not isinstance(filtered_data, pd.DataFrame):
            raise ValueError("Filtered data after removing specific values is None or not a DataFrame.")

        # Remove duplicate rows
        filtered_data.drop_duplicates(inplace=True)

        # Reset the index and rename it to 'id'
        filtered_data.reset_index(drop=True, inplace=True)
        filtered_data.index.name = 'id'

        # Convert DataFrame to SQL table with 'id' as the primary key
        filtered_data.to_sql(name='loc_pallet', con=engine, if_exists='replace', index=True, chunksize=2500, index_label='id')

        # Return success response
        return ORJSONResponse(
            status_code=status.HTTP_200_OK,
            content={"message": "Locations of pallets saved successfully."}
        )

    
    except ValueError as e:
        logging.error(f"Value error: {e}")
        return ORJSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"message": str(e)}
        )
    except Exception as e:
        # Log the error and return an error response
        logging.error(f"An error occurred: {e}")
        return ORJSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"message": "An error occurred while saving the data."}
        )