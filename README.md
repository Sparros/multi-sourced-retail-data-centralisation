# multinational-retail-data-centralisation

This project demonstrates the centralisation of retail data from diverse sources into a centralized PostgreSQL database using pgAdmin 4. The extracted data encompasses crucial aspects such as company information, user data, card details, store information, product data, and order information. The primary objective is to enable comprehensive analysis and derive valuable insights from this consolidated data.

## Project Overview
The primary components of this project include:

__Database Setup:__ Setting up a local PostgreSQL database named sales_data using pgAdmin 4 to store the extracted retail data.

__AWS RDS Data Extraction:__ Extracting historical user data from an AWS RDS database using the provided credentials and methods in the DatabaseConnector and DataExtractor classes.

__Card Details Extraction from PDF:__ Extracting users' card details from a PDF document stored in an AWS S3 bucket, cleaning the data, and storing it in the dim_card_details table.

__Store Data Retrieval from API:__ Retrieving store information through API endpoints, cleaning the data, and storing it in the dim_store_details table.

__Product Data Extraction from CSV:__ Extracting product data stored in a CSV format in an AWS S3 bucket, cleaning and standardising the weights, and storing it in the dim_products table.

__Order Data Extraction:__ Extracting order information from a database table and cleaning the data before storing it in the orders_table table.


File Structure
```
retail-data-centralisation/
│
├── data_extractor.py        # DataExtractor class for data extraction
├── data_cleaning.py         # DataCleaning class for data cleaning
├── database_connector.py    # DatabaseConnector class for database interactions
├── my_file.py                  # Main script for data extraction and storage
└── README.md                # Project README file
```
