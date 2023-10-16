# multinational-retail-data-centralisation

This project focuses on centralising retail data from various sources into a central PostgreSQL database using pgAdmin 4. The extracted data includes company information, user data, card details, store information, product data, and order information. The goal is to facilitate analysis and gain valuable insights from this consolidated data.

## Project Overview
The primary components of this project include:

###<b>Database Setup:</b>### Setting up a local PostgreSQL database named sales_data using pgAdmin 4 to store the extracted retail data.

### AWS RDS Data Extraction: 
Extracting historical user data from an AWS RDS database using the provided credentials and methods in the DatabaseConnector and DataExtractor classes.

Card Details Extraction from PDF: Extracting users' card details from a PDF document stored in an AWS S3 bucket, cleaning the data, and storing it in the dim_card_details table.

Store Data Retrieval from API: Retrieving store information through API endpoints, cleaning the data, and storing it in the dim_store_details table.

Product Data Extraction from CSV: Extracting product data stored in a CSV format in an AWS S3 bucket, cleaning and standardising the weights, and storing it in the dim_products table.

Order Data Extraction: Extracting order information from a database table and cleaning the data before storing it in the orders_table table.


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
