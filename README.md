# Building ETL for Binance Testnet data 

This project is part of another project which involves building a trading bot using machine learning techniques and data
obtained from Binance, a cryptocurrency exchange. The primary goal of the bot is to make informed trading decisions 
based on historical data and market trends. This repository focuses on its ETL (Extract, Transform and Load) component. 
The project uses mainly Python even if SQL is also used.

### Data Retrieval

To train the machine learning model, data is retrieved from Binance Testnet using their API. The API allows us to access 
historical market data for various cryptocurrencies. The retrieved data consists of 12 columns, providing valuable 
information about the price, volume, and other relevant indicators for each cryptocurrency.

### Data Transformation

Once the data is retrieved, it undergoes a transformation process to enhance its usefulness for the machine learning model. During this step, various data preprocessing techniques are applied, including feature engineering, normalization, and scaling. The transformation process results in an expanded dataset with 337 columns, capturing a wide range of relevant information.

### MySQL Database Setup

To store and manage the transformed data, a MySQL database has been set up using the LAMP (Linux, Apache, MySQL, PHP) stack. The database provides a structured environment for storing the processed data efficiently. This allows for easy retrieval and analysis of the data when training and deploying the trading bot.
ETL Python Script

The project includes an Extract-Transform-Load (ETL) Python script that handles the data pipeline. The script retrieves the data from Binance Testnet API, performs the necessary data transformations locally, and inserts the processed data into the MySQL database. The ETL script ensures a smooth and automated flow of data from the data source to the database.

### Project Structure

The project is organized as follows:

- functions.py       # Python scripts for data extraction, transformation, and loading
- config.py          # PConstants such as  MYSQL_HOST, MYSQL_PORT, or MYSQL_DATABASE_NAME,
- README.md          # Project documentation (you are here)

Getting Started

To run the trading bot project, follow these steps:

    Set up a LAMP environment on your system.
    Install the required Python libraries and dependencies (requirements.txt).
    Create a database and a user with all privileges over the created databse
    You do not need to create the table, it will be created automatically ()
    You can create a .env file to store your database credentials
    Open the config.py file update variables related to your database
    You can run the main.py file. This will perform only one execution.
    For coninuous execution use a while loop or set up a cronjob
    The project was tested with a cronjob that runs the main.py script every 5 minutes.
    Update this line and add it to your crontab: */5 * * * * /path/.../bin/python3.10 /path/.../my_project/main.py

### License

This project is licensed under the MIT License. Feel free to modify and adapt the code to suit your needs.