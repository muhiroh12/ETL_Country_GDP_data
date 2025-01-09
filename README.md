# ETL Project: Country-GDP Data

### Overview
This project involves creating a complete ETL (Extract, Transform, Load) pipeline. It is designed to scrape data about the largest banks by market capitalization from Wikipedia, transform this data by applying currency exchange rates, and then load the results into a SQLite database. Additionally, the transformed data is saved as a CSV file for further analysis.

### Prerequisites:
- Python 3.8+
- required packages:
    ```bash
    pip install beautifulsoup4
    ```
    ```bash
    pip install pandas
    ```
    ```bash
    pip install requests
    ```
    ```bash
    sudo apt install sqlite3
    ```

### Run the project
- clone the repository or download the files
- get the api key:
    1. Visit the [ExchangeRate-API website](https://www.exchangerate-api.com)
    2. Enter your email and password, then click Accept and Create API Key
    3. Check your email for a notification, and click the provided link to verify your account
    4. Copy the API key and paste it into the api_key variable in the api_exchange_rate.py file
- Run the api_exchange_rate.py file to fetch currency rates:
    ```bash
    python api_exchange_rate.py
    ```
- Execute the etl pipline:
    ```bash
    python main.py
    ```

### Logs

Process logs are saved to code_log.txt. Each step in the ETL process logs a timestamped message to track the pipeline's execution status.

