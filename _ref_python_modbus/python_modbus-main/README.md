# Python Backend Project with PostgreSQL

This project demonstrates how to connect to a PostgreSQL database using credentials from a `.env` file and select all records from the `inverter` table.

## Setup

##

1. Create a `.env` file in the project root with your PostgreSQL credentials:
   ```env
   DB_HOST=localhost
   DB_PORT=5432
   DB_NAME=your_db_name
   DB_USER=your_db_user
   DB_PASSWORD=your_db_password
   ```
2. Install dependencies:
   ```sh
   pip install -r requirements.txt
   ```
3. Run the script:
   ```sh
   python main.py
   ```

## Functionality

- Connects to PostgreSQL using environment variables.
- Selects and lists all records from the `inverter` table.
