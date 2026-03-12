# Enterprise Data Simulation Framework - Data Warehouse Environment

This project contains a robust backend database schema and a synthetic data generation engine designed for simulating large-scale enterprise environments. It provides a comprehensive data warehouse simulation covering diverse business domains, including CRM, Billing, Finance, HR, Marketing, and Operations.

The framework is built to populate a PostgreSQL database with realistic, relationally complex data, making it an ideal tool for stress-testing database performance, developing complex SQL queries, and building advanced Analytics Pipelines or Data Visualization dashboards.

## Project Structure

*   **`generators/`**: Python modules containing the logic to generate synthetic data for various business domains (e.g., `orgs.py`, `users.py`, `billing.py`, `crm.py`).
*   **`enterprise_schema.sql`**: The SQL Data Definition Language (DDL) script defining the tables, indexes, and relationships.
*   **`load_all.py`**: The main orchestration script that generates data using the `generators` and populates the PostgreSQL database.
*   **`db.py`**: Database connection configuration and utility functions.
*   **`config.py`**: Configuration settings for data scale (e.g., number of users, organizations) and simulation dates.
*   **`verify_table_counts.py`**: A utility script to verify that data has been successfully loaded.

## Prerequisites

*   **Python 3.8+**
*   **PostgreSQL**: A local or remote PostgreSQL instance running.

## Installation

1.  **Clone the repository** to your local machine:
    ```bash
    git clone <repository_url>
    cd enterprise-data-simulator
    ```

2.  **Create a Virtual Environment**:
    It is recommended to use a virtual environment to manage dependencies.
    ```bash
    python -m venv venv
    ```

3.  **Activate the Virtual Environment**:
    *   **Windows (PowerShell)**:
        ```powershell
        .\venv\Scripts\Activate
        ```
    *   **macOS/Linux**:
        ```bash
        source venv/bin/activate
        ```

4.  **Install Dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

## Database Configuration

Before running the scripts, you need to configure your database connection.

1.  **Create a PostgreSQL User and Database**:
    The default configuration in `db.py` expects:
    *   **Database Name**: `enterprise_db`
    *   **User**: `db_admin`
    *   **Password**: `your_secure_password`
    *   **Host**: `localhost`

    If you wish to use different credentials, open **`db.py`** and update the `get_connection` function:
    ```python
    def get_connection():
        return psycopg2.connect(
            host="localhost",
            database="YOUR_DATABASE_NAME",
            user="YOUR_USERNAME",
            password="YOUR_PASSWORD",
            sslmode="prefer" # Set to 'require' if needed
        )
    ```

2.  **Schema Configuration (Important)**:
    *   The `load_all.py` script attempts to write to a schema named **`enterprise_data_local`**.
    *   The `enterprise_schema.sql` file defines the schema as **`enterprise_data`**.

    **Recommendation**: Update `db.py` and `load_all.py` to match your desired schema name, or create the `enterprise_data_local` schema in your database.

    To create the schema manually in SQL:
    ```sql
    CREATE SCHEMA IF NOT EXISTS enterprise_data_local;
    ```

## Usage

### 1. Initialize the Database Schema

You can create the tables using the provided SQL script by running it in your database management tool (e.g., pgAdmin, psql).

Run the `enterprise_schema.sql` script in your database tool. **Note:** Make sure to modify the `SET search_path` at the top of the file if you are using `enterprise_data_local`.

Alternatively, if `recreate_schema.py` is configured for your environment (check `TARGET_SCHEMA` inside dependencies), you can run:

```bash
python recreate_schema.py
```

### 2. Generate and Load Data

Run the main loading script. This process may take some time depending on the `SCALE` configured in `config.py`.

```bash
python load_all.py
```

This script will:
1.  Connect to your PostgreSQL database.
2.  Generate synthetic data for Organizations, Users, Accounts, Transactions, etc.
3.  Load the data directly into your database tables.

### 3. Verify Data Load

After the load is complete, you can verify the population of tables:

```bash
python verify_table_counts.py
```

## Customization

*   **Adjusting Data Scale**: Open **`config.py`** to change the volume of data generated.
    ```python
    SCALE = {
        "organizations": 400,
        "users": 40000,
        # ... modify as needed
    }
    ```
*   **Modifying Data Logic**: The generation logic is modular. You can edit specific files in the `generators/` directory to change distribution patterns, fields, or business logic.

