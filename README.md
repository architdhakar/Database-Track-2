# Database-Track-2

⚙️ Setup & Installation
1. Prerequisites

Ensure you have the following installed locally:

Python 3.x

MySQL Server

MongoDB Community Server

2. Clone the Repository

Bash
git clone https://github.com/architdhakar/Database-Track-2
cd Database-Track-2

3. Install Dependencies

Bash
pip install -r requirements.txt
4. Configure Environment Variables

Create a .env file in the root directory and add your database credentials:


# MySQL Configuration
SQL_HOST=localhost

SQL_PORT=Enter the port

SQL_USER=Enter your username    

SQL_PASSWORD=your_password

SQL_DB_NAME=adaptive_db

# MongoDB Configuration
MONGO_URI=Enter the mongo db uri

MONGO_DB_NAME= Enter the DB name

5. Initialize the Database

Before running the engine, ensure your SQL user has permissions. The script will automatically create the tables if they don't exist.
