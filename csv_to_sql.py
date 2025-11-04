{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 1,
   "id": "ecf528f0-945c-4cd1-83ea-0f7a4f0d4d58",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "C:\\Users\\91939\\miniconda3\\python.exe\n"
     ]
    }
   ],
   "source": [
    "import sys\n",
    "print(sys.executable)\n",
    "\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 2,
   "id": "c2cafefa-e3fa-4db6-9ff6-af8f0f403793",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "✅ Connected successfully!\n"
     ]
    }
   ],
   "source": [
    "import mysql.connector\n",
    "import pandas as pd\n",
    "\n",
    "conn = mysql.connector.connect(\n",
    "    host=\"localhost\",\n",
    "    user=\"root\",\n",
    "    password=\"vinay@12345678\",\n",
    "    database=\"ecommerce\"\n",
    ")\n",
    "print(\"✅ Connected successfully!\")\n",
    "\n",
    "conn.close()\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 3,
   "id": "43b7629c-4acd-419d-ba53-826463919c7a",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "  Tables_in_ecommerce\n",
      "0           customers\n",
      "1         geolocation\n",
      "2         order_items\n",
      "3              orders\n",
      "4            payments\n",
      "5            products\n",
      "6             sellers\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "C:\\Users\\91939\\AppData\\Local\\Temp\\ipykernel_89520\\4068832110.py:4: UserWarning: pandas only supports SQLAlchemy connectable (engine/connection) or database string URI or sqlite3 DBAPI2 connection. Other DBAPI2 objects are not tested. Please consider using SQLAlchemy.\n",
      "  return pd.read_sql(query, conn)\n"
     ]
    }
   ],
   "source": [
    "import pandas as pd\n",
    "\n",
    "def run_query(query):\n",
    "    return pd.read_sql(query, conn)\n",
    "\n",
    "conn = mysql.connector.connect(\n",
    "    host=\"localhost\",\n",
    "    user=\"root\",\n",
    "    password=\"vinay@12345678\",\n",
    "    database=\"ecommerce\"\n",
    ")\n",
    "\n",
    "query = \"SHOW TABLES;\"\n",
    "print(run_query(query))\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 4,
   "id": "130d08f0-d7f2-4dff-8665-40419379ade6",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "\n",
      "📥 Loading customers.csv → Table: customers\n",
      "\n",
      "📥 Loading orders.csv → Table: orders\n",
      "\n",
      "📥 Loading products.csv → Table: products\n",
      "\n",
      "📥 Loading payments.csv → Table: payments\n",
      "\n",
      "📥 Loading order_items.csv → Table: order_items\n",
      "\n",
      "📥 Loading geolocation.csv → Table: geolocation\n",
      "\n",
      "📥 Loading sellers.csv → Table: sellers\n",
      "\n",
      "✅ All CSV files successfully loaded into MySQL!\n"
     ]
    }
   ],
   "source": [
    "\n",
    "import pandas as pd\n",
    "import mysql.connector\n",
    "import os\n",
    "\n",
    "\n",
    "conn = mysql.connector.connect(\n",
    "    host=\"localhost\",\n",
    "    user=\"root\",\n",
    "    password=\"vinay@12345678\",  \n",
    "    database=\"ecommerce\"\n",
    ")\n",
    "cursor = conn.cursor()\n",
    "\n",
    "\n",
    "\n",
    "\n",
    "folder_path = r\"C:\\Users\\91939\\OneDrive\\Desktop\\Ecommerce\\Target Sales Dataset\"\n",
    "\n",
    "\n",
    "csv_files = [\n",
    "    (\"customers.csv\", \"customers\"),\n",
    "    (\"orders.csv\", \"orders\"),\n",
    "    (\"products.csv\", \"products\"),\n",
    "    (\"payments.csv\", \"payments\"),\n",
    "    (\"order_items.csv\", \"order_items\"),\n",
    "    (\"geolocation.csv\", \"geolocation\"),\n",
    "    (\"sellers.csv\", \"sellers\")\n",
    "]\n",
    "\n",
    "def get_sql_type(dtype):\n",
    "    if pd.api.types.is_integer_dtype(dtype):\n",
    "        return \"INT\"\n",
    "    elif pd.api.types.is_float_dtype(dtype):\n",
    "        return \"FLOAT\"\n",
    "    elif pd.api.types.is_bool_dtype(dtype):\n",
    "        return \"BOOLEAN\"\n",
    "    elif pd.api.types.is_datetime64_any_dtype(dtype):\n",
    "        return \"DATETIME\"\n",
    "    else:\n",
    "        return \"TEXT\"\n",
    "\n",
    "for file, table in csv_files:\n",
    "    path = os.path.join(folder_path, file)\n",
    "    if not os.path.exists(path):\n",
    "        print(f\"⚠️ File not found: {file}\")\n",
    "        continue\n",
    "\n",
    "    print(f\"\\n📥 Loading {file} → Table: {table}\")\n",
    "    df = pd.read_csv(path)\n",
    "    df = df.where(pd.notnull(df), None)\n",
    "\n",
    "    \n",
    "    df.columns = [col.strip().replace(\" \", \"_\").replace(\"-\", \"_\") for col in df.columns]\n",
    "\n",
    "\n",
    "    cols = \", \".join([f\"`{col}` {get_sql_type(df[col].dtype)}\" for col in df.columns])\n",
    "    cursor.execute(f\"CREATE TABLE IF NOT EXISTS `{table}` ({cols});\")\n",
    "\n",
    "\n",
    "    for _, row in df.iterrows():\n",
    "        values = tuple(None if pd.isna(x) else x for x in row)\n",
    "        sql = f\"INSERT INTO `{table}` ({', '.join(['`' + c + '`' for c in df.columns])}) VALUES ({', '.join(['%s'] * len(row))})\"\n",
    "        cursor.execute(sql, values)\n",
    "    conn.commit()\n",
    "\n",
    "print(\"\\n All CSV files successfully loaded into MySQL!\")\n",
    "conn.close()\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "6d6f234c-b7c3-43f7-9bd8-5688b3f473f6",
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.12.9"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
