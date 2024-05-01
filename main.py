from pyspark.sql import SparkSession
import os
from typing import List


def read_parquets(path: str) -> List[str]:
    parquets = []
    if os.path.exists(path):
        files = os.listdir(path)
        for file in files:
            if file.endswith(".parquet"):
                parquets.append(f"{path}/{file}")

    return parquets


def write_parquet_to_postgres(parquet_files: List[str], hostname: str,
                              database: str, username: str,
                              password: str, port: str = "5432"):
    jdbc_driver = "./drivers/postgresql-jdbc.jar"

    spark = (SparkSession
             .builder
             .appName("ConvertParquets")
             .config("spark.jars", jdbc_driver)
             .getOrCreate())

    for parquet_file in parquet_files:
        delimiter = "/"
        if "\\" in parquet_file:
            delimiter = "\\"
        file_with_extension = parquet_file.split(delimiter)[-1]
        table_name = file_with_extension.replace(".parquet", "")

        try:
            spark_df = spark.read.parquet(parquet_file)
            spark_df.createOrReplaceTempView(table_name)
            (spark_df.write
             .format("jdbc")
             .option("driver", "org.postgresql.Driver")
             .option("url", f"jdbc:postgresql://{hostname}:{port}/{database}")
             .option("dbtable", table_name)
             .option("user", f"{username}")
             .option("password", f"{password}")
             .save())
            print(f"Successfully written file: {parquet_file}")
        except Exception as e:
            print(f"Failed to write to database: {database} ----> Error: " + str(e))

    spark.stop()


def main():
    while True:
        print("-----------------------")
        print(" PARQUET TO POSTGRESQL ")
        print("-----------------------")
        print("0 -> Convert to PostgreSQL")
        print("1 -> Exit")
        print("-----------------------")
        option = input("Select option: ")

        if option == "0":
            path = input("Parquet directory: ")
            parquets = read_parquets(path)
            if len(parquets) != 0:
                print("-- Parquet files loaded --")
                print("-----------------")
                print(" Postgres server ")
                print("-----------------")
                hostname = input("Hostname: ")
                database = input("Database: ")
                username = input("Username: ")
                password = input("Password: ")
                port = input("Port [def. 5432]: ")
                if port == "":
                    write_parquet_to_postgres(parquets, hostname, database, username, password)
                else:
                    write_parquet_to_postgres(parquets, hostname, database, username, password, port)
                input("Press enter to continue...")
            else:
                print("No parquet files found in that directory")
                input("Press enter to continue...")
        elif option == "1":
            print("Bye!")
            break

        os.system('cls' if os.name == 'nt' else 'clear')

    input("Press enter to continue...")


if __name__ == "__main__":
    main()
