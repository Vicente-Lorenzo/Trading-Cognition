from Setup.Universe import populate
from Library.Database.Postgres.Postgres import PostgresDatabaseAPI

def main():
    print("Connecting to database...")
    db = PostgresDatabaseAPI(database="Quant")
    db.connect()
    
    try:
        print("Populating universe...")
        populate(db)
        print("Population successful.")
    finally:
        db.disconnect()

if __name__ == "__main__":
    main()