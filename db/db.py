from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from db.model import Base

class DatabaseConnection:
    def __init__(self):
        self.engine = create_engine('postgresql+psycopg2://admin:admin_password@localhost:5432/meu_banco')
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        Base.metadata.create_all(bind=self.engine)

    def get_session(self):
        return self.SessionLocal()

