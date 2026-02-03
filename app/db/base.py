from sqlalchemy.ext.declarative import as_declarative, declared_attr, declarative_base

# Sqlite declarative base
SqliteBase = declarative_base()

@as_declarative()
class Base:
    id: int
    __name__: str

    @declared_attr
    def __tablename__(cls) -> str:
        return cls.__name__.lower()