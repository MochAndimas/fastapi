from sqlalchemy import Column, ForeignKey, ForeignKeyConstraint, Integer, String, DateTime, JSON, Boolean, Float
from sqlalchemy.orm import relationship
from app.db.base import Base, SqliteBase


class GooddreamerUserData(Base):
    """
    GooddreamerUserData represents the user data model for the Gooddreamer application.

    Attributes:
        id (int): Primary key for the user data table.
        fullname (str): Full name of the user.
        email (str): Email address of the user.
        is_guest (int): Indicator if the user is a guest (1 for yes, 0 for no).
        registered_at (datetime): Date and time when the user registered.
        created_at (datetime): Date and time when the user data was created.
        password_hash (str): Hashed password of the user.
    """

    __tablename__ = 'gooddreamer_user_data'
    __mapper_args__ = {
        'polymorphic_identity': 'gooddreamer_user_data'
    }

    id = Column('id', Integer, primary_key=True, index=True)
    fullname = Column('fullname', String)
    email = Column('email', String)
    is_guest = Column('is_guest', Integer)
    registered_at = Column('registered_at', DateTime)
    created_at = Column('created_at', DateTime)
    password_hash = Column('password', String)

    model_has_sources = relationship(
        'ModelHasSources', 
        lazy=True, 
        back_populates='gooddreamer_user_data', 
        viewonly=True)
    gooddreamer_user_chapter_progression = relationship(
        'GooddreamerUserChapterProgression', 
        lazy=True, 
        back_populates='gooddreamer_user_data', 
        viewonly=True)
    gooddreamer_transaction = relationship(
        'GooddreamerTransaction', 
        lazy=True, 
        back_populates='gooddreamer_user_data', 
        viewonly=True)
    gooddreamer_chapter_transaction = relationship(
        'GooddreamerChapterTransaction', 
        lazy=True, 
        back_populates='gooddreamer_user_data', 
        viewonly=True)
    gooddreamer_user_chapter_admob = relationship(
        'GooddreamerUserChapterAdmob', 
        lazy=True, 
        back_populates='gooddreamer_user_data',
        viewonly=True)
    code_redeem = relationship(
        'CodeRedeem', 
        lazy=True, 
        back_populates='gooddreamer_user_data',
        viewonly=True)
    illustration_transaction = relationship(
        "IllustrationTransaction",
        lazy=True,
        back_populates="gooddreamer_user_data",
        viewonly=True
    )
    gooddreamer_user_collection = relationship(
        "GooddreamerUserCollection",
        lazy=True,
        back_populates="gooddreamer_user_data",
        viewonly=True
    )
    gooddreamer_user_favorite = relationship(
        "GooddreamerUserFavorite",
        lazy=True,
        back_populates="gooddreamer_user_data",
        viewonly=True
    )


class GooddreamerUserWalletItem(Base):
    """
    Model for representing items in a user's wallet in the Gooddreamer application.
    
    This class defines the schema for the 'gooddreamer_user_wallet_item' table in the database,
    and it records details about items related to chapter transactions, including the coin type and transaction item.
    
    Attributes:
        id (int): Primary key for the wallet item.
        reffable_id (int): Foreign key referencing the chapter transaction.
        coin_type (str): Type of coin involved in the transaction.
        transaction_item (str): Item involved in the transaction.
        reffable_type (str): Type of the referenceable entity.
        
        gooddreamer_chapter_transaction (relationship): Relationship to the `GooddreamerChapterTransaction` model.
    """
    __tablename__ = 'gooddreamer_user_wallet_item'
    __table_args__ = (
        ForeignKeyConstraint(['reffable_id'], ['gooddreamer_chapter_transaction.id']),
        {'schema': None}
    )
    __mapper_args__ = {
        'polymorphic_identity': 'gooddreamer_user_wallet_item'
    }

    id = Column("id", Integer, primary_key=True, index=True)
    reffable_id = Column("reffable_id", Integer, ForeignKey("gooddreamer_chapter_transaction.id"), index=True)
    coin_type = Column("coin_type", String)
    transaction_item = Column("transaction_item", String)
    reffable_type = Column("reffable_type", String)

    gooddreamer_chapter_transaction = relationship(
        'GooddreamerChapterTransaction', 
        lazy=True, 
        back_populates='gooddreamer_user_wallet_item', 
        viewonly=True
    )


class Codes(Base):
    """
    """
    __tablename__ = "codes"
    __mapper_args__ = {
        "polymorphic_identity": "codes"
    }

    id = Column("id", Integer, primary_key=True, index=True)
    name = Column("name", String)
    code = Column("code", String)
    ads_coin_amount = Column("ads_coin_amount", Integer)
    start_date = Column("start_date", DateTime)
    end_date = Column("end_date", DateTime)
    user_type = Column("user_type", Integer)
    type = Column("type", Integer)
    active = Column("active", Integer)

    code_redeem = relationship(
        'CodeRedeem', 
        lazy=True, 
        back_populates='codes',
        viewonly=True)


class Illustrations(Base):
    """
    """
    __tablename__ = "illustrations"
    __table_args__ = (
        ForeignKeyConstraint(["novel_id"], ['gooddreamer_novel.id']),
        {"schema": None}
    )
    __mapper_args__ = {
        "polymorphic_identity": "illustrations"
    }

    id = Column("id", Integer, primary_key=True, index=True)
    novel_id = Column("novel_id", Integer, ForeignKey("gooddreamer_novel.id"), index=True)
    title = Column("title", String)
    price = Column("price", Integer)
    created_at = Column("created_at", DateTime)

    gooddreamer_novel = relationship(
        "GooddreamerNovel",
        lazy=True,
        back_populates="illustrations",
        viewonly=True
    )
    illustration_transaction = relationship(
        "IllustrationTransaction",
        lazy=True,
        back_populates="illustrations",
        viewonly=True
    )


class CodeRedeem(Base):
    """
    """
    __tablename__ = "code_redeem"
    __table_args__ = (
        ForeignKeyConstraint(["code_id"], ["codes.id"]),
        ForeignKeyConstraint(["user_id"], ["gooddreamer_user_data.id"]),
        {"schema": None}
    )
    __mapper_args__ = {
        "polymorphic_identity" : "code_redeem"
    }

    id = Column("id", Integer, primary_key=True, index=True)
    code_id = Column("code_id", Integer, ForeignKey("codes.id"), index=True)
    user_id = Column("user_id", Integer, ForeignKey("gooddreamer_user_data.id"), index=True)
    created_at = Column("created_at", DateTime)

    codes = relationship(
        'Codes', 
        lazy=True, 
        back_populates='code_redeem', 
        viewonly=True
    )
    gooddreamer_user_data = relationship(
        'GooddreamerUserData', 
        lazy=True, 
        back_populates='code_redeem', 
        viewonly=True
    )


class IllustrationTransaction(Base):
    """
    """
    __tablename__ = "illustration_transaction"
    __table_args__ = (
        ForeignKeyConstraint(['user_id'], ['gooddreamer_user_data.id']),
        ForeignKeyConstraint(['illustration_id'], ['illustrations.id']),
        {"schema": None}
    )
    __mapper_args__ = {
        "polymorphic_identity" : "illustration_transaction"
    }

    id = Column("id", Integer, primary_key=True, index=True)
    user_id = Column("user_id", Integer, ForeignKey("gooddreamer_user_data.id"), index=True)
    illustration_id = Column("illustration_id", Integer, ForeignKey("illustrations.id"), index=True)
    transaction_status = Column("transaction_status", Integer)
    transaction_coin_value = Column("transaction_coin_value", Integer)
    created_at = Column("created_at", DateTime)

    gooddreamer_user_data = relationship(
        "GooddreamerUserData",
        lazy=True,
        back_populates="illustration_transaction",
        viewonly=True
    )

    illustrations = relationship(
        "Illustrations",
        lazy=True,
        back_populates="illustration_transaction",
        viewonly=True
    )


class UserToken(SqliteBase):
    """
    """
    __tablename__ = "user_token"

    id  = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(Integer, nullable=False)
    user_id = Column(Integer, nullable=False)
    page = Column(String, nullable=False)
    logged_in = Column(Boolean, nullable=False)
    role = Column(String, nullable=False)
    expiry = Column(DateTime, nullable=False)
    access_token = Column(String, nullable=False)
    refresh_token = Column(String, nullable=False)
    is_revoked = Column(Boolean, nullable=False)
    created_at = Column(DateTime, nullable=False)
    updated_at  = Column(DateTime, nullable=False)
    deleted_at = Column(DateTime, nullable=True)


class LogData(SqliteBase):
    """
    """
    __tablename__ = "log_data"

    id = Column(Integer, primary_key=True, autoincrement=True)
    url = Column(String, nullable=False)
    method = Column(String, nullable=False)
    time = Column(Float, nullable=False)
    status = Column(Integer, nullable=False)
    response = Column(JSON, nullable=False)
    created_at = Column(DateTime, nullable=False)


