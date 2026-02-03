from sqlalchemy import Column, ForeignKey, ForeignKeyConstraint, Integer, String, DateTime, JSON
from sqlalchemy.orm import relationship
from app.db.base import Base


class GooddreamerTransaction(Base):
    """
    Model for representing a transaction involving coins in the Gooddreamer application.
    
    This class defines the schema for the 'gooddreamer_transaction' table in the database,
    along with the relationships to other tables. It records transactions related to coins,
    including the status, value, and creation timestamp.
    
    Attributes:
        id (int): Primary key for the transaction.
        user_id (int): Foreign key referencing the user who made the transaction.
        transaction_status (int): Status of the transaction.
        transaction_coin_value (int): Value of the transaction in coins.
        created_at (datetime): Timestamp when the transaction was created.
        
        gooddreamer_user_data (relationship): Relationship to the `GooddreamerUserData` model.
        gooddreamer_transaction_details (relationship): Relationship to the `GooddreamerTransactionDetails` model.
        gooddreamer_payment_data (relationship): Relationship to the `GooddreamerPaymentData` model.
        model_has_sources (relationship): Relationship to the `ModelHasSources` model.
    """
    __tablename__ = 'gooddreamer_transaction'
    __table_args__ = (
        ForeignKeyConstraint(['user_id'], ['gooddreamer_user_data.id']),
        {'schema': None}
    )
    __mapper_args__ = {
        'polymorphic_identity': 'gooddreamer_transaction'
    }

    id = Column("id", Integer, primary_key=True, index=True)
    user_id = Column("user_id", Integer, ForeignKey("gooddreamer_user_data.id"), index=True)
    transaction_status = Column("transaction_status", Integer)
    transaction_coin_value = Column("transaction_coin_value", Integer)
    created_at = Column("created_at", DateTime)

    gooddreamer_user_data = relationship(
        'GooddreamerUserData', 
        lazy=True, 
        back_populates='gooddreamer_transaction', 
        viewonly=True
    )
    gooddreamer_transaction_details = relationship(
        'GooddreamerTransactionDetails', 
        lazy=True, 
        back_populates='gooddreamer_transaction', 
        viewonly=True
    )
    gooddreamer_payment_data = relationship(
        'GooddreamerPaymentData', 
        lazy=True, 
        back_populates='gooddreamer_transaction', 
        viewonly=True
    )
    model_has_sources = relationship(
        'ModelHasSources', 
        lazy=True, 
        back_populates='gooddreamer_transaction', 
        viewonly=True
    )


class GooddreamerTransactionDetails(Base):
    """
    Model for representing the details of a transaction in the Gooddreamer application.
    
    This class defines the schema for the 'gooddreamer_transaction_details' table in the database,
    along with the relationships to other tables. It records detailed information about
    transactions, including the package price, fee, and discount value.
    
    Attributes:
        id (int): Primary key for the transaction details.
        transaction_id (int): Foreign key referencing the transaction.
        package_price (int): Price of the package involved in the transaction.
        package_fee (int): Fee associated with the package.
        discount_value (int): Discount value applied to the package.
        
        gooddreamer_transaction (relationship): Relationship to the `GooddreamerTransaction` model.
    """
    __tablename__ = 'gooddreamer_transaction_details'
    __table_args__ = (
        ForeignKeyConstraint(['transaction_id'], ['gooddreamer_transaction.id']),
        {'schema': None}
    )
    __mapper_args__ = {
        'polymorphic_identity': 'gooddreamer_transaction_details'
    }

    id = Column("id", Integer, primary_key=True, index=True)
    transaction_id = Column("transaction_id", Integer, ForeignKey("gooddreamer_transaction.id"), index=True)
    package_price = Column("package_price", Integer)
    package_fee = Column("package_fee", Integer)
    discount_value = Column('discount_value', Integer)

    gooddreamer_transaction = relationship(
        'GooddreamerTransaction', 
        lazy=True, 
        back_populates='gooddreamer_transaction_details', 
        viewonly=True
    )


class GooddreamerPaymentData(Base):
    """
    Model for representing payment data in the Gooddreamer application.
    
    This class defines the schema for the 'gooddreamer_payment_data' table in the database,
    and it stores information related to payment transactions such as the gateway name,
    payment channel, status, and timestamps.
    
    Attributes:
        id (int): Primary key for the payment data.
        transaction_id (int): Foreign key referencing the transaction.
        payment_gateway_name (int): Name of the payment gateway used.
        payment_channel (str): Payment channel used for the transaction.
        status (str): Status of the payment.
        paid_at (datetime): Timestamp when the payment was made.
        meta (str): Metadata related to the payment.
        bank_code (str): Bank code used for the transaction.
        
        gooddreamer_transaction (relationship): Relationship to the `GooddreamerTransaction` model.
    """
    __tablename__ = 'gooddreamer_payment_data'
    __table_args__ = (
        ForeignKeyConstraint(['transaction_id'], ['gooddreamer_transaction.id']),
        {'schema': None}
    )
    __mapper_args__ = {
        'polymorphic_identity': 'gooddreamer_payment_data'
    }

    id = Column("id", Integer, primary_key=True, index=True)
    transaction_id = Column("transaction_id", Integer, ForeignKey("gooddreamer_transaction.id"), index=True)
    payment_gateway_name = Column("payment_gateway_name", String)
    payment_channel = Column("payment_channel", String)
    status = Column("status", String)
    paid_at = Column("paid_at", DateTime)
    meta = Column("meta", JSON)
    bank_code = Column("bank_code", String)

    gooddreamer_transaction = relationship(
        'GooddreamerTransaction', 
        lazy=True, 
        back_populates='gooddreamer_payment_data', 
        viewonly=True
    )

