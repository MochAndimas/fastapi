from sqlalchemy import Column, ForeignKey, ForeignKeyConstraint, Integer, String, DateTime, JSON
from sqlalchemy.orm import relationship
from app.db.base import Base


class Sources(Base):
    """
    Model for representing sources in the Gooddreamer application.
    
    This class defines the schema for the 'sources' table in the database,
    and it records source information such as the name and creation timestamp.
    
    Attributes:
        id (int): Primary key for the source.
        name (str): Name of the source.
        created_at (datetime): Timestamp when the source was created.
        
        model_has_sources (relationship): Relationship to the `ModelHasSources` model.
    """
    
    __tablename__ = 'sources'
    __mapper_args__ = {
        'polymorphic_identity': 'sources'
    }

    id = Column('id', Integer, primary_key=True, index=True)
    name = Column('name', String)
    created_at = Column('created_at', DateTime)

    model_has_sources = relationship(
        'ModelHasSources', 
        lazy=True, 
        back_populates='sources', 
        viewonly=True
    )


class ModelHasSources(Base):
    """
    Model for representing the association between models and sources in the Gooddreamer application.
    
    This class defines the schema for the 'model_has_sources' table in the database,
    and it creates a many-to-many relationship between different models and sources.
    
    Attributes:
        source_id (int): Foreign key referencing the source.
        model_type (str): Type of the model.
        model_id (int): Primary key for the model reference.
        
        sources (relationship): Relationship to the `Sources` model.
        gooddreamer_user_data (relationship): Relationship to the `GooddreamerUserData` model.
        gooddreamer_user_chapter_progression (relationship): Relationship to the `GooddreamerUserChapterProgression` model.
        gooddreamer_chapter_transaction (relationship): Relationship to the `GooddreamerChapterTransaction` model.
        gooddreamer_user_chapter_admob (relationship): Relationship to the `GooddreamerUserChapterAdmob` model.
        gooddreamer_transaction (relationship): Relationship to the `GooddreamerTransaction` model.
    """
    
    __tablename__ = 'model_has_sources'
    __table_args__ = (
        ForeignKeyConstraint(['source_id'], ['sources.id']),
        ForeignKeyConstraint(['model_id'], ['gooddreamer_user_data.id']),
        ForeignKeyConstraint(['model_id'], ['gooddreamer_user_chapter_progression.id']),
        ForeignKeyConstraint(['model_id'], ['gooddreamer_chapter_transaction.id']),
        ForeignKeyConstraint(['model_id'], ['gooddreamer_user_chapter_admob.id']),
        ForeignKeyConstraint(['model_id'], ['gooddreamer_transaction.id']),
        {'schema': None}
    )
    __mapper_args__ = {
        'polymorphic_identity':'model_has_sources'
    }

    source_id = Column('source_id', Integer, ForeignKey('sources.id'), index=True)
    model_type = Column('model_type', String, index=True)
    model_id = Column(
        'model_id', 
        Integer,
        ForeignKey('gooddreamer_user_data.id'),
        ForeignKey('gooddreamer_user_chapter_progression.id'),
        ForeignKey('gooddreamer_chapter_transaction.id'),
        ForeignKey('gooddreamer_user_chapter_admob.id'),
        ForeignKey('gooddreamer_transaction.id'),
        index=True,
        primary_key=True
    )
    
    sources = relationship(
        'Sources', 
        lazy=True, 
        back_populates='model_has_sources', 
        viewonly=True, 
        uselist=False
    )
    gooddreamer_user_data = relationship(
        'GooddreamerUserData', 
        lazy=True, 
        back_populates='model_has_sources', 
        viewonly=True
    )
    gooddreamer_user_chapter_progression = relationship(
        'GooddreamerUserChapterProgression', 
        lazy=True, 
        back_populates='model_has_sources', 
        viewonly=True
    )
    gooddreamer_chapter_transaction = relationship(
        'GooddreamerChapterTransaction', 
        lazy=True, 
        back_populates='model_has_sources', 
        viewonly=True
    )
    gooddreamer_user_chapter_admob = relationship(
        'GooddreamerUserChapterAdmob', 
        lazy=True, 
        back_populates='model_has_sources', 
        viewonly=True
    )
    gooddreamer_transaction = relationship(
        'GooddreamerTransaction', 
        lazy=True, 
        back_populates='model_has_sources', 
        viewonly=True
    )

