from sqlalchemy import Column, ForeignKey, ForeignKeyConstraint, Integer, String, DateTime, Boolean
from sqlalchemy.orm import relationship
from app.db.base import Base


class GooddreamerNovel(Base):
    """
    Model for representing a novel in the Gooddreamer application.
    
    This class defines the schema for the 'gooddreamer_novel' table in the database,
    along with the relationships to other tables. It includes fields for novel metadata
    such as author, title, publication status, and category. The class also handles the
    relationships with the `DataCategory`, `GooddreamerNovelChapter`, and 
    `GooddreamerChapterTransaction` models.
    
    Attributes:
        id (int): Primary key for the novel.
        author_id (int): Foreign key referencing the author of the novel.
        published_by (int): Foreign key referencing the user who published the novel.
        novel_title (str): Title of the novel.
        publication (int): Publication status of the novel.
        published_at (datetime): Timestamp when the novel was published.
        updated_at (datetime): Timestamp when the novel was last updated.
        status (int): Current status of the novel.
        finish_status (int): Status indicating whether the novel is finished.
        main_category (int): Foreign key referencing the main category of the novel.
        deleted_at (datetime): Timestamp when the novel was deleted (soft delete).
        
        data_category (relationship): Relationship to the `DataCategory` model.
        gooddreamer_novel_chapter (relationship): Relationship to the `GooddreamerNovelChapter` model.
        gooddreamer_chapter_transaction (relationship): Relationship to the `GooddreamerChapterTransaction` model.
    """
    __tablename__ = "gooddreamer_novel"
    __table_args__ = (
        ForeignKeyConstraint(['author_id'], ['gooddreamer_user_data.id']),
        ForeignKeyConstraint(['published_by'], ['gooddreamer_user_data.id']),
        ForeignKeyConstraint(['main_category'], ['data_category.id']),
        {'schema': None}
    )
    __mapper_args__ = {
        'polymorphic_identity': 'gooddreamer_novel'
    }

    id = Column("id", Integer, primary_key=True, index=True)
    author_id = Column("author_id", Integer, index=True)
    published_by = Column("published_by", Integer, index=True)
    novel_title = Column("novel_title", String)
    publication = Column("publication", Integer)
    published_at = Column("published_at", DateTime)
    updated_at = Column("updated_at", DateTime)
    status = Column("status", Integer)
    finish_status = Column("finish_status", Integer)
    main_category = Column("main_category", Integer, ForeignKey("data_category.id"))
    deleted_at = Column("deleted_at", DateTime)

    data_category = relationship(
        'DataCategory', 
        lazy=True, 
        back_populates='gooddreamer_novel', 
        viewonly=True
    )
    gooddreamer_novel_chapter = relationship(
        'GooddreamerNovelChapter', 
        lazy=True, 
        back_populates='gooddreamer_novel', 
        viewonly=True
    )
    gooddreamer_chapter_transaction = relationship(
        'GooddreamerChapterTransaction', 
        lazy=True, 
        back_populates='gooddreamer_novel', 
        viewonly=True
    )
    illustrations = relationship(
        "Illustrations",
        lazy=True,
        back_populates='gooddreamer_novel',
        viewonly=True
    )
    gooddreamer_user_collection = relationship(
        'GooddreamerUserCollection', 
        lazy=True, 
        back_populates='gooddreamer_novel', 
        viewonly=True
    )
    gooddreamer_user_favorite = relationship(
        "GooddreamerUserFavorite",
        lazy=True,
        back_populates="gooddreamer_novel",
        viewonly=True
    )


class GooddreamerNovelChapter(Base):
    """
    Model for representing a chapter of a novel in the Gooddreamer application.
    
    This class defines the schema for the 'gooddreamer_novel_chapter' table in the database,
    along with the relationships to other tables. It includes fields for chapter metadata
    such as title, word count, publication status, and sort order. The class also handles the
    relationships with the `GooddreamerNovel`, `GooddreamerUserChapterProgression`, and 
    `GooddreamerUserChapterAdmob` models.
    
    Attributes:
        id (int): Primary key for the chapter.
        novel_id (int): Foreign key referencing the novel to which the chapter belongs.
        chapter_title (str): Title of the chapter.
        word_count (int): Word count of the chapter.
        publication (int): Publication status of the chapter.
        sort (int): Sort order of the chapter.
        status (int): Current status of the chapter.
        deleted_at (datetime): Timestamp when the chapter was deleted (soft delete).
        
        gooddreamer_novel (relationship): Relationship to the `GooddreamerNovel` model.
        gooddreamer_user_chapter_progression (relationship): Relationship to the `GooddreamerUserChapterProgression` model.
        gooddreamer_user_chapter_admob (relationship): Relationship to the `GooddreamerUserChapterAdmob` model.
    """
    __tablename__ = 'gooddreamer_novel_chapter'
    __table_args__ = (
        ForeignKeyConstraint(['novel_id'], ['gooddreamer_novel.id']),
        {'schema': None}
    )
    __mapper_args__ = {
        'polymorphic_identity': 'gooddreamer_novel_chapter'
    }

    id = Column("id", Integer, primary_key=True, index=True)
    novel_id = Column("novel_id", Integer, ForeignKey("gooddreamer_novel.id"), index=True)
    chapter_title = Column("chapter_title", String)
    word_count = Column("word_count", Integer)
    publication = Column("publication", Integer)
    sort = Column("sort", Integer)
    status = Column("status", Integer)
    deleted_at = Column("deleted_at", DateTime)

    gooddreamer_novel = relationship(
        'GooddreamerNovel', 
        lazy=True, 
        back_populates='gooddreamer_novel_chapter', 
        viewonly=True
    )
    gooddreamer_user_chapter_progression = relationship(
        'GooddreamerUserChapterProgression', 
        lazy=True, 
        back_populates='gooddreamer_novel_chapter', 
        viewonly=True
    )
    gooddreamer_user_chapter_admob = relationship(
        'GooddreamerUserChapterAdmob', 
        lazy=True, 
        back_populates='gooddreamer_novel_chapter', 
        viewonly=True
    )


class GooddreamerUserChapterProgression(Base):
    """
    Model for representing the progression of a user through a chapter of a novel in the Gooddreamer application.
    
    This class defines the schema for the 'gooddreamer_user_chapter_progression' table in the database,
    along with the relationships to other tables. It tracks the user's progress through a specific chapter
    of a novel.
    
    Attributes:
        id (int): Primary key for the user chapter progression.
        user_id (int): Foreign key referencing the user.
        chapter_id (int): Foreign key referencing the chapter.
        created_at (datetime): Timestamp when the progression record was created.
        updated_at (datetime): Timestamp when the progression record was last updated.
        
        gooddreamer_user_data (relationship): Relationship to the `GooddreamerUserData` model.
        gooddreamer_novel_chapter (relationship): Relationship to the `GooddreamerNovelChapter` model.
        model_has_sources (relationship): Relationship to the `ModelHasSources` model.
    """
    __tablename__ = 'gooddreamer_user_chapter_progression'
    __table_args__ = (
        ForeignKeyConstraint(['user_id'], ['gooddreamer_user_data.id']),
        ForeignKeyConstraint(['chapter_id'], ['gooddreamer_novel_chapter.id']),
        {'schema': None}
    )
    __mapper_args__ = {
        'polymorphic_identity': 'gooddreamer_user_chapter_progression'
    }

    id = Column("id", Integer, primary_key=True, index=True)
    user_id = Column("user_id", Integer, ForeignKey("gooddreamer_user_data.id"), index=True)
    chapter_id = Column("chapter_id", Integer, ForeignKey("gooddreamer_novel_chapter.id"), index=True)
    is_completed = Column("is_completed", Boolean)
    created_at = Column("created_at", DateTime)
    updated_at = Column("updated_at", DateTime)

    gooddreamer_user_data = relationship(
        'GooddreamerUserData', 
        lazy=True, 
        back_populates='gooddreamer_user_chapter_progression', 
        viewonly=True
    )
    gooddreamer_novel_chapter = relationship(
        'GooddreamerNovelChapter', 
        lazy=True, 
        back_populates='gooddreamer_user_chapter_progression', 
        viewonly=True
    )
    model_has_sources = relationship(
        'ModelHasSources', 
        lazy=True, 
        back_populates='gooddreamer_user_chapter_progression', 
        viewonly=True
    )


class DataCategory(Base):
    """
    Model for representing a category of data in the Gooddreamer application.
    
    This class defines the schema for the 'data_category' table in the database.
    It includes fields for category metadata such as the category name and sets up
    the relationship with the `GooddreamerNovel` model.
    
    Attributes:
        id (int): Primary key for the category.
        category_name (str): Name of the category.
        
        gooddreamer_novel (relationship): Relationship to the `GooddreamerNovel` model.
    """
    __tablename__ = 'data_category'
    __mapper_args__ = {
        'polymorphic_identity': 'data_category'
    }

    id = Column("id", Integer, primary_key=True, index=True)
    category_name = Column("category_name", String)

    gooddreamer_novel = relationship(
        'GooddreamerNovel', 
        lazy=True, 
        back_populates='data_category', 
        viewonly=True
    )


class GooddreamerUserCollection(Base):
    """
    Model for representing a user's collection of novels in the Gooddreamer application.
    
    This class defines the schema for the 'gooddreamer_user_collection' table in the database,
    and it records which novels have been purchased or collected by a user, along with timestamps.
    
    Attributes:
        id (int): Primary key for the user collection.
        user_id (int): Foreign key referencing the user.
        novel_id (int): Foreign key referencing the novel.
        purchased_at (datetime): Timestamp when the novel was purchased.
        created_at (datetime): Timestamp when the collection entry was created.
    """
    __tablename__ = 'gooddreamer_user_collection'
    __table_args__ = (
        ForeignKeyConstraint(['user_id'], ['gooddreamer_user_data.id']),
        ForeignKeyConstraint(['novel_id'], ['gooddreamer_novel.id']),
        {'schema': None}
    )
    __mapper_args__ = {
        'polymorphic_identity': 'gooddreamer_user_collection'
    }

    id = Column("id", Integer, primary_key=True, index=True)
    user_id = Column("user_id", Integer, ForeignKey("gooddreamer_user_data.id"), index=True)
    novel_id = Column("novel_id", Integer, ForeignKey("gooddreamer_novel.id"), index=True)
    purchased_at  = Column("purchased_at", DateTime)
    created_at = Column("created_at", DateTime)

    gooddreamer_user_data = relationship(
        'GooddreamerUserData', 
        lazy=True, 
        back_populates='gooddreamer_user_collection', 
        viewonly=True
    )
    gooddreamer_novel = relationship(
        'GooddreamerNovel', 
        lazy=True, 
        back_populates='gooddreamer_user_collection', 
        viewonly=True
    )


class GooddreamerChapterTransaction(Base):
    """
    Model for representing chapter transactions in the Gooddreamer application.
    
    This class defines the schema for the 'gooddreamer_chapter_transaction' table in the database,
    and it records transactions related to specific chapters in a novel, including user details,
    timestamps, and the number of chapters involved.
    
    Attributes:
        id (int): Primary key for the chapter transaction.
        user_id (int): Foreign key referencing the user who made the transaction.
        novel_id (int): Foreign key referencing the novel involved in the transaction.
        created_at (datetime): Timestamp when the transaction was created.
        chapter_count (int): Number of chapters involved in the transaction.
        
        gooddreamer_user_data (relationship): Relationship to the `GooddreamerUserData` model.
        gooddreamer_novel (relationship): Relationship to the `GooddreamerNovel` model.
        gooddreamer_user_wallet_item (relationship): Relationship to the `GooddreamerUserWalletItem` model.
        model_has_sources (relationship): Relationship to the `ModelHasSources` model.
    """
    __tablename__ = 'gooddreamer_chapter_transaction'
    __table_args__ = (
        ForeignKeyConstraint(['user_id'], ['gooddreamer_user_data.id']),
        ForeignKeyConstraint(['novel_id'], ['gooddreamer_novel.id']),
        {'schema': None}
    )
    __mapper_args__ = {
        'polymorphic_identity': 'gooddreamer_chapter_transaction'
    }

    id  = Column("id", Integer, primary_key=True, index=True)
    user_id = Column("user_id", Integer, ForeignKey("gooddreamer_user_data.id"), index=True)
    novel_id = Column("novel_id", Integer, ForeignKey("gooddreamer_novel.id"), index=True)
    created_at = Column("created_at", DateTime)
    chapter_count = Column("chapter_count", Integer)

    gooddreamer_user_data = relationship(
        'GooddreamerUserData', 
        lazy=True, 
        back_populates='gooddreamer_chapter_transaction', 
        viewonly=True
    )
    gooddreamer_novel = relationship(
        'GooddreamerNovel', 
        lazy=True, 
        back_populates='gooddreamer_chapter_transaction', 
        viewonly=True
    )
    gooddreamer_user_wallet_item = relationship(
        'GooddreamerUserWalletItem', 
        lazy=True, 
        back_populates='gooddreamer_chapter_transaction', 
        viewonly=True
    )
    model_has_sources = relationship(
        'ModelHasSources', 
        lazy=True, 
        back_populates='gooddreamer_chapter_transaction', 
        viewonly=True
    )


class GooddreamerUserChapterAdmob(Base):
    """
    Model for representing user interactions with chapter advertisements in the Gooddreamer application.
    
    This class defines the schema for the 'gooddreamer_user_chapter_admob' table in the database,
    and it records details about users viewing ads related to specific chapters.
    
    Attributes:
        id (int): Primary key for the user chapter admob entry.
        user_id (int): Foreign key referencing the user.
        chapter_id (int): Foreign key referencing the novel chapter.
        slug (str): Slug identifier for the ad.
        created_at (datetime): Timestamp when the ad interaction was created.
        
        gooddreamer_user_data (relationship): Relationship to the `GooddreamerUserData` model.
        gooddreamer_novel_chapter (relationship): Relationship to the `GooddreamerNovelChapter` model.
        model_has_sources (relationship): Relationship to the `ModelHasSources` model.
    """
    __tablename__ = 'gooddreamer_user_chapter_admob'
    __table_args__ = (
        ForeignKeyConstraint(['user_id'], ['gooddreamer_user_data.id']),
        ForeignKeyConstraint(['chapter_id'], ['gooddreamer_novel_chapter.id']),
        {'schema': None}
    )
    __mapper_args__ = {
        'polymorphic_identity': 'gooddreamer_user_chapter_admob'
    }

    id = Column("id", Integer, primary_key=True, index=True)
    user_id = Column("user_id", Integer, ForeignKey("gooddreamer_user_data.id"), index=True)
    chapter_id = Column("chapter_id", Integer, ForeignKey("gooddreamer_novel_chapter.id"), index=True)
    slug = Column("slug", String)
    created_at = Column("created_at", DateTime)

    gooddreamer_user_data = relationship(
        'GooddreamerUserData', 
        lazy=True, 
        back_populates='gooddreamer_user_chapter_admob', 
        viewonly=True
    )
    gooddreamer_novel_chapter = relationship(
        'GooddreamerNovelChapter', 
        lazy=True, 
        back_populates='gooddreamer_user_chapter_admob', 
        viewonly=True
    )
    model_has_sources = relationship(
        'ModelHasSources', 
        lazy=True, 
        back_populates='gooddreamer_user_chapter_admob', 
        viewonly=True
    )


class GooddreamerUserFavorite(Base):
    """
    Model for representing the association between models and sources in the Gooddreamer application.
    
    This class defines the schema for the 'gooddreamer_user_favorite' table in the database,
    and it creates a many-to-many relationship between different models and sources.
    
    Attributes:
        novel_id (int): Primary key for the model reference.
        user_id (int): Foreign key referencing the source.
        
        gooddreamer_novel (relationship): Relationship to the `GooddreamerNovel` model.
        gooddreamer_user_data (relationship): Relationship to the `GooddreamerUserData` model.
    """
    
    __tablename__ = 'gooddreamer_user_favorite'
    __table_args__ = (
        ForeignKeyConstraint(['novel_id'], ['gooddreamer_novel.id']),
        ForeignKeyConstraint(['user_id'], ['gooddreamer_user_data.id']),
        {'schema': None}
    )
    __mapper_args__ = {
        'polymorphic_identity':'gooddreamer_user_favorite'
    }
    
    novel_id = Column(
        'novel_id', 
        Integer,
        ForeignKey('gooddreamer_novel.id'),
        index=True,
        primary_key=True
    )
    user_id = Column('user_id', Integer, ForeignKey('gooddreamer_user_data.id'), index=True)
    
    gooddreamer_novel = relationship(
        'GooddreamerNovel', 
        lazy=True, 
        back_populates='gooddreamer_user_favorite', 
        viewonly=True
    )
    gooddreamer_user_data = relationship(
        'GooddreamerUserData', 
        lazy=True, 
        back_populates='gooddreamer_user_favorite', 
        viewonly=True
    )
