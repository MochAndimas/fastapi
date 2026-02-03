from pydantic import BaseModel
from typing import Any
import datetime


class NovelData(BaseModel):
    """Schemas for fetch user novel data"""
    
    novel_table: str

    class Config:
        from_attributes = True

class NovelDetailsData(BaseModel):
    """Schemas for fetch user novel details data"""

    id_novel: int
    judul_novel: str
    category: str
    total_bab: int
    total_favorite: int
    belum_terbit: int
    bab_terbit: int
    tanggal_terbit: Any
    status: str
    last_updated: Any
    nama_pena: str
    nama_penulis: str
    gender: str
    email: str
    no_tlp: str
    alamat: str
    total_pembaca_unique: int
    guest_pembaca_unique: int
    regis_pembaca_unique: int
    total_pembaca_count: int
    guest_pembaca_count: int
    regis_pembaca_count: int
    chapter_coin_unique: int
    chapter_adscoin_unique: int
    chapter_ads_unique: int
    chapter_coin_count: int
    chapter_adscoin_count: int
    chapter_ads_count: int
    total_chapter_unique: int
    total_chapter_count: int


class NovelDetailsChartData(BaseModel):
    """Schemas for fetch user novel details chart data"""

    user_table_pembaca: str
    user_table_chapter_coin: str
    user_table_chapter_adscoin: str
    user_table_chapter_ads: str
    frequency_table: str
    frequency_chart: str
