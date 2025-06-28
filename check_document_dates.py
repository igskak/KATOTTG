#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для перевірки та відображення дат документів у базі даних
Автор: AI Assistant
Дата: 2025
"""

import pymongo
from pymongo import MongoClient
from urllib.parse import quote_plus
from datetime import datetime
import json

# РЯДОК ПІДКЛЮЧЕННЯ ДО MONGODB ATLAS
username = quote_plus("test")
password = quote_plus("test")
MONGO_CONNECTION_STRING = f"mongodb+srv://{username}:{password}@clusterrd4u.7180sy4.mongodb.net/?retryWrites=true&w=majority&appName=ClusterRD4U"

# Назва бази даних
DATABASE_NAME = "ua_admin_territory"

def connect_to_mongodb():
    """Підключення до MongoDB Atlas"""
    try:
        print("🔌 Підключаюся до MongoDB Atlas...")
        client = MongoClient(MONGO_CONNECTION_STRING)
        client.admin.command('ping')
        print("✅ Успішно підключено до MongoDB Atlas")
        return client
    except Exception as e:
        print(f"❌ Помилка підключення до MongoDB: {e}")
        return None

def check_document_dates(client):
    """
    Перевірка та відображення дат документів у базі даних
    """
    db = client[DATABASE_NAME]
    
    collections = [
        'level1_regions',
        'level2_raions', 
        'level3_hromadas',
        'level4_settlements',
        'level_additional_city_districts'
    ]
    
    print("\n📅 ПЕРЕВІРКА ДАТ ДОКУМЕНТІВ У БАЗІ ДАНИХ")
    print("=" * 60)
    
    all_document_dates = set()
    territories_with_dates = 0
    territories_without_dates = 0
    status_records_with_dates = 0
    status_records_without_dates = 0
    
    for collection_name in collections:
        print(f"\n📊 Колекція: {collection_name}")
        print("-" * 40)
        
        collection = db[collection_name]
        
        # Знаходимо території з історією статусів
        territories = collection.find({
            "$or": [
                {"occupation_history": {"$exists": True}},
                {"combat_history": {"$exists": True}},
                {"status_history": {"$exists": True}}
            ]
        })
        
        collection_territories_with_dates = 0
        collection_territories_without_dates = 0
        collection_records_with_dates = 0
        collection_records_without_dates = 0
        
        for territory in territories:
            has_dates = False
            
            for history_field in ['occupation_history', 'combat_history', 'status_history']:
                if history_field in territory:
                    for period in territory[history_field]:
                        if 'document_date' in period:
                            all_document_dates.add(period['document_date'])
                            collection_records_with_dates += 1
                            has_dates = True
                        else:
                            collection_records_without_dates += 1
            
            if has_dates:
                collection_territories_with_dates += 1
            else:
                collection_territories_without_dates += 1
        
        territories_with_dates += collection_territories_with_dates
        territories_without_dates += collection_territories_without_dates
        status_records_with_dates += collection_records_with_dates
        status_records_without_dates += collection_records_without_dates
        
        print(f"  Територій з датами документів: {collection_territories_with_dates}")
        print(f"  Територій без дат документів: {collection_territories_without_dates}")
        print(f"  Записів статусів з датами: {collection_records_with_dates}")
        print(f"  Записів статусів без дат: {collection_records_without_dates}")
    
    print(f"\n📈 ЗАГАЛЬНА СТАТИСТИКА:")
    print("-" * 40)
    print(f"  Територій з датами документів: {territories_with_dates}")
    print(f"  Територій без дат документів: {territories_without_dates}")
    print(f"  Записів статусів з датами: {status_records_with_dates}")
    print(f"  Записів статусів без дат: {status_records_without_dates}")
    
    if all_document_dates:
        print(f"\n📅 ЗНАЙДЕНІ ДАТИ ДОКУМЕНТІВ:")
        print("-" * 40)
        for doc_date in sorted(all_document_dates):
            print(f"  • {doc_date}")
    else:
        print(f"\n⚠️  ДАТИ ДОКУМЕНТІВ НЕ ЗНАЙДЕНІ")
        print("-" * 40)
        print("  Жоден запис статусу не містить поле 'document_date'")
    
    return all_document_dates, territories_with_dates, territories_without_dates

def show_sample_records_with_dates(client):
    """
    Показ прикладів записів з датами документів
    """
    db = client[DATABASE_NAME]
    
    collections = [
        'level1_regions',
        'level2_raions', 
        'level3_hromadas',
        'level4_settlements',
        'level_additional_city_districts'
    ]
    
    print(f"\n📋 ПРИКЛАДИ ЗАПИСІВ З ДАТАМИ ДОКУМЕНТІВ:")
    print("=" * 60)
    
    sample_count = 0
    max_samples = 5
    
    for collection_name in collections:
        if sample_count >= max_samples:
            break
            
        collection = db[collection_name]
        
        # Знаходимо території з датами документів
        territories = collection.find({
            "$or": [
                {"occupation_history.document_date": {"$exists": True}},
                {"combat_history.document_date": {"$exists": True}},
                {"status_history.document_date": {"$exists": True}}
            ]
        }).limit(max_samples - sample_count)
        
        for territory in territories:
            if sample_count >= max_samples:
                break
                
            print(f"\n🏛️  Територія: {territory['name']} ({collection_name})")
            
            for history_field in ['occupation_history', 'combat_history', 'status_history']:
                if history_field in territory:
                    for period in territory[history_field]:
                        if 'document_date' in period:
                            print(f"  📄 {history_field}: {period['status']}")
                            print(f"     📅 Дата документа: {period['document_date']}")
                            if 'document_date_iso' in period:
                                print(f"     📅 Дата документа (ISO): {period['document_date_iso']}")
                            if 'source_document' in period:
                                print(f"     📋 Джерело: {period['source_document']}")
                            print()
                            sample_count += 1
                            break
                    if sample_count >= max_samples:
                        break

def main():
    """Головна функція"""
    print("🔍 ПЕРЕВІРКА ДАТ ДОКУМЕНТІВ У БАЗІ ДАНИХ")
    print("=" * 60)
    
    # Підключаємося до MongoDB
    client = connect_to_mongodb()
    if not client:
        return
    
    try:
        # Перевіряємо дати документів
        document_dates, with_dates, without_dates = check_document_dates(client)
        
        # Показуємо приклади записів
        if document_dates:
            show_sample_records_with_dates(client)
        
        # Рекомендації
        print(f"\n💡 РЕКОМЕНДАЦІЇ:")
        print("-" * 40)
        if without_dates > 0:
            print(f"  ⚠️  Знайдено {without_dates} територій без дат документів")
            print(f"     Рекомендується оновити їх, додавши поле 'document_date'")
        else:
            print(f"  ✅ Всі території мають дати документів")
        
        if document_dates:
            print(f"  📅 Використовуються дати: {', '.join(sorted(document_dates))}")
        
    except Exception as e:
        print(f"\n❌ Помилка: {e}")
    
    finally:
        client.close()
        print("\n🔌 З'єднання з MongoDB закрито")

if __name__ == "__main__":
    main() 