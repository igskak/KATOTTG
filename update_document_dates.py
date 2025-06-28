#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для оновлення існуючих записів, додаючи читабельний формат дати документа
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

# Конфігурація документа
DOCUMENT_CONFIG = {
    'document_name': 'Перелік 07052025',
    'document_date': '07.05.2025',  # Читабельний формат дати
    'document_date_iso': '2025-05-07',  # ISO формат для системних потреб
    'document_description': 'Документ Перелік 07052025 від 7 травня 2025 року'
}

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

def update_document_dates(client):
    """
    Оновлення існуючих записів, додаючи читабельний формат дати документа
    """
    db = client[DATABASE_NAME]
    
    collections = [
        'level1_regions',
        'level2_raions', 
        'level3_hromadas',
        'level4_settlements',
        'level_additional_city_districts'
    ]
    
    print("\n🔄 ОНОВЛЕННЯ ДАТ ДОКУМЕНТІВ У БАЗІ ДАНИХ")
    print("=" * 60)
    print(f"📄 Документ: {DOCUMENT_CONFIG['document_name']}")
    print(f"📅 Нова дата документа: {DOCUMENT_CONFIG['document_date']}")
    print(f"📅 ISO дата документа: {DOCUMENT_CONFIG['document_date_iso']}")
    print("=" * 60)
    
    total_updated = 0
    total_errors = 0
    
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
        
        collection_updated = 0
        collection_errors = 0
        
        for territory in territories:
            try:
                needs_update = False
                update_data = {}
                
                # Перевіряємо кожне поле історії
                for history_field in ['occupation_history', 'combat_history', 'status_history']:
                    if history_field in territory and territory[history_field]:
                        updated_history = []
                        
                        for period in territory[history_field]:
                            updated_period = period.copy()
                            
                            # Додаємо читабельну дату, якщо її немає
                            if 'document_date' not in period or period['document_date'] != DOCUMENT_CONFIG['document_date']:
                                updated_period['document_date'] = DOCUMENT_CONFIG['document_date']
                                needs_update = True
                            
                            # Додаємо ISO дату, якщо її немає
                            if 'document_date_iso' not in period:
                                updated_period['document_date_iso'] = DOCUMENT_CONFIG['document_date_iso']
                                needs_update = True
                            
                            # Оновлюємо джерело документа, якщо потрібно
                            if 'source_document' not in period or period['source_document'] != DOCUMENT_CONFIG['document_name']:
                                updated_period['source_document'] = DOCUMENT_CONFIG['document_name']
                                needs_update = True
                            
                            updated_history.append(updated_period)
                        
                        if needs_update:
                            update_data[history_field] = updated_history
                
                # Оновлюємо документ, якщо є зміни
                if needs_update:
                    result = collection.update_one(
                        {"_id": territory["_id"]},
                        {"$set": update_data}
                    )
                    
                    if result.modified_count > 0:
                        collection_updated += 1
                        total_updated += 1
                        print(f"  ✅ Оновлено: {territory['name']}")
                    else:
                        collection_errors += 1
                        total_errors += 1
                        print(f"  ❌ Помилка оновлення: {territory['name']}")
                
            except Exception as e:
                collection_errors += 1
                total_errors += 1
                print(f"  ❌ Помилка обробки {territory.get('name', 'невідома територія')}: {e}")
        
        print(f"  📊 Оновлено: {collection_updated}, помилок: {collection_errors}")
    
    print(f"\n📈 ЗАГАЛЬНА СТАТИСТИКА ОНОВЛЕННЯ:")
    print("-" * 40)
    print(f"  ✅ Успішно оновлено: {total_updated}")
    print(f"  ❌ Помилок: {total_errors}")
    
    return total_updated, total_errors

def verify_updates(client):
    """
    Перевірка результатів оновлення
    """
    db = client[DATABASE_NAME]
    
    collections = [
        'level1_regions',
        'level2_raions', 
        'level3_hromadas',
        'level4_settlements',
        'level_additional_city_districts'
    ]
    
    print(f"\n🔍 ПЕРЕВІРКА РЕЗУЛЬТАТІВ ОНОВЛЕННЯ:")
    print("=" * 60)
    
    total_with_readable_dates = 0
    total_without_readable_dates = 0
    
    for collection_name in collections:
        collection = db[collection_name]
        
        # Знаходимо території з історією статусів
        territories = collection.find({
            "$or": [
                {"occupation_history": {"$exists": True}},
                {"combat_history": {"$exists": True}},
                {"status_history": {"$exists": True}}
            ]
        })
        
        collection_with_readable = 0
        collection_without_readable = 0
        
        for territory in territories:
            has_readable_dates = False
            
            for history_field in ['occupation_history', 'combat_history', 'status_history']:
                if history_field in territory:
                    for period in territory[history_field]:
                        if 'document_date' in period and period['document_date'] == DOCUMENT_CONFIG['document_date']:
                            has_readable_dates = True
                            break
                    if has_readable_dates:
                        break
            
            if has_readable_dates:
                collection_with_readable += 1
            else:
                collection_without_readable += 1
        
        total_with_readable_dates += collection_with_readable
        total_without_readable_dates += collection_without_readable
        
        print(f"  {collection_name}: {collection_with_readable} з читабельними датами, {collection_without_readable} без")
    
    print(f"\n📊 ПІДСУМОК ПЕРЕВІРКИ:")
    print("-" * 40)
    print(f"  ✅ З читабельними датами: {total_with_readable_dates}")
    print(f"  ❌ Без читабельних дат: {total_without_readable_dates}")
    
    if total_without_readable_dates == 0:
        print(f"  🎉 Всі записи успішно оновлені!")
    else:
        print(f"  ⚠️  Потрібно додатково оновити {total_without_readable_dates} записів")

def main():
    """Головна функція"""
    print("🔄 ОНОВЛЕННЯ ДАТ ДОКУМЕНТІВ У БАЗІ ДАНИХ")
    print("=" * 60)
    
    # Підключаємося до MongoDB
    client = connect_to_mongodb()
    if not client:
        return
    
    try:
        # Підтвердження оновлення
        print(f"\n🤔 Підтвердження оновлення:")
        print(f"  📄 Документ: {DOCUMENT_CONFIG['document_name']}")
        print(f"  📅 Нова дата: {DOCUMENT_CONFIG['document_date']}")
        print(f"  📅 ISO дата: {DOCUMENT_CONFIG['document_date_iso']}")
        
        confirm = input("\n🤔 Продовжити оновлення? (y/N): ").strip().lower()
        if confirm != 'y':
            print("❌ Оновлення скасовано")
            return
        
        # Оновлюємо дати документів
        updated, errors = update_document_dates(client)
        
        # Перевіряємо результати
        verify_updates(client)
        
        print(f"\n✅ Оновлення завершено!")
        print(f"📊 Оновлено записів: {updated}")
        print(f"❌ Помилок: {errors}")
        
    except Exception as e:
        print(f"\n❌ Помилка: {e}")
    
    finally:
        client.close()
        print("\n🔌 З'єднання з MongoDB закрито")

if __name__ == "__main__":
    main() 