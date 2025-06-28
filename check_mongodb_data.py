#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для перевірки даних в MongoDB Atlas
Автор: AI Assistant
Дата: 2025
"""

import pymongo
from pymongo import MongoClient
import sys
from urllib.parse import quote_plus
import os

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
        sys.exit(1)

def check_collection_data(db, collection_name):
    """Перевірка даних в колекції"""
    collection = db[collection_name]
    total_count = collection.count_documents({})
    
    print(f"\n📊 Колекція: {collection_name}")
    print(f"   Всього записів: {total_count}")
    
    if total_count > 0:
        # Показуємо перші 5 записів
        print("   Перші 5 записів:")
        for doc in collection.find().limit(5):
            print(f"     • {doc['name']} ({doc['_id']}) - категорія: {doc['category']}")
        
        # Статистика по категоріях
        pipeline = [
            {"$group": {"_id": "$category", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        
        category_stats = list(collection.aggregate(pipeline))
        print("   Статистика по категоріях:")
        for stat in category_stats:
            print(f"     • {stat['_id']}: {stat['count']} записів")
    
    return total_count

def main():
    """Головна функція"""
    print("=" * 60)
    print("🔍 ПЕРЕВІРКА ДАНИХ В MONGODB ATLAS")
    print("=" * 60)
    
    # Підключаємося до MongoDB
    client = connect_to_mongodb()
    db = client[DATABASE_NAME]
    
    # Список колекцій для перевірки
    collections = [
        'level1_regions',      # Області та АРК
        'level2_raions',       # Райони
        'level3_hromadas',     # Громади
        'level4_settlements',  # Населені пункти
        'level_additional_city_districts'  # Міські райони
    ]
    
    total_records = 0
    
    for collection_name in collections:
        count = check_collection_data(db, collection_name)
        total_records += count
    
    print(f"\n📈 ЗАГАЛЬНА СТАТИСТИКА:")
    print(f"   Всього записів в базі: {total_records}")
    
    # Перевіряємо, чи є записи з окупацією
    print(f"\n🎯 ПЕРЕВІРКА ДАНИХ ПРО ОКУПАЦІЮ:")
    for collection_name in collections:
        collection = db[collection_name]
        occupation_count = collection.count_documents({"occupation_history": {"$exists": True}})
        if occupation_count > 0:
            print(f"   {collection_name}: {occupation_count} записів з даними про окупацію")
    
    # Закриваємо з'єднання
    client.close()
    print("\n🔌 З'єднання з MongoDB закрито")
    print("=" * 60)
    print("🎉 Перевірка завершена!")

def check_database_structure():
    """Check the structure of territory data in MongoDB"""
    print("🔍 ПЕРЕВІРКА СТРУКТУРИ БАЗИ ДАНИХ MONGODB")
    print("=" * 50)
    
    try:
        client = MongoClient(MONGO_CONNECTION_STRING)
        db = client.katottg
        
        # Check collections
        collections = ['level1_regions', 'level2_raions', 'level3_hromadas', 'level4_settlements']
        
        for collection_name in collections:
            print(f"\n=== {collection_name.upper()} ===")
            collection = db[collection_name]
            count = collection.count_documents({})
            print(f"Кількість документів: {count}")
            
            if count > 0:
                # Get sample documents
                sample = list(collection.find({}, {'code': 1, 'name': 1, '_id': 0}).limit(3))
                for i, item in enumerate(sample, 1):
                    code = item.get('code', 'N/A')
                    name = item.get('name', 'N/A')
                    print(f"  {i}. Code: {code} | Name: {name}")
                    
                    # Check code length
                    if code != 'N/A':
                        print(f"     Довжина коду: {len(code)} символів")
        
        # Check if there are any territories with the new status fields
        print(f"\n=== ПЕРЕВІРКА СТАТУСІВ ===")
        for collection_name in collections:
            collection = db[collection_name]
            with_status = collection.count_documents({"status_periods": {"$exists": True, "$ne": []}})
            print(f"{collection_name}: {with_status} територій зі статусами")
        
        client.close()
        
    except Exception as e:
        print(f"❌ Помилка підключення до MongoDB: {e}")

def check_specific_codes():
    """Check if specific codes from the document exist in the database"""
    print(f"\n🔍 ПЕРЕВІРКА КОНКРЕТНИХ КОДІВ З ДОКУМЕНТА")
    print("=" * 50)
    
    # Sample codes from the document
    test_codes = [
        "UA12060090000074553",  # Грушівська сільська територіальна громада
        "UA12060130000012028",  # Зеленодольська міська територіальна громада
        "UA12080010000029838",  # Марганецька міська територіальна громада
        "UA01000000000013043",  # Автономна Республіка Крим
        "UA14140000000070889",  # Маріупольський район
    ]
    
    try:
        client = MongoClient(MONGO_CONNECTION_STRING)
        db = client.katottg
        
        collections = ['level1_regions', 'level2_raions', 'level3_hromadas', 'level4_settlements']
        
        for code in test_codes:
            print(f"\n🔍 Пошук коду: {code}")
            found = False
            
            for collection_name in collections:
                collection = db[collection_name]
                result = collection.find_one({"code": code})
                
                if result:
                    print(f"  ✅ Знайдено в {collection_name}")
                    print(f"     Назва: {result.get('name', 'N/A')}")
                    found = True
                    break
            
            if not found:
                print(f"  ❌ Не знайдено в жодній колекції")
                
                # Try to find similar codes
                print(f"  🔍 Пошук подібних кодів...")
                for collection_name in collections:
                    collection = db[collection_name]
                    # Search for codes that start with the same pattern
                    pattern = code[:8]  # First 8 characters
                    similar = list(collection.find({"code": {"$regex": f"^{pattern}"}}, 
                                                  {"code": 1, "name": 1, "_id": 0}).limit(2))
                    if similar:
                        print(f"    {collection_name}: {[item['code'] for item in similar]}")
        
        client.close()
        
    except Exception as e:
        print(f"❌ Помилка: {e}")

if __name__ == "__main__":
    main()
    check_database_structure()
    check_specific_codes() 