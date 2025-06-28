#!/usr/bin/env python3
"""
Script to analyze the actual database structure and territory codes
"""

import sys
from urllib.parse import quote_plus
from pymongo import MongoClient

# РЯДОК ПІДКЛЮЧЕННЯ ДО MONGODB ATLAS
username = quote_plus("test")
password = quote_plus("test")
MONGO_CONNECTION_STRING = f"mongodb+srv://{username}:{password}@clusterrd4u.7180sy4.mongodb.net/?retryWrites=true&w=majority&appName=ClusterRD4U"

# Назва бази даних
DATABASE_NAME = "ua_admin_territory"

def analyze_database_structure():
    """Analyze the actual database structure"""
    print("🔍 АНАЛІЗ СТРУКТУРИ БАЗИ ДАНИХ")
    print("=" * 50)
    
    try:
        client = MongoClient(MONGO_CONNECTION_STRING)
        db = client[DATABASE_NAME]
        
        # Get all collection names
        collections = db.list_collection_names()
        print(f"📋 Доступні колекції: {collections}")
        
        # Analyze each collection
        for collection_name in collections:
            print(f"\n=== {collection_name.upper()} ===")
            collection = db[collection_name]
            count = collection.count_documents({})
            print(f"Кількість документів: {count}")
            
            if count > 0:
                # Get a sample document to understand structure
                sample = collection.find_one()
                if sample:
                    print("📄 Структура документа:")
                    for key, value in sample.items():
                        if key == '_id':
                            continue
                        if isinstance(value, str) and len(value) > 100:
                            print(f"  {key}: {value[:100]}...")
                        else:
                            print(f"  {key}: {value}")
                
                # Check for code field specifically
                if 'code' in sample:
                    # Get sample codes
                    codes = list(collection.find({}, {'code': 1, '_id': 0}).limit(5))
                    print("🔢 Приклади кодів:")
                    for i, doc in enumerate(codes, 1):
                        code = doc.get('code', 'N/A')
                        print(f"  {i}. {code} (довжина: {len(code)})")
        
        client.close()
        
    except Exception as e:
        print(f"❌ Помилка: {e}")

def check_document_codes_in_database():
    """Check if document codes exist in the actual database collections"""
    print(f"\n🔍 ПЕРЕВІРКА КОДІВ З ДОКУМЕНТА В БАЗІ ДАНИХ")
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
        db = client[DATABASE_NAME]
        
        # Get all collection names
        collections = db.list_collection_names()
        
        for code in test_codes:
            print(f"\n🔍 Пошук коду: {code}")
            found = False
            
            for collection_name in collections:
                collection = db[collection_name]
                result = collection.find_one({"code": code})
                
                if result:
                    print(f"  ✅ Знайдено в {collection_name}")
                    print(f"     Назва: {result.get('name', 'N/A')}")
                    print(f"     Категорія: {result.get('category', 'N/A')}")
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
                                                  {"code": 1, "name": 1, "_id": 0}).limit(3))
                    if similar:
                        print(f"    {collection_name}:")
                        for item in similar:
                            print(f"      {item['code']} - {item['name']}")
        
        client.close()
        
    except Exception as e:
        print(f"❌ Помилка: {e}")

def check_import_script_collections():
    """Check what collections the import script is actually using"""
    print(f"\n🔍 ПЕРЕВІРКА КОЛЕКЦІЙ В СКРИПТІ ІМПОРТУ")
    print("=" * 50)
    
    # The collections that the import script is trying to use
    import_collections = ['level1_regions', 'level2_raions', 'level3_hromadas', 'level4_settlements']
    
    try:
        client = MongoClient(MONGO_CONNECTION_STRING)
        db = client[DATABASE_NAME]
        
        for collection_name in import_collections:
            print(f"\n📋 {collection_name}:")
            collection = db[collection_name]
            count = collection.count_documents({})
            print(f"  Кількість документів: {count}")
            
            if count > 0:
                sample = collection.find_one()
                if sample:
                    print(f"  Приклад документа: {list(sample.keys())}")
        
        client.close()
        
    except Exception as e:
        print(f"❌ Помилка: {e}")

if __name__ == "__main__":
    analyze_database_structure()
    check_document_codes_in_database()
    check_import_script_collections() 