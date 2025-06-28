#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для повного очищення всіх статусів територій
Видаляє всі поля, пов'язані зі статусами та історією
"""

import pymongo
from pymongo import MongoClient
from urllib.parse import quote_plus
from datetime import datetime, timezone
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

def clean_all_status_fields(client):
    """
    Повне очищення всіх полів, пов'язаних зі статусами
    """
    db = client[DATABASE_NAME]
    
    collections = [
        'level1_regions',
        'level2_raions', 
        'level3_hromadas',
        'level4_settlements',
        'level_additional_city_districts'
    ]
    
    # Поля для видалення
    fields_to_remove = [
        # Поточні статуси
        "current_status",
        "status_start_date", 
        "status_end_date",
        "last_status_update",
        
        # Окупація
        "current_occupation_status",
        "occupation_start_date",
        "occupation_end_date", 
        "last_occupation_update",
        "occupation_history",
        
        # Бойові дії
        "combat_history",
        
        # Загальна історія статусів
        "status_history"
    ]
    
    total_processed = 0
    total_updated = 0
    total_errors = 0
    
    print("\n🧹 ПОЧИНАЮ ПОВНЕ ОЧИЩЕННЯ ВСІХ СТАТУСІВ...")
    print("-" * 60)
    print("🗑️  Видаляю наступні поля:")
    for field in fields_to_remove:
        print(f"   • {field}")
    print("-" * 60)
    
    for collection_name in collections:
        print(f"\n📊 Обробляю колекцію: {collection_name}")
        
        collection = db[collection_name]
        
        # Знаходимо всі території, які мають хоча б одне поле статусу
        territories = collection.find({
            "$or": [
                {"current_status": {"$exists": True}},
                {"status_start_date": {"$exists": True}},
                {"status_end_date": {"$exists": True}},
                {"last_status_update": {"$exists": True}},
                {"current_occupation_status": {"$exists": True}},
                {"occupation_start_date": {"$exists": True}},
                {"occupation_end_date": {"$exists": True}},
                {"last_occupation_update": {"$exists": True}},
                {"occupation_history": {"$exists": True}},
                {"combat_history": {"$exists": True}},
                {"status_history": {"$exists": True}}
            ]
        })
        
        collection_processed = 0
        collection_updated = 0
        collection_errors = 0
        
        for territory in territories:
            total_processed += 1
            collection_processed += 1
            
            try:
                # Підготовуємо дані для видалення
                unset_data = {}
                for field in fields_to_remove:
                    if field in territory:
                        unset_data[field] = ""
                
                if unset_data:
                    # Виконуємо видалення полів
                    result = collection.update_one(
                        {"_id": territory["_id"]}, 
                        {"$unset": unset_data}
                    )
                    
                    if result.modified_count > 0:
                        total_updated += 1
                        collection_updated += 1
                        print(f"  ✅ {territory['name']}: видалено {len(unset_data)} полів")
                    else:
                        print(f"  ⚠️  {territory['name']}: немає змін")
                else:
                    print(f"  ℹ️  {territory['name']}: немає полів для видалення")
                
            except Exception as e:
                print(f"  ❌ Помилка обробки {territory.get('name', 'Невідома територія')}: {e}")
                total_errors += 1
                collection_errors += 1
        
        print(f"  📊 {collection_name}: оброблено {collection_processed}, оновлено {collection_updated}, помилок {collection_errors}")
    
    print(f"\n🎯 ПІДСУМКИ ОЧИЩЕННЯ:")
    print(f"  Загалом оброблено: {total_processed}")
    print(f"  Оновлено: {total_updated}")
    print(f"  Помилок: {total_errors}")
    
    return total_processed, total_updated, total_errors

def verify_cleanup(client):
    """
    Перевірка, що всі поля статусів були видалені
    """
    db = client[DATABASE_NAME]
    
    collections = [
        'level1_regions',
        'level2_raions', 
        'level3_hromadas',
        'level4_settlements',
        'level_additional_city_districts'
    ]
    
    print("\n🔍 ПЕРЕВІРКА ОЧИЩЕННЯ:")
    print("-" * 40)
    
    fields_to_check = [
        "current_status",
        "status_start_date", 
        "status_end_date",
        "last_status_update",
        "current_occupation_status",
        "occupation_start_date",
        "occupation_end_date", 
        "last_occupation_update",
        "occupation_history",
        "combat_history",
        "status_history"
    ]
    
    total_remaining = 0
    
    for collection_name in collections:
        collection = db[collection_name]
        
        # Перевіряємо кожне поле
        for field in fields_to_check:
            count = collection.count_documents({field: {"$exists": True}})
            if count > 0:
                print(f"  ⚠️  {collection_name}.{field}: {count} записів")
                total_remaining += count
        
        if total_remaining == 0:
            print(f"  ✅ {collection_name}: всі поля статусів видалені")
    
    if total_remaining == 0:
        print("\n✅ ВСІ ПОЛЯ СТАТУСІВ УСПІШНО ВИДАЛЕНІ!")
    else:
        print(f"\n⚠️  Залишилося {total_remaining} записів з полями статусів")
    
    return total_remaining

def show_collection_statistics(client):
    """
    Показ статистики колекцій після очищення
    """
    db = client[DATABASE_NAME]
    
    collections = [
        'level1_regions',
        'level2_raions', 
        'level3_hromadas',
        'level4_settlements',
        'level_additional_city_districts'
    ]
    
    print("\n📈 СТАТИСТИКА КОЛЕКЦІЙ ПІСЛЯ ОЧИЩЕННЯ:")
    print("-" * 50)
    
    total_territories = 0
    
    for collection_name in collections:
        collection = db[collection_name]
        count = collection.count_documents({})
        total_territories += count
        print(f"  {collection_name}: {count} територій")
    
    print(f"\n📊 ЗАГАЛЬНА СТАТИСТИКА:")
    print(f"  Загалом територій: {total_territories}")

def main():
    """Головна функція"""
    print("🗑️  СКРИПТ ПОВНОГО ОЧИЩЕННЯ СТАТУСІВ")
    print("=" * 60)
    print("⚠️  УВАГА: Цей скрипт видалить ВСІ поля, пов'язані зі статусами!")
    print("=" * 60)
    
    # Запитуємо підтвердження
    confirm = input("\n🤔 Ви впевнені, що хочете видалити всі статуси? (yes/no): ").strip().lower()
    
    if confirm != 'yes':
        print("❌ Операцію скасовано")
        return
    
    # Підключаємося до MongoDB
    client = connect_to_mongodb()
    if not client:
        return
    
    try:
        # Очищуємо всі поля статусів
        processed, updated, errors = clean_all_status_fields(client)
        
        if errors == 0:
            print("\n✅ Очищення завершено успішно!")
            
            # Перевіряємо результат
            remaining = verify_cleanup(client)
            
            if remaining == 0:
                print("\n🎉 БАЗА ДАНИХ ГОТОВА ДЛЯ НОВОГО ІМПОРТУ!")
                
                # Показуємо статистику
                show_collection_statistics(client)
            else:
                print(f"\n⚠️  Залишилося {remaining} записів з полями статусів")
        else:
            print(f"\n⚠️  Очищення завершено з {errors} помилками")
            
    except Exception as e:
        print(f"\n❌ Помилка виконання: {e}")
    
    finally:
        client.close()
        print("\n🔌 З'єднання з MongoDB закрито")

if __name__ == "__main__":
    main() 