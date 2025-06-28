#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Інтерактивний менеджер для роботи з даними про окупацію територій
Автор: AI Assistant
Дата: 2025
"""

import pandas as pd
import pymongo
from pymongo import MongoClient
import sys
import os
from urllib.parse import quote_plus
from dateutil import parser
from datetime import datetime, date
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
        sys.exit(1)

def get_occupation_status_on_date(client, query_date):
    """
    Отримання статусу окупації на конкретну дату
    """
    db = client[DATABASE_NAME]
    
    collections = [
        'level1_regions',
        'level2_raions', 
        'level3_hromadas',
        'level4_settlements',
        'level_additional_city_districts'
    ]
    
    occupied_territories = []
    
    for collection_name in collections:
        collection = db[collection_name]
        
        # Шукаємо території, які були окуповані на вказану дату
        pipeline = [
            {
                "$match": {
                    "occupation_history": {"$exists": True}
                }
            },
            {
                "$addFields": {
                    "occupation_status_on_date": {
                        "$filter": {
                            "input": "$occupation_history",
                            "as": "period",
                            "cond": {
                                "$and": [
                                    {
                                        "$lte": [
                                            "$$period.start_date",
                                            query_date
                                        ]
                                    },
                                    {
                                        "$or": [
                                            {"$eq": ["$$period.end_date", None]},
                                            {"$gte": ["$$period.end_date", query_date]}
                                        ]
                                    }
                                ]
                            }
                        }
                    }
                }
            },
            {
                "$match": {
                    "occupation_status_on_date": {"$ne": []}
                }
            }
        ]
        
        results = list(collection.aggregate(pipeline))
        
        for result in results:
            occupied_territories.append({
                'name': result['name'],
                'category': result['category'],
                'collection': collection_name,
                'occupation_periods': result['occupation_status_on_date']
            })
    
    return occupied_territories

def get_territory_occupation_history(client, territory_name):
    """
    Отримання повної історії окупації для конкретної території
    """
    db = client[DATABASE_NAME]
    
    collections = [
        'level1_regions',
        'level2_raions', 
        'level3_hromadas',
        'level4_settlements',
        'level_additional_city_districts'
    ]
    
    for collection_name in collections:
        collection = db[collection_name]
        
        # Пошук за точним співпадінням назви
        result = collection.find_one({"name": territory_name})
        if not result:
            # Пошук за частковим співпадінням
            result = collection.find_one({"name": {"$regex": territory_name, "$options": "i"}})
        
        if result and 'occupation_history' in result:
            return result, collection_name
    
    return None, None

def add_occupation_period(client, territory_name, start_date, end_date=None, status="Тимчасово окупована"):
    """
    Додавання нового періоду окупації для території
    """
    db = client[DATABASE_NAME]
    
    # Шукаємо територію
    territory_doc, collection_name = get_territory_occupation_history(client, territory_name)
    
    if not territory_doc:
        print(f"❌ Територію '{territory_name}' не знайдено")
        return False
    
    collection = db[collection_name]
    
    # Створюємо запис про окупацію
    occupation_record = {
        'start_date': start_date,
        'end_date': end_date,
        'status': status,
        'updated_at': datetime.now()
    }
    
    # Отримуємо поточну історію окупації
    if 'occupation_history' in territory_doc:
        occupation_history = territory_doc['occupation_history']
    else:
        occupation_history = []
    
    # Додаємо новий запис до історії
    occupation_history.append(occupation_record)
    
    # Оновлюємо документ
    update_data = {
        "$set": {
            "occupation_history": occupation_history,
            "current_occupation_status": status,
            "last_occupation_update": datetime.now()
        }
    }
    
    if start_date:
        update_data["$set"]["occupation_start_date"] = start_date
    
    if end_date:
        update_data["$set"]["occupation_end_date"] = end_date
    
    collection.update_one({"_id": territory_doc["_id"]}, update_data)
    
    print(f"✅ Додано період окупації для: {territory_doc['name']}")
    return True

def export_occupation_data_to_csv(client, output_file="occupation_data.csv"):
    """
    Експорт даних про окупацію в CSV файл
    """
    db = client[DATABASE_NAME]
    
    collections = [
        'level1_regions',
        'level2_raions', 
        'level3_hromadas',
        'level4_settlements',
        'level_additional_city_districts'
    ]
    
    all_data = []
    
    for collection_name in collections:
        collection = db[collection_name]
        
        # Знаходимо всі території з історією окупації
        territories = collection.find({"occupation_history": {"$exists": True}})
        
        for territory in territories:
            for period in territory['occupation_history']:
                all_data.append({
                    'territory_name': territory['name'],
                    'category': territory['category'],
                    'collection': collection_name,
                    'start_date': period['start_date'].strftime('%d.%m.%Y') if period['start_date'] else '',
                    'end_date': period['end_date'].strftime('%d.%m.%Y') if period['end_date'] else '',
                    'status': period['status'],
                    'updated_at': period['updated_at'].strftime('%d.%m.%Y %H:%M:%S') if 'updated_at' in period else ''
                })
    
    if all_data:
        df = pd.DataFrame(all_data)
        df.to_csv(output_file, index=False, encoding='utf-8')
        print(f"✅ Експортовано {len(all_data)} записів в {output_file}")
    else:
        print("❌ Немає даних для експорту")

def show_menu():
    """Показ головного меню"""
    print("\n" + "=" * 50)
    print("🏛️  МЕНЕДЖЕР ДАНИХ ПРО ОКУПАЦІЮ ТЕРИТОРІЙ")
    print("=" * 50)
    print("1. 🔍 Перевірити статус окупації на конкретну дату")
    print("2. 📋 Показати історію окупації території")
    print("3. ➕ Додати період окупації")
    print("4. 📊 Експорт даних в CSV")
    print("5. 📈 Статистика окупації")
    print("0. 🚪 Вихід")
    print("=" * 50)

def show_occupation_statistics(client):
    """
    Показ статистики окупації
    """
    db = client[DATABASE_NAME]
    
    collections = [
        'level1_regions',
        'level2_raions', 
        'level3_hromadas',
        'level4_settlements',
        'level_additional_city_districts'
    ]
    
    total_territories = 0
    occupied_territories = 0
    total_occupation_periods = 0
    
    print("\n📈 СТАТИСТИКА ОКУПАЦІЇ:")
    print("-" * 30)
    
    for collection_name in collections:
        collection = db[collection_name]
        
        # Загальна кількість територій
        total_in_collection = collection.count_documents({})
        total_territories += total_in_collection
        
        # Території з історією окупації
        occupied_in_collection = collection.count_documents({"occupation_history": {"$exists": True}})
        occupied_territories += occupied_in_collection
        
        # Підрахунок періодів окупації
        territories_with_history = collection.find({"occupation_history": {"$exists": True}})
        periods_in_collection = 0
        for territory in territories_with_history:
            periods_in_collection += len(territory['occupation_history'])
        total_occupation_periods += periods_in_collection
        
        print(f"{collection_name}:")
        print(f"  Загалом територій: {total_in_collection}")
        print(f"  З історією окупації: {occupied_in_collection}")
        print(f"  Періодів окупації: {periods_in_collection}")
        print()
    
    print(f"📊 ЗАГАЛЬНА СТАТИСТИКА:")
    print(f"  Загалом територій: {total_territories}")
    print(f"  З історією окупації: {occupied_territories}")
    print(f"  Загалом періодів окупації: {total_occupation_periods}")

def main():
    """Головна функція"""
    print("🚀 ІНТЕРАКТИВНИЙ МЕНЕДЖЕР ДАНИХ ПРО ОКУПАЦІЮ")
    
    # Підключаємося до MongoDB
    client = connect_to_mongodb()
    
    while True:
        show_menu()
        
        try:
            choice = input("Виберіть опцію (0-5): ").strip()
            
            if choice == "0":
                print("👋 До побачення!")
                break
                
            elif choice == "1":
                date_str = input("Введіть дату (формат: ДД.ММ.РРРР): ").strip()
                try:
                    query_date = parser.parse(date_str, dayfirst=True)
                    occupied_territories = get_occupation_status_on_date(client, query_date)
                    
                    print(f"\n🔍 Території, окуповані на {query_date.strftime('%d.%m.%Y')}:")
                    if occupied_territories:
                        for territory in occupied_territories:
                            print(f"  • {territory['name']} ({territory['category']})")
                            for period in territory['occupation_periods']:
                                start_str = period['start_date'].strftime('%d.%m.%Y') if period['start_date'] else 'н/д'
                                end_str = period['end_date'].strftime('%d.%m.%Y') if period['end_date'] else 'н/д'
                                print(f"    Період: {start_str} - {end_str} ({period['status']})")
                    else:
                        print("  Немає окупованих територій на цю дату")
                        
                except Exception as e:
                    print(f"❌ Помилка парсингу дати: {e}")
                    
            elif choice == "2":
                territory_name = input("Введіть назву території: ").strip()
                territory_doc, collection_name = get_territory_occupation_history(client, territory_name)
                
                if territory_doc:
                    print(f"\n📋 Історія окупації для '{territory_doc['name']}':")
                    for i, period in enumerate(territory_doc['occupation_history'], 1):
                        start_str = period['start_date'].strftime('%d.%m.%Y') if period['start_date'] else 'н/д'
                        end_str = period['end_date'].strftime('%d.%m.%Y') if period['end_date'] else 'н/д'
                        print(f"  {i}. {start_str} - {end_str} ({period['status']})")
                else:
                    print(f"❌ Територію '{territory_name}' не знайдено або немає історії окупації")
                    
            elif choice == "3":
                territory_name = input("Введіть назву території: ").strip()
                start_date_str = input("Введіть дату початку окупації (ДД.ММ.РРРР): ").strip()
                end_date_str = input("Введіть дату кінця окупації (ДД.ММ.РРРР, або Enter для пропуску): ").strip()
                status = input("Введіть статус (за замовчуванням 'Тимчасово окупована'): ").strip()
                
                if not status:
                    status = "Тимчасово окупована"
                
                try:
                    start_date = parser.parse(start_date_str, dayfirst=True)
                    end_date = None
                    if end_date_str:
                        end_date = parser.parse(end_date_str, dayfirst=True)
                    
                    add_occupation_period(client, territory_name, start_date, end_date, status)
                    
                except Exception as e:
                    print(f"❌ Помилка парсингу дати: {e}")
                    
            elif choice == "4":
                output_file = input("Введіть назву файлу для експорту (за замовчуванням 'occupation_data.csv'): ").strip()
                if not output_file:
                    output_file = "occupation_data.csv"
                export_occupation_data_to_csv(client, output_file)
                
            elif choice == "5":
                show_occupation_statistics(client)
                
            else:
                print("❌ Невірний вибір. Спробуйте ще раз.")
                
        except KeyboardInterrupt:
            print("\n👋 До побачення!")
            break
        except Exception as e:
            print(f"❌ Помилка: {e}")
    
    # Закриваємо з'єднання
    client.close()
    print("🔌 З'єднання з MongoDB закрито")

if __name__ == "__main__":
    main() 