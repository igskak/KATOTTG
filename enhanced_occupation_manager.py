#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Розширений менеджер для роботи з даними про окупацію та бойові дії територій
Підтримує статуси з документа "Перелік 07052025"
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
from enum import Enum

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

class TerritoryStatus(Enum):
    """Статуси територій згідно з документом Перелік 07052025"""
    POSSIBLE_COMBAT = "1. Території можливих бойових дій"
    ACTIVE_COMBAT = "2. Території активних бойових дій"
    ACTIVE_COMBAT_WITH_RESOURCES = "3. Території активних бойових дій, на яких функціонують державні електронні інформаційні ресурси"
    TEMPORARILY_OCCUPIED = "Тимчасово окуповані території"

class DateColumnType(Enum):
    """Типи дата-колонок"""
    START_DATE = "Дата начала события"
    END_DATE = "Дата окончания события"

# Конфігурація дата-колонок для кожного статусу
STATUS_DATE_CONFIG = {
    TerritoryStatus.POSSIBLE_COMBAT: {
        'start_date_column': 'Дата виникнення можливості бойових дій',
        'end_date_column': 'Дата припинення можливості бойових дій*',
        'start_date_type': DateColumnType.START_DATE,
        'end_date_type': DateColumnType.END_DATE
    },
    TerritoryStatus.ACTIVE_COMBAT: {
        'start_date_column': 'Дата початку бойових дій',
        'end_date_column': 'Дата завершення бойових дій*',
        'start_date_type': DateColumnType.START_DATE,
        'end_date_type': DateColumnType.END_DATE
    },
    TerritoryStatus.ACTIVE_COMBAT_WITH_RESOURCES: {
        'start_date_column': 'Дата початку бойових дій',
        'end_date_column': 'Дата завершення бойових дій*',
        'start_date_type': DateColumnType.START_DATE,
        'end_date_type': DateColumnType.END_DATE
    },
    TerritoryStatus.TEMPORARILY_OCCUPIED: {
        'start_date_column': 'Дата початку тимчасової окупації',
        'end_date_column': 'Дата завершення тимчасової окупації*',
        'start_date_type': DateColumnType.START_DATE,
        'end_date_type': DateColumnType.END_DATE
    }
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
        sys.exit(1)

def get_territory_status_on_date(client, query_date, status_type=None):
    """
    Отримання статусу території на конкретну дату з підтримкою нових статусів
    """
    db = client[DATABASE_NAME]
    
    collections = [
        'level1_regions',
        'level2_raions', 
        'level3_hromadas',
        'level4_settlements',
        'level_additional_city_districts'
    ]
    
    territories_with_status = []
    
    for collection_name in collections:
        collection = db[collection_name]
        
        # Базовий pipeline для пошуку територій з історією
        pipeline = [
            {
                "$match": {
                    "$or": [
                        {"occupation_history": {"$exists": True}},
                        {"combat_history": {"$exists": True}},
                        {"status_history": {"$exists": True}}
                    ]
                }
            }
        ]
        
        # Додаємо фільтр по статусу, якщо вказано
        if status_type:
            pipeline.append({
                "$match": {
                    "$or": [
                        {"status_history.status": status_type.value},
                        {"occupation_history.status": status_type.value},
                        {"combat_history.status": status_type.value}
                    ]
                }
            })
        
        # Додаємо поля для аналізу статусу на конкретну дату
        pipeline.extend([
            {
                "$addFields": {
                    "status_on_date": {
                        "$concatArrays": [
                            {"$ifNull": ["$occupation_history", []]},
                            {"$ifNull": ["$combat_history", []]},
                            {"$ifNull": ["$status_history", []]}
                        ]
                    }
                }
            },
            {
                "$addFields": {
                    "active_status_on_date": {
                        "$filter": {
                            "input": "$status_on_date",
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
                    "active_status_on_date": {"$ne": []}
                }
            }
        ])
        
        results = list(collection.aggregate(pipeline))
        
        for result in results:
            territories_with_status.append({
                'name': result['name'],
                'category': result.get('category', 'Невідома'),
                'collection': collection_name,
                'active_periods': result['active_status_on_date']
            })
    
    return territories_with_status

def add_territory_status_period(client, territory_name, status, start_date, end_date=None, 
                               source_document="Перелік 07052025", additional_data=None):
    """
    Додавання нового періоду статусу для території з підтримкою нових статусів
    """
    db = client[DATABASE_NAME]
    
    # Шукаємо територію
    territory_doc, collection_name = find_territory(client, territory_name)
    
    if not territory_doc:
        print(f"❌ Територію '{territory_name}' не знайдено")
        return False
    
    collection = db[collection_name]
    
    # Створюємо запис про статус
    status_record = {
        'status': status.value if isinstance(status, TerritoryStatus) else status,
        'start_date': start_date,
        'end_date': end_date,
        'source_document': source_document,
        'document_date': DOCUMENT_CONFIG['document_date'],  # Читабельний формат
        'document_date_iso': DOCUMENT_CONFIG['document_date_iso'],  # ISO формат
        'updated_at': datetime.now()
    }
    
    # Додаємо додаткові дані, якщо є
    if additional_data:
        status_record.update(additional_data)
    
    # Визначаємо, в яку історію додавати запис
    if status == TerritoryStatus.TEMPORARILY_OCCUPIED:
        history_field = 'occupation_history'
    elif status in [TerritoryStatus.POSSIBLE_COMBAT, TerritoryStatus.ACTIVE_COMBAT, TerritoryStatus.ACTIVE_COMBAT_WITH_RESOURCES]:
        history_field = 'combat_history'
    else:
        history_field = 'status_history'
    
    # Отримуємо поточну історію
    if history_field in territory_doc:
        history = territory_doc[history_field]
    else:
        history = []
    
    # Додаємо новий запис до історії
    history.append(status_record)
    
    # Оновлюємо документ
    update_data = {
        "$set": {
            history_field: history,
            "current_status": status.value if isinstance(status, TerritoryStatus) else status,
            "last_status_update": datetime.now()
        }
    }
    
    if start_date:
        update_data["$set"]["status_start_date"] = start_date
    
    if end_date:
        update_data["$set"]["status_end_date"] = end_date
    
    collection.update_one({"_id": territory_doc["_id"]}, update_data)
    
    print(f"✅ Додано період статусу '{status.value if isinstance(status, TerritoryStatus) else status}' для: {territory_doc['name']}")
    return True

def find_territory(client, territory_name):
    """
    Пошук території в базі даних
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
        
        if result:
            return result, collection_name
    
    return None, None

def import_from_perelik_document(client, document_data):
    """
    Імпорт даних з документа Перелік 07052025
    """
    print("📄 Починаю імпорт даних з документа Перелік 07052025...")
    
    imported_count = 0
    errors_count = 0
    
    for table_data in document_data.get('tables', []):
        table_index = table_data.get('table_index')
        headers = table_data.get('headers', [])
        
        # Визначаємо статус на основі індексу таблиці
        if table_index == 1:
            status = TerritoryStatus.POSSIBLE_COMBAT
        elif table_index == 2:
            status = TerritoryStatus.ACTIVE_COMBAT
        elif table_index == 3:
            status = TerritoryStatus.ACTIVE_COMBAT_WITH_RESOURCES
        elif table_index in [4, 5]:
            status = TerritoryStatus.TEMPORARILY_OCCUPIED
        else:
            continue
        
        print(f"\n📊 Обробляю таблицю {table_index} - {status.value}")
        
        # Отримуємо індекси дата-колонок
        date_config = STATUS_DATE_CONFIG[status]
        start_date_idx = None
        end_date_idx = None
        
        for i, header in enumerate(headers):
            if date_config['start_date_column'] in header:
                start_date_idx = i
            elif date_config['end_date_column'] in header:
                end_date_idx = i
        
        if start_date_idx is None:
            print(f"⚠️  Не знайдено колонку з датою початку для таблиці {table_index}")
            continue
        
        # Обробляємо дані таблиці
        for row_data in table_data.get('sample_data', []):
            if len(row_data) < 4:  # Мінімум 4 колонки
                continue
            
            territory_code = row_data[0] if len(row_data) > 0 else ""
            territory_name = row_data[1] if len(row_data) > 1 else ""
            
            # Пропускаємо заголовки областей/районів
            if not territory_code or territory_code.startswith(('1.', '2.', '3.', '4.', '5.')):
                continue
            
            # Парсимо дати
            start_date = None
            end_date = None
            
            try:
                if start_date_idx < len(row_data) and row_data[start_date_idx]:
                    start_date_str = row_data[start_date_idx].split('\n')[0]  # Беремо першу дату, якщо їх кілька
                    start_date = parser.parse(start_date_str, dayfirst=True)
                
                if end_date_idx and end_date_idx < len(row_data) and row_data[end_date_idx]:
                    end_date_str = row_data[end_date_idx]
                    if end_date_str.strip():
                        end_date = parser.parse(end_date_str, dayfirst=True)
            except Exception as e:
                print(f"⚠️  Помилка парсингу дати для {territory_name}: {e}")
                continue
            
            # Додаємо запис
            try:
                success = add_territory_status_period(
                    client, 
                    territory_name, 
                    status, 
                    start_date, 
                    end_date,
                    source_document=DOCUMENT_CONFIG['document_name'],
                    additional_data={
                        'territory_code': territory_code,
                        'table_source': table_index
                    }
                )
                
                if success:
                    imported_count += 1
                else:
                    errors_count += 1
                    
            except Exception as e:
                print(f"❌ Помилка додавання запису для {territory_name}: {e}")
                errors_count += 1
    
    print(f"\n✅ Імпорт завершено!")
    print(f"📊 Успішно імпортовано: {imported_count}")
    print(f"❌ Помилок: {errors_count}")

def export_enhanced_data_to_csv(client, output_file="enhanced_territory_data.csv"):
    """
    Експорт розширених даних про статуси територій в CSV файл
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
        
        # Знаходимо всі території з історією
        territories = collection.find({
            "$or": [
                {"occupation_history": {"$exists": True}},
                {"combat_history": {"$exists": True}},
                {"status_history": {"$exists": True}}
            ]
        })
        
        for territory in territories:
            # Обробляємо історію окупації
            if 'occupation_history' in territory:
                for period in territory['occupation_history']:
                    all_data.append({
                        'territory_name': territory['name'],
                        'territory_code': territory.get('code', ''),
                        'category': territory.get('category', ''),
                        'collection': collection_name,
                        'status': period['status'],
                        'start_date': period['start_date'].strftime('%d.%m.%Y') if period['start_date'] else '',
                        'end_date': period['end_date'].strftime('%d.%m.%Y') if period['end_date'] else '',
                        'source_document': period.get('source_document', ''),
                        'updated_at': period['updated_at'].strftime('%d.%m.%Y %H:%M:%S') if 'updated_at' in period else ''
                    })
            
            # Обробляємо історію бойових дій
            if 'combat_history' in territory:
                for period in territory['combat_history']:
                    all_data.append({
                        'territory_name': territory['name'],
                        'territory_code': territory.get('code', ''),
                        'category': territory.get('category', ''),
                        'collection': collection_name,
                        'status': period['status'],
                        'start_date': period['start_date'].strftime('%d.%m.%Y') if period['start_date'] else '',
                        'end_date': period['end_date'].strftime('%d.%m.%Y') if period['end_date'] else '',
                        'source_document': period.get('source_document', ''),
                        'updated_at': period['updated_at'].strftime('%d.%m.%Y %H:%M:%S') if 'updated_at' in period else ''
                    })
            
            # Обробляємо загальну історію статусів
            if 'status_history' in territory:
                for period in territory['status_history']:
                    all_data.append({
                        'territory_name': territory['name'],
                        'territory_code': territory.get('code', ''),
                        'category': territory.get('category', ''),
                        'collection': collection_name,
                        'status': period['status'],
                        'start_date': period['start_date'].strftime('%d.%m.%Y') if period['start_date'] else '',
                        'end_date': period['end_date'].strftime('%d.%m.%Y') if period['end_date'] else '',
                        'source_document': period.get('source_document', ''),
                        'updated_at': period['updated_at'].strftime('%d.%m.%Y %H:%M:%S') if 'updated_at' in period else ''
                    })
    
    if all_data:
        df = pd.DataFrame(all_data)
        df.to_csv(output_file, index=False, encoding='utf-8')
        print(f"✅ Експортовано {len(all_data)} записів в {output_file}")
    else:
        print("❌ Немає даних для експорту")

def show_enhanced_statistics(client):
    """
    Показ розширеної статистики з підтримкою нових статусів
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
    territories_with_history = 0
    status_counts = {}
    document_dates = set()
    
    print("\n📈 РОЗШИРЕНА СТАТИСТИКА СТАТУСІВ ТЕРИТОРІЙ:")
    print("-" * 50)
    print(f"📄 Документ: {DOCUMENT_CONFIG['document_name']}")
    print(f"📅 Дата документа: {DOCUMENT_CONFIG['document_date']}")
    print("-" * 50)
    
    for collection_name in collections:
        collection = db[collection_name]
        
        # Загальна кількість територій
        total_in_collection = collection.count_documents({})
        total_territories += total_in_collection
        
        # Території з історією
        with_history = collection.count_documents({
            "$or": [
                {"occupation_history": {"$exists": True}},
                {"combat_history": {"$exists": True}},
                {"status_history": {"$exists": True}}
            ]
        })
        territories_with_history += with_history
        
        print(f"\n{collection_name}:")
        print(f"  Загалом територій: {total_in_collection}")
        print(f"  З історією статусів: {with_history}")
        
        # Підрахунок по статусах та збір дат документів
        territories = collection.find({
            "$or": [
                {"occupation_history": {"$exists": True}},
                {"combat_history": {"$exists": True}},
                {"status_history": {"$exists": True}}
            ]
        })
        
        for territory in territories:
            for history_field in ['occupation_history', 'combat_history', 'status_history']:
                if history_field in territory:
                    for period in territory[history_field]:
                        status = period.get('status', 'Невідомий')
                        if status not in status_counts:
                            status_counts[status] = 0
                        status_counts[status] += 1
                        
                        # Збираємо дати документів
                        if 'document_date' in period:
                            document_dates.add(period['document_date'])
    
    print(f"\n📊 ЗАГАЛЬНА СТАТИСТИКА:")
    print(f"  Загалом територій: {total_territories}")
    print(f"  З історією статусів: {territories_with_history}")
    
    if document_dates:
        print(f"\n📅 ДАТИ ДОКУМЕНТІВ У БАЗІ:")
        for doc_date in sorted(document_dates):
            print(f"  • {doc_date}")
    
    print(f"\n📋 РОЗПОДІЛ ПО СТАТУСАХ:")
    for status, count in sorted(status_counts.items()):
        print(f"  {status}: {count}")

def show_enhanced_menu():
    """Показ розширеного головного меню"""
    print("\n" + "=" * 60)
    print("🏛️  РОЗШИРЕНИЙ МЕНЕДЖЕР СТАТУСІВ ТЕРИТОРІЙ")
    print("=" * 60)
    print(f"📄 Документ: {DOCUMENT_CONFIG['document_name']}")
    print(f"📅 Дата документа: {DOCUMENT_CONFIG['document_date']}")
    print("=" * 60)
    print("1. 🔍 Перевірити статус території на конкретну дату")
    print("2. 📋 Показати історію статусів території")
    print("3. ➕ Додати період статусу")
    print("4. 📄 Імпорт з документа Перелік 07052025")
    print("5. 📊 Експорт даних в CSV")
    print("6. 📈 Розширена статистика")
    print("7. 🔧 Налаштування статусів")
    print("0. 🚪 Вихід")
    print("=" * 60)

def show_status_settings():
    """Показ налаштувань статусів"""
    print("\n🔧 НАЛАШТУВАННЯ СТАТУСІВ:")
    print("-" * 30)
    for i, status in enumerate(TerritoryStatus, 1):
        config = STATUS_DATE_CONFIG[status]
        print(f"{i}. {status.value}")
        print(f"   Дата початку: {config['start_date_column']}")
        print(f"   Дата кінця: {config['end_date_column']}")
        print()

def main():
    """Головна функція"""
    print("🚀 РОЗШИРЕНИЙ МЕНЕДЖЕР СТАТУСІВ ТЕРИТОРІЙ")
    print("Підтримує статуси з документа 'Перелік 07052025'")
    
    # Підключаємося до MongoDB
    client = connect_to_mongodb()
    
    while True:
        show_enhanced_menu()
        
        try:
            choice = input("Виберіть опцію (0-7): ").strip()
            
            if choice == "0":
                print("👋 До побачення!")
                break
                
            elif choice == "1":
                date_str = input("Введіть дату (формат: ДД.ММ.РРРР): ").strip()
                status_filter = input("Введіть статус для фільтрації (або Enter для всіх): ").strip()
                
                try:
                    query_date = parser.parse(date_str, dayfirst=True)
                    status_type = None
                    
                    if status_filter:
                        for status in TerritoryStatus:
                            if status.value == status_filter:
                                status_type = status
                                break
                    
                    territories = get_territory_status_on_date(client, query_date, status_type)
                    
                    print(f"\n🔍 Території зі статусом на {query_date.strftime('%d.%m.%Y')}:")
                    if territories:
                        for territory in territories:
                            print(f"  • {territory['name']} ({territory['category']})")
                            for period in territory['active_periods']:
                                start_str = period['start_date'].strftime('%d.%m.%Y') if period['start_date'] else 'н/д'
                                end_str = period['end_date'].strftime('%d.%m.%Y') if period['end_date'] else 'н/д'
                                print(f"    Статус: {period['status']} ({start_str} - {end_str})")
                    else:
                        print("  Немає територій з вказаним статусом на цю дату")
                        
                except Exception as e:
                    print(f"❌ Помилка: {e}")
                    
            elif choice == "2":
                territory_name = input("Введіть назву території: ").strip()
                territory_doc, collection_name = find_territory(client, territory_name)
                
                if territory_doc:
                    print(f"\n📋 Історія статусів для '{territory_doc['name']}':")
                    
                    # Показуємо всі типи історії
                    for history_type in ['occupation_history', 'combat_history', 'status_history']:
                        if history_type in territory_doc and territory_doc[history_type]:
                            print(f"\n  {history_type.replace('_', ' ').title()}:")
                            for i, period in enumerate(territory_doc[history_type], 1):
                                start_str = period['start_date'].strftime('%d.%m.%Y') if period['start_date'] else 'н/д'
                                end_str = period['end_date'].strftime('%d.%m.%Y') if period['end_date'] else 'н/д'
                                source = period.get('source_document', '')
                                print(f"    {i}. {period['status']}: {start_str} - {end_str} {f'({source})' if source else ''}")
                else:
                    print(f"❌ Територію '{territory_name}' не знайдено")
                    
            elif choice == "3":
                territory_name = input("Введіть назву території: ").strip()
                
                print("\nДоступні статуси:")
                for i, status in enumerate(TerritoryStatus, 1):
                    print(f"{i}. {status.value}")
                
                status_choice = input("Виберіть статус (1-4): ").strip()
                try:
                    status = list(TerritoryStatus)[int(status_choice) - 1]
                except:
                    print("❌ Невірний вибір статусу")
                    continue
                
                start_date_str = input("Введіть дату початку (ДД.ММ.РРРР): ").strip()
                end_date_str = input("Введіть дату кінця (ДД.ММ.РРРР, або Enter для пропуску): ").strip()
                
                try:
                    start_date = parser.parse(start_date_str, dayfirst=True)
                    end_date = None
                    if end_date_str:
                        end_date = parser.parse(end_date_str, dayfirst=True)
                    
                    add_territory_status_period(client, territory_name, status, start_date, end_date)
                    
                except Exception as e:
                    print(f"❌ Помилка парсингу дати: {e}")
                    
            elif choice == "4":
                print("📄 Імпорт з документа Перелік 07052025")
                print("Завантажую дані з categories_analysis.json...")
                
                try:
                    with open('categories_analysis.json', 'r', encoding='utf-8') as f:
                        document_data = json.load(f)
                    
                    import_from_perelik_document(client, document_data)
                    
                except FileNotFoundError:
                    print("❌ Файл categories_analysis.json не знайдено")
                    print("Спочатку запустіть analyze_categories.py для аналізу документа")
                except Exception as e:
                    print(f"❌ Помилка імпорту: {e}")
                    
            elif choice == "5":
                output_file = input("Введіть назву файлу для експорту (за замовчуванням 'enhanced_territory_data.csv'): ").strip()
                if not output_file:
                    output_file = "enhanced_territory_data.csv"
                export_enhanced_data_to_csv(client, output_file)
                
            elif choice == "6":
                show_enhanced_statistics(client)
                
            elif choice == "7":
                show_status_settings()
                
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