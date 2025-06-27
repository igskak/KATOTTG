#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для імпорту даних з CSV-файлу kodifikator-16-05-2025.csv в MongoDB Atlas
Автор: AI Assistant
Дата: 2025
"""

import pandas as pd
import pymongo
from pymongo import MongoClient
import sys
import os
from urllib.parse import quote_plus

# РЯДОК ПІДКЛЮЧЕННЯ ДО MONGODB ATLAS
# Замініть цей рядок на свій рядок підключення з MongoDB Atlas
# Використовуємо quote_plus для правильного кодування логіна та пароля
username = quote_plus("test")
password = quote_plus("test")
MONGO_CONNECTION_STRING = f"mongodb+srv://{username}:{password}@clusterrd4u.7180sy4.mongodb.net/?retryWrites=true&w=majority&appName=ClusterRD4U"

# Назва бази даних
DATABASE_NAME = "ua_admin_territory"

# Мапінг категорій до колекцій
CATEGORY_TO_COLLECTION = {
    'O': 'level1_regions',      # Області та АРК
    'K': 'level1_regions',      # Області та АРК
    'P': 'level2_raions',       # Райони
    'H': 'level3_hromadas',     # Громади
    'M': 'level4_settlements',  # Міста
    'X': 'level4_settlements',  # Селища міського типу
    'C': 'level4_settlements',  # Села
    'B': 'level_additional_city_districts'  # Міські райони
}

def connect_to_mongodb():
    """Підключення до MongoDB Atlas"""
    try:
        print("🔌 Підключаюся до MongoDB Atlas...")
        client = MongoClient(MONGO_CONNECTION_STRING)
        # Тестуємо підключення
        client.admin.command('ping')
        print("✅ Успішно підключено до MongoDB Atlas")
        return client
    except Exception as e:
        print(f"❌ Помилка підключення до MongoDB: {e}")
        sys.exit(1)

def read_csv_file():
    """Читання CSV-файлу"""
    try:
        print("📖 Читаю CSV-файл...")
        # Правильні назви колонок
        column_names = [
            'Перший рівень',
            'Другий рівень', 
            'Третій рівень',
            'Четвертий рівень',
            'Додатковий рівень',
            'Категорія об\'єкта',
            'Назва об\'єкта'
        ]
        
        # Читаємо CSV з правильним кодуванням, роздільником та назвами колонок
        df = pd.read_csv('kodifikator-16-05-2025.csv', 
                        encoding='utf-8', 
                        sep=';',
                        skiprows=7,  # Пропускаємо заголовки та метадані
                        names=column_names)  # Вказуємо назви колонок
        
        print(f"✅ Прочитано {len(df)} рядків з CSV-файлу")
        print("\n🧩 Список колонок у DataFrame:")
        print(df.columns.tolist())
        print("\n🔍 Перші 3 рядки:")
        print(df.head(3))
        return df
    except Exception as e:
        print(f"❌ Помилка читання CSV-файлу: {e}")
        sys.exit(1)

def determine_object_code_and_parent(row):
    """
    Визначає код об'єкта та код батьківського об'єкта
    на основі останньої заповненої колонки рівнів
    """
    levels = ['Додатковий рівень', 'Четвертий рівень', 'Третій рівень', 'Другий рівень', 'Перший рівень']
    
    # Знаходимо останню заповнену колонку рівня
    object_code = None
    parent_code = None
    
    for i, level in enumerate(levels):
        if pd.notna(row[level]) and str(row[level]).strip() != '':
            object_code = str(row[level]).strip()
            # Батьківський код - це значення з наступного рівня (якщо він існує)
            if i + 1 < len(levels):
                parent_level = levels[i + 1]
                if pd.notna(row[parent_level]) and str(row[parent_level]).strip() != '':
                    parent_code = str(row[parent_level]).strip()
            break
    
    return object_code, parent_code

def import_data_to_mongodb(client, df):
    """Імпорт даних в MongoDB"""
    db = client[DATABASE_NAME]
    
    # Лічильники для статистики
    stats = {collection: 0 for collection in set(CATEGORY_TO_COLLECTION.values())}
    
    print("🚀 Починаю імпорт даних...")
    
    for index, row in df.iterrows():
        try:
            # Отримуємо категорію об'єкта
            category = str(row['Категорія об\'єкта']).strip()
            
            # Пропускаємо рядки без категорії
            if pd.isna(category) or category == '' or category == 'nan':
                continue
            
            # Визначаємо код об'єкта та батьківський код
            object_code, parent_code = determine_object_code_and_parent(row)
            
            # Пропускаємо рядки без коду об'єкта
            if not object_code:
                continue
            
            # Отримуємо назву об'єкта
            object_name = str(row['Назва об\'єкта']).strip()
            
            # Визначаємо колекцію на основі категорії
            if category not in CATEGORY_TO_COLLECTION:
                print(f"⚠️  Невідома категорія '{category}' для об'єкта {object_name}")
                continue
            
            collection_name = CATEGORY_TO_COLLECTION[category]
            collection = db[collection_name]
            
            # Створюємо документ для MongoDB
            document = {
                "_id": object_code,
                "name": object_name,
                "category": category,
                "parent_code": parent_code
            }
            
            # Видаляємо parent_code якщо він None
            if parent_code is None:
                del document["parent_code"]
            
            # Використовуємо replace_one з upsert=True
            collection.replace_one(
                {"_id": object_code}, 
                document, 
                upsert=True
            )
            
            stats[collection_name] += 1
            
            # Показуємо прогрес кожні 1000 записів
            if (index + 1) % 1000 == 0:
                print(f"📊 Оброблено {index + 1} рядків...")
                
        except Exception as e:
            print(f"❌ Помилка обробки рядка {index + 1}: {e}")
            continue
    
    print("\n📈 Статистика імпорту:")
    for collection, count in stats.items():
        print(f"   {collection}: {count} записів")
    
    total_imported = sum(stats.values())
    print(f"\n✅ Імпорт завершено! Всього імпортовано {total_imported} записів")

def main():
    """Головна функція"""
    print("=" * 60)
    print("🚀 СКРИПТ ІМПОРТУ ДАНИХ В MONGODB ATLAS")
    print("=" * 60)
    
    # Перевіряємо наявність CSV-файлу
    if not os.path.exists('kodifikator-16-05-2025.csv'):
        print("❌ Файл 'kodifikator-16-05-2025.csv' не знайдено в поточній директорії")
        sys.exit(1)
    
    # Підключаємося до MongoDB
    client = connect_to_mongodb()
    
    # Читаємо CSV-файл
    df = read_csv_file()
    
    # Імпортуємо дані
    import_data_to_mongodb(client, df)
    
    # Закриваємо з'єднання
    client.close()
    print("\n🔌 З'єднання з MongoDB закрито")
    print("=" * 60)
    print("🎉 Робота скрипту завершена успішно!")

if __name__ == "__main__":
    main() 