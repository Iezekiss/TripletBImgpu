#!/bin/bash

# Скрипт для настройки Kaggle API
# Лабораторная работа №4: Анализ президентов США (Вариант 30)

set -e

echo "🔑 Настройка Kaggle API"
echo "======================="

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Функция для вывода сообщений
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Проверка наличия kaggle.json в корне проекта
if [ ! -f "kaggle.json" ]; then
    print_error "Файл kaggle.json не найден в корне проекта!"
    echo ""
    print_status "Для получения kaggle.json:"
    echo "1. Зайдите на сайт https://www.kaggle.com"
    echo "2. Войдите в свой аккаунт"
    echo "3. Перейдите в Account -> API -> Create New API Token"
    echo "4. Скачайте файл kaggle.json"
    echo "5. Поместите файл в корень проекта (рядом с docker-compose.yml)"
    echo "6. Запустите этот скрипт снова"
    exit 1
fi

print_success "Файл kaggle.json найден в корне проекта."

# Создание папки .kaggle
print_status "Создание папки dags/.kaggle..."
mkdir -p dags/.kaggle
print_success "Папка dags/.kaggle создана."

# Копирование kaggle.json
print_status "Копирование kaggle.json в dags/.kaggle/..."
cp kaggle.json dags/.kaggle/
print_success "kaggle.json скопирован."

# Установка прав доступа
print_status "Установка прав доступа..."
chmod 777 dags/.kaggle
chmod 644 dags/.kaggle/kaggle.json
chmod 777 dags/data 
print_success "Права доступа установлены."

# Проверка структуры
print_status "Проверка структуры файлов..."
echo "Структура dags/.kaggle/:"
ls -la dags/.kaggle/

# Проверка содержимого kaggle.json
print_status "Проверка содержимого kaggle.json..."
if grep -q "username" dags/.kaggle/kaggle.json && grep -q "key" dags/.kaggle/kaggle.json; then
    print_success "kaggle.json содержит необходимые поля (username, key)."
else
    print_warning "kaggle.json может быть поврежден или неполным."
    print_status "Убедитесь, что файл содержит поля 'username' и 'key'."
fi

echo ""
echo "✅ Настройка Kaggle API завершена!"
echo "=================================="
echo "Теперь можно запускать проект:"
echo "sudo docker compose up -d"
echo ""
echo "Файл kaggle.json настроен и готов к использованию в контейнерах Airflow."
