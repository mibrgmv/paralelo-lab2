# Качество данных: Online Retail

## 0. Общая характеристика

Датасет – продажи британского онлайн-магазина подарков с 01.12.2010 по 09.12.2011.

- 541 909 строк, 8 колонок: `InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country`.
- 25 900 заказов, 4 372 клиента, 4 070 товаров, 38 стран.
- Кодировка ISO-8859-1, строки разделены `\r\n`.

Файл целый: битых строк нет, все числа и даты в правильном формате. Загрузка (`OnlineRetail.loadSales`) оставляет только продажи и убирает дубли. После неё остаётся 524 878 строк (96.9%).

## 1. Правила качества данных

**1. Текст читается в кодировке ISO-8859-1**
- Нарушение: символы вроде `£` превращаются в мусор.
- В исходных данных: 32 строки с `£` (`Gift Voucher £40.00`).
- Проверяет: `decodesLatin1`.

**2. Каждая строка разбирается по схеме: 8 полей, `Quantity` и `CustomerID` – целые числа, `UnitPrice` – число**
- Нарушение: загрузка падает с ошибкой, а не пропускает строку молча.
- В исходных данных: 0 нарушений.
- Проверяют: `keepsSchema`, `parsesSale`, `failsOnMalformedRow`.

**3. Количество больше нуля**
- Нарушение: строка отсекается.
- В исходных данных: 9 288 отмен (номер заказа на `C`, количество у всех отрицательное) и 1 336 списаний.
- Проверяют: `dropsNotSales` (cancellation, zero quantity, negative quantity).

**4. Цена больше нуля**
- Нарушение: строка отсекается.
- В исходных данных: 2 517 строк (2 515 с ценой 0, 2 с отрицательной).
- Проверяют: `dropsNotSales` (zero price, negative price).

**5. Нет полностью совпадающих строк**
- Нарушение: остаётся одна копия.
- В исходных данных: 5 268 лишних копий.
- Проверяют: `dropsExactDuplicates`, `keepsRowsDifferingInOneField`.

**6. `CustomerID` может быть пустым**
- Такая строка остаётся и учитывается в статистике по товарам, но не участвует в метриках по клиентам.
- В исходных данных: 135 080 строк (25%).
- Проверяют: `keepsSaleWithoutCustomer`, `topProductsSumsQuantityPerProduct`, `customerStatsSkipsRowsWithoutCustomer`.

**7. После загрузки остаётся ровно 524 878 строк**
- Число сверено с независимым расчётом на Python. Отсекается 17 031 строка, это меньше суммы чисел выше, потому что часть строк нарушает сразу несколько правил.
- Проверяет: `fullDataset`.

## 2. DQ report

Spark-задача `DataQualityReport` проверяет сырой CSV по 28 пунктам: структура строк, пустые значения в каждой колонке, форматы полей, отмены и служебные записи, дубли, согласованность заказов и товаров. Результат печатается таблицей в консоль и сохраняется в `reports/dq_report.json`: по каждой проверке число нарушений, доля и до 3 строк-примеров.

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 21) mvn clean package
docker compose up -d
docker exec spark-master /opt/spark/bin/spark-submit \
  --class com.example.retail.quality.DataQualityReport \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --executor-memory 512m \
  /opt/spark-apps/online-retail.jar \
  /opt/spark-data/data.csv /opt/spark-reports/dq_report.json
```

CSV нужно заранее распаковать в `data/data.csv`.

## 3. Тесты

`OnlineRetailTest`, 22 теста. Каждый собирает маленький CSV из строк настоящего датасета и проверяет одно правило.

### Общие тесты на загрузку данных

1. `keepsSchema` – колонки и их типы совпадают со схемой.
2. `parsesSale` – обычная продажа читается с правильными значениями во всех 8 полях.
3. `dropsNotSales[cancellation]` – отмена (номер на `C`) отсекается.
4. `dropsNotSales[zero quantity]` – строка с количеством 0 отсекается.
5. `dropsNotSales[negative quantity]` – строка с отрицательным количеством отсекается.
6. `dropsNotSales[zero price]` – строка с ценой 0 отсекается.
7. `dropsNotSales[negative price]` – строка с отрицательной ценой отсекается.
8. `dropsExactDuplicates` – три одинаковые строки схлопываются в одну.
9. `keepsRowsDifferingInOneField` – две строки, отличающиеся одним полем, остаются обе.
10. `decodesLatin1` – знак `£` читается правильно.
11. `keepsSaleWithoutCustomer` – продажа без `CustomerID` остаётся, клиент = null.
12. `failsOnMalformedRow[quantity not a number]` – загрузка падает, если в количестве не число.
13. `failsOnMalformedRow[price not a number]` – загрузка падает, если в цене не число.
14. `failsOnMalformedRow[missing field]` – загрузка падает, если в строке не хватает поля.
15. `fullDataset` – на полном датасете 524 878 строк, без отмен, без количества или цены ≤ 0, без дублей.

### Тесты на топ-5 товаров и метрики клиентов

1. `topProductsSumsQuantityPerProduct` – количество товара суммируется по разным заказам, продажи без клиента тоже учитываются.
2. `topProductsReturnsFiveLargestDescending` – из 6 товаров возвращаются 5 самых продаваемых, по убыванию.
3. `topProductsTakesMostFrequentDescription` – если у товара несколько описаний, берётся самое частое.
4. `customerStatsCountsOrdersSpentAndAvgCheck` – 3 позиции в 2 заказах: 2 заказа, сумма 10, средний чек 5 (а не 3.33 при среднем по строкам).
5. `customerStatsRoundsToCents` – сумма и средний чек округляются до копеек (10 / 3 → 3.33).
6. `customerStatsSkipsRowsWithoutCustomer` – строки без `CustomerID` в результат не попадают.
7. `customerStatsOneRowPerCustomerSortedBySpent` – одна строка на клиента, клиенты отсортированы по сумме по убыванию.
