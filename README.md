# Репозиторий с ML частью нашего сервиса
В папке `notebooks` лежат jupyter-ноутбуки, в которых происходил анализ данных и обучение модели
В папке `incident_predictor` находится код сервиса, который применяет обученную модель для предсказания аварийных ситуаций

# Сервис предсказаний

Сервис написан на Python с использованием Flask. Для предсказания использует данные из поднятой базы данных Postgesql и открытого погодного API Open-Meteo (https://open-meteo.com).

## Сборка и запуск

Для сборки и запуска используется docker. 
- Сборка docker-образа: `docker build -t incident_predictor:latest incident_predictor_app`
- Запуск контейнера: `docker run --name incident_predictor --env <определение переменных окружения> -d -p 5000:5000 incident_predictor:latest`

Переменные окружения:
- `BACKEND_URL` - адрес бэкенда
- `BACKEND_PORT` - порт бэкенда
- `BD_HOST` - адрес базы данных
- `BD_PORT` - порт базы данных
- `BD_USER` - имя пользователя базы данных
- `BD_PASS` - пароль базы данных

Для упрощения процесса запуска можно использовать shell-скрипт `run.sh`

## API

- `/ping` [GET]
- `/predict` [GET] - возвращает json с предсказаниями для адресов, которые есть в базе. Через cgi-параметр `date` можно указать дату, за коротую хочется получить рассчет. Пример: `/predict?date=2024-05-15`
- `/predict_for_today_and_save` [GET] - реализует логику `/predict` за сегодняшнюю дату, после рассчета предсказания отправляет их POST-запросом в ручку `/v0/predications/save` бэкенда для сохранения в базе.
  Вызывается при старте сервиса и далее регулярно каждые 2 часа.

# Обучение модели

Требуется получить модель, способную определять объекты с повышенной вероятностью возникновения аварийных ситуаций. Весь процесс построения такой модели реализован в jupyter-ноутбуке `notebooks/train_model.ipynb`. 
Ноутбук написан универсально и при незначительных модификациях может быть запущен на данных для других округов/за другие периоды времени.

## Формирование обучающего набора данных

В данной задаче правильное формирование обучающего набора данных играет очень важную роль. Авторы задачи предоставили выгрузку обращений жителей по различным проблемам с ЖКХ за часть 2023 и 2024 года в Восточном округе. 

Отфильтровав из них целевые обращения о проблемах с отоплением и сгруппировав обращения по адресу в течении дня можно получить список событий, которые требуется опреденить.
Теперь требуется сгенерировать примеры событий в виде пар <дата, адрес> когда все было хорошо и инцидентов не возникало. 
Далее объединив событий инцидентов и моментов когда все хорошо можно рассматривать данную задачу как задачу классификации где требуется правильно классифицировать моменты инцидентов.

При формировании такого обучающего набора данных на каждое событие инцидента добавлялось 4 события со зданиями, по которым не было обращений. 
Также чтобы повысить сложность задачи для модели в набор данных подмешивались пары <дата, адрес>, где адрес - это адрес, 
по которому происходил целевой инцидент, но дата - когда все было хорошо или было какое-то другое незначительное обращение. 
Это сделано для того чтобы требовать от модели больше зависеть от времени.

Набор данных делится на тренировочный и тестовый. Тестовый никак не участвует в обучении и используется только для оценки качества предсказаний. Тестовые данные начинаютя с 2024.03.01 и составляют 13% от всего набора данных.

## Оценка качества предсказаний

Данная задача формируется как задача классификации с сильным дисбаллансом классов. 
Встает вопрос что нам важнее: `Precision` (сколько из того, что мы назвали инцидентом реально является инцидентом) или `Recall` (сколько инцидентов мы смогли определить). 

Было принято решение, что в данной задаче важнее `Recall` потому что лучше подсветить все дома с проблемами, но в некоторых местах перестраховаться, чем будучи аккуратным в предсказаниях пропустить потенциальный инцидент.
Поэтому оптимизируется `Recall` с попыткой сильно не просадить `Precision`.

Также часто прибегают к оценке модели с использованием `F-1` и `F-beta`, которые объединяют в себе `Precision` и `Recall`. 
Мы оцениваем их, но так как в данной постановке ошибки первого рода не равноценны, больший приоритет отдается `F-beta` с `beta=2`, для которой `Recall` в 2 раза более важен чем `Precision`.

## Кросс-валидация на временном ряде

При обучении модели использовалась кросс-валидация на временном ряде. Ее визуализация представлена ниже:

![](/assets/cv.png)

Обучающее множество разбивается на N фолдов. Далее из них берутся первые K и используются для обучения, а K+1 - для валидации. В данном решении использовалась кросс-валидация с 5 фолдами. 
Обученные на каждом фолде модели объединялись в ансамбль чтобы повысить итоговое качество предсказания. 

## Подбор порога предсказания

Для получения наилучших результатов для модели на каждом фолде по валидационной выборке определяется порог, позволяющий достичь потимального баланса между `Precision` и `Recall`. Пример такого подбора представлен ниже:
![](/assets/thresholding.png)

## Использованные модели

В качестве базовой модели используется градиентный бустинг. Взята его реализация из библиотеки CatBoost (https://catboost.ai/).

## Отбор признаков

В данной задачи модели были склонны к переобучению и избежать его помог отбор признаков. К признакам добавлялись 3 признака со случайными значениями из нормального распределения N(0, 1). Далее оценивалась важность признаков для модели. Использовался показатель FSTR PredictionValuesChange, зависящий от того, как сильно признак влияет на предсказания. Признаки, которые по нему оказывались ниже чем введенные выше рандомные считались также рандомными и убирались из обучения. Это позволило значительно снизить переобучение и повысить качество на тестовой выборке.

## Итоговое качество

Итоговый ансамбль имеет `Recall=0.74` и `F-2=0.51` на тесте длиной в 2 месяца. `Precision` оказался 0.22, но при необходимости балланс между метриками может быть изменен варьированием одного из обучающиз параметров, влияющиего на выбор порога предсказания.
