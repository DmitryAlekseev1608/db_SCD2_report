## Предварительные настройки

Работа сдается с уже созданными и заполненными таблицами в схеме public.
Поэтому, если хотите полностью проверить всю работоспособность сервиса прошу изначально
запустить скрипт backup.py из папки py_scripts, после чего все таблицы в public сотрутся, данные переместятся в папку input из папки
archive и из их наименования исчезнет указание .backup.
После чего надо будет запустить команды в main.ddl для создания исходных таблиц.

## Запуск работы через airflow 

Запуск через airflow является основным способом запуска:
1. Поднять контейнеры `sudo docker-compose up`.
2. Перейти по ссылке http://localhost:8080.
3. Данные для входа в сервис:
    login: `airflow`;
    password: `airflow`.
4. После входа запустить даг alex_work (нажать Trigger DAG), в котором имеется три задачи, выполняемых последовательно для
каждого из трех имеющихся дней.
5. После выполнения всех задач рекомендуется остановить контейнеры командой:
    `sudo docker-compose down --volumes --rmi all`.

Важно, чтобы данные по всем трем дням были в папке input, иначе программе нечего будет обрабатывать.
Еще в папке configs имеются конфигурациии в файле configs.yaml в двух вариантах для запуска с файла main.py и
через airflow.

## Запуск работы через main.py

При необходимости закомментировать пути к папкам input и archive из configs.yaml для airflow и разкоментировать
эти же пути для main.py. Подробности в файле configs.yaml.
1. Создать виртуальную среду `python -m venv /home/oem/Desktop/db_SCD2_report/venv_db` (путь уточнить по факту).
2. Активировать виртуальную среду `source /home/oem/Desktop/db_SCD2_report/venv_db/bin/activate` (путь уточнить по факту).
3. Установить зависимости `pip install -r requirements.txt`.
4. Запустить файл main.py.

## Описание работ по ТЗ

Помимо требований ТЗ учитывались требования, озвученные в переписке в чате, что от части противоречили ТЗ.
1. В работе полностью реализован поиск всех четырех мошенеческих операций из ТЗ и добавление их в отчет.
2. Все таблицы без исключения приведены к форме SCD2.
3. Crone при наличии airflow разрешили не реализовывать, поэтому файл
main.cron отсутствует.
4. Сдача проекта через сервер была отменена преподавателем, о чем он сообщил в телеграмм-чате.
5. Данные были загружены в таблицу фактов (transactions).
6. Созданы и заполнены все таблицы измерений (terminals, blacklist, accounts, cards, clients).
7.	Структурированный код: отступы, табуляции, комментирование, разделение на отдельные файлы логических блоков присутствует.
8.	Форма SCD2.
9.	Выделены все типы мошеннических транзакций в отчете.
10. Процесс полностью автоматизирован через airflow.