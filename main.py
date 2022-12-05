import csv
import os
import time
from xml.dom import minidom

import fdb
import pymssql
from dotenv import load_dotenv

load_dotenv('.env')

MAN_REPORT_HEADER_D = [
    'id',
    'from_time',
    'to_time',
    'task_type',
    'place',
    'person',
    'w',
    'c',
    'cnt_u',
    'cnt_w',
    'price',
    'group',
    'p_per_k',
    'p_per_l',
    'p_per_cnt',
    'p_ware_w',
    'p_task',
    'doc',
    'client',
    'assigner'
]

MAN_REPORT_HEADER = [
    'Идентификатор',
    'Время начала задания',
    'Время завершения задания',
    'Тип задания',
    'Зона местоположений',
    'Сотрудник',
    'Вес',
    'Объем',
    'Количество ШТ',
    'Количество ВЕС',
    'Стоимость задания',
    'Группа отборки',
    'Тариф веса за кг',
    'Тариф объема за литр',
    'Тариф товара шт',
    'Тариф товара вес',
    'Тариф за задание',
    'Документ',
    'Клиент',
    'Назначил'
]

DRV_REPORT_HEADER_D = [
    'gate_task_id',
    'client',
    'city',
    'region',
    'transport_type',
    'transport_max_w',
    'transport_max_vol',
    'transport_max_cap',
    'driver',
    'gate',
    'task',
    'begin_time',
    'end_time',
    'p_plan',
    'p_wares',
    'p_wares_tools',
    'wares_weight',
    'wares_volume',
    'doc_number',
    'doc_date',
    'p_number',
    'group',
    'w_percent_load',
    'v_percent_load',
    'transport_number'
]

DRV_REPORT_HEADER = [
    'Идентификатор',
    'Подразделение',
    'Город',
    'Регион',
    'Тип транспорта',
    'Тоннаж ТС',
    'Объем ТС',
    'Емкость ТС',
    'Водитель',
    'Ворота',
    'Задание',
    'Начало погрузки',
    'Завершение погрузки',
    'План паллет',
    'Паллет товара',
    'Паллет оборудования',
    'Вес товара',
    'Объем товара',
    'Номер ТТН',
    'Дата ТТН',
    'Номер паллета',
    'ГО',
    'Процент загрузки (Вес)',
    'Процент загрузки (Объем)',
    'Номер ТС'
]


def decor(func):
    def wrapper(*args):
        start_time = time.time()
        func(*args)
        end_time = time.time()
        print(f'Время работы функции {str(func).split()[1]} составило: {end_time - start_time} сек.')

    return wrapper


def get_mssql_connection():
    return pymssql.connect(server=os.getenv("MSSQL_SERVER"), user=os.getenv("MSSQL_LOGIN"),
                           password=os.getenv("MSSQL_PASSW"), database=os.getenv("MSSQL_DATABASE"))


def file_to_binary_data(filename, delete_after_load=True):
    try:
        with open(filename, "rb") as f:
            binary_data = f.read()
        if delete_after_load:
            os.remove(filename)
        return binary_data
    except Exception as E:
        print(f"Исключительная ситуация при загрузке данных из файла: {E}")
        return 0


def get_wms_data_drv_load(fd=-3, td=1):
    fbsql = fdb.connect(dsn=os.getenv("FBSQL"), user=os.getenv("FBSQL_DRV_LOGIN"),
                        password=os.getenv("FBSQL_DRV_PASSW"))
    cur = fbsql.cursor()
    cur.execute("SELECT "
                "gatetaskid,"
                "toobjname,"
                "cityname,"
                "regionname,"
                "trtype,"
                "carrying,"
                "carcapacity,"
                "capacitypal,"
                "drfio,"
                "SITENAME,"
                "loadtask,"
                "dtbeg,"
                "dtend,"
                "coalesce(plankp, 0),"
                "coalesce(p, 0),"
                "coalesce(po, 0),"
                "coalesce(weight, 0),"
                "coalesce(capacity, 0),"
                "docnumber,"
                "realdocdate,"
                "coalesce(palnumber, 'Нет'),"
                "sgname,"
                "coalesce(weight/carrying, 0),"
                "coalesce(capacity/carcapacity, 0),"
                "COALESCE(carlic, 'Нет')"
                "from WH_GATEAUTO_FACTKP(dateadd(day, ?, current_date), dateadd(day, ?, current_date))", (fd, td,)
                )
    for drv_load_row in cur:
        yield {DRV_REPORT_HEADER_D[el]: drv_load_row[el] for el in range(0, len(drv_load_row))}


def get_wms_data_man_report(fd=-3, td=-1):
    fbsql = fdb.connect(dsn=os.getenv("FBSQL"), user=os.getenv("FBSQL_MAN_LOGIN"),
                        password=os.getenv("FBSQL_MAN_PASSW"))
    cur = fbsql.cursor()
    cur.execute("select "
                "taskid,"
                "begtime,"
                "endtime,"
                "coalesce(TYPENAME, 'Нет'),"
                "coalesce(ZONENAME, 'Нет'),"
                "coalesce(MANFIO, ''),"
                "coalesce(WEIGHT, 0),"
                "coalesce(CAPACITY, 0),"
                "coalesce(CNTUNITS, 0),"
                "coalesce(CNTUNITSW, 0),"
                "coalesce(COST, 0),"
                "coalesce(SELGROUPNAME, 'Нет'),"
                "coalesce(RATEWEIGHT, 0),"
                "coalesce(RATEVOLUME, 0), "
                "coalesce(RATEUNIT, 0), "
                "coalesce(RATEUNITW, 0), "
                "coalesce(RATETASK, 0),"
                "coalesce(DOCNUM, 'Нет'),"
                "coalesce(CLIENTNAME, 'Нет'),"
                "coalesce(AUTHORNAME, 'Диспетчеризация') "
                "from WH_EXTERNAL_PERIODTASK (dateadd(day, ?, current_date), dateadd(day, ?, current_date))", (fd, td,)
                )
    for man_report_row in cur:
        yield {MAN_REPORT_HEADER_D[el]: man_report_row[el] for el in range(0, len(man_report_row))}


def write_data_to_xml(data, filename, header="root"):
    """
    :param data: Объект - список или генератор. Можно передать словари внутри списков, или списки списков.
    :param filename: Имя файла в который требуется записать данные.
    :param header: Имя корневого элемента xml
    :return: Вернет истину, если все отработает корректно.
    """
    g = (n for n in [0, 1])
    try:
        # Умеем сохранять списки или генераторы.
        if type(data) in (list, type(g)):
            with open(filename, "w", encoding="UTF8") as f:
                root = minidom.Document()
                xml = root.createElement(header)
                root.appendChild(xml)
                for i, dd in enumerate(data):
                    row = root.createElement("row")
                    row.setAttribute("id", str(i + 1))
                    if type(dd) == list:
                        for z, vl in enumerate(list(dd)):
                            val = root.createElement("row")
                            val.setAttribute("id", str(z + 1))
                            val.setAttribute("data", str(vl))
                            row.appendChild(val)
                        xml.appendChild(row)
                    elif type(dd) == dict:
                        for x in dd.keys():
                            row.setAttribute(x, str(dd.get(x)))
                        xml.appendChild(row)
                    else:
                        row.setAttribute("data", str(dd))
                        xml.appendChild(row)
                xml_str = root.toprettyxml(indent="\t")
                f.write(xml_str)
                return True
        else:
            print("Записывать в xml данные можно только генераторы или списки!")
            return False
    except Exception as E:
        print(f"Исключительная ситуация при преобразовании в XML: {E}.")
        return False


@decor
def upload_man_report(fd=-3, td=-1, file_name='man_report.csv'):
    with open(file_name, mode='w', encoding='UTF8', newline="") as f:
        writer = csv.writer(f, dialect='excel', delimiter=';')
        writer.writerow(MAN_REPORT_HEADER)
        for row in get_wms_data_man_report(fd, td):
            writer.writerow(row)


@decor
def upload_drv_report(fd=-3, td=1, file_name='wms_export.csv'):
    with open(file_name, mode='w', encoding='UTF8', newline="") as f:
        writer = csv.writer(f, dialect='excel', delimiter=';')
        writer.writerow(DRV_REPORT_HEADER)
        for row in get_wms_data_drv_load(fd, td):
            writer.writerow(row)


@decor
def upload_man_report_xml(fd=-3, td=1, file_name='man_report.xml', retry_counter=5):
    try:
        if retry_counter > 0:
            if write_data_to_xml(get_wms_data_man_report(fd, td), file_name):
                sql = get_mssql_connection()
                cur = sql.cursor()
                cur.execute('insert into [import_buffer] (packet_name, packet_data) values (%s, %s)',
                            ('man_report', file_to_binary_data(file_name)))
                sql.commit()
            else:
                upload_man_report_xml(fd, td, file_name, retry_counter - 1)
    except Exception as E:
        print(E)
        upload_man_report_xml(fd, td, file_name, retry_counter)


@decor
def upload_drv_report_xml(fd=-3, td=1, file_name='wms_export.xml', retry_counter=5):
    try:
        if retry_counter > 0:
            if write_data_to_xml(get_wms_data_drv_load(fd, td), file_name):
                sql = get_mssql_connection()
                cur = sql.cursor()
                cur.execute('insert into [import_buffer] (packet_name, packet_data) values (%s, %s)',
                            ('drv_report', file_to_binary_data(file_name)))
                sql.commit()
            else:
                upload_drv_report_xml(fd, td, file_name, retry_counter - 1)
    except Exception as E:
        print(E)
        upload_drv_report_xml(fd, td, file_name, retry_counter - 1)


def main(fd=-3, td=1):
    for i in range(fd, td):
        upload_man_report_xml(i, i + 1)
        upload_drv_report_xml(i, i + 1)

    sql = get_mssql_connection()
    cur = sql.cursor()
    print('Обработка загруженных данных.')
    cur.execute('exec [process_drv_report_import]')
    cur.execute('exec [process_man_report_import]')
    print('Оптимизация хранения данных.')
    cur.execute('exec [maintenance]')
    sql.commit()


main()
