import mysql.connector as mariadb


class dsmr5_server:
    def __init__(self):
        pass


def aggregate_voltages():
    db = mariadb.connect(host='192.168.0.10', user='dsmr_user', passwd='dsmr_5098034ph', database='dsmr5')
    cursor = db.cursor()

    sql = "SELECT value FROM voltage_L1 ORDER BY date DESC LIMIT 1"
    cursor.execute(sql)
    results = cursor.fetchall()
    last_voltage = results[0][0]

    cursor = db.cursor()

    sql = "SELECT * FROM data WHERE OBIS_ref = '1-0:32.7.0' ORDER BY date"
    cursor.execute(sql)
    results = cursor.fetchall()
    last_key = 0

    for x in results:
        if x[3] != last_voltage:
            print(x)
            last_voltage = x[3]
            sql = "INSERT INTO voltage_L1 (`date`, `value`) VALUES (%s, %s)"
            val = (x[2], x[3])
            cursor.execute(sql, val)
            db.commit()
        last_key = x[0]

    sql = "DELETE FROM data WHERE OBIS_ref = '1-0:32.7.0' AND `key` < {0}".format(last_key)
    cursor.execute(sql)

    db.commit()

    cursor = db.cursor()

    sql = "SELECT value FROM voltage_L2 ORDER BY date DESC LIMIT 1"
    cursor.execute(sql)
    results = cursor.fetchall()
    last_voltage = results[0][0]

    cursor = db.cursor()

    sql = "SELECT * FROM data WHERE OBIS_ref = '1-0:52.7.0' ORDER BY date"
    cursor.execute(sql)
    results = cursor.fetchall()
    last_key = 0

    for x in results:
        if x[3] != last_voltage:
            print(x)
            last_voltage = x[3]
            sql = "INSERT INTO voltage_L2 (`date`, `value`) VALUES (%s, %s)"
            val = (x[2], x[3])
            cursor.execute(sql, val)
            db.commit()
        last_key = x[0]

    sql = "DELETE FROM data WHERE OBIS_ref = '1-0:52.7.0' AND `key` < {0}".format(last_key)
    cursor.execute(sql)

    db.commit()

    cursor = db.cursor()

    sql = "SELECT value FROM voltage_L3 ORDER BY date DESC LIMIT 1"
    cursor.execute(sql)
    results = cursor.fetchall()
    last_voltage = results[0][0]

    cursor = db.cursor()

    sql = "SELECT * FROM data WHERE OBIS_ref = '1-0:72.7.0' ORDER BY date"
    cursor.execute(sql)
    results = cursor.fetchall()
    last_key = 0

    for x in results:
        if x[3] != last_voltage:
            print(x)
            last_voltage = x[3]
            sql = "INSERT INTO voltage_L3 (`date`, `value`) VALUES (%s, %s)"
            val = (x[2], x[3])
            cursor.execute(sql, val)
            db.commit()
        last_key = x[0]

    sql = "DELETE FROM data WHERE OBIS_ref = '1-0:72.7.0' AND `key` < {0}".format(last_key)
    cursor.execute(sql)

    db.commit()
