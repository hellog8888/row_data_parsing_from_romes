from math import trunc
import psycopg2


def toFixed(numObj, digits=0):
    return f"{numObj:.{digits}f}"


def recursive_flatten_generator(array):
    lst = []
    for i in array:
        if isinstance(i, list):
            lst.extend(recursive_flatten_generator(i))
        else:
            lst.append(i)
    return lst


def convert_coords(data, lan_lon):
    DD = trunc(float(data))
    MM = trunc((float(data) - DD) * 60)
    SS = trunc(((float(data) - DD) * 60 - MM) * 60)

    return f'{DD}{lan_lon}{str(MM).rjust(2, "0")}{str(SS).rjust(2, "0")}'


def query_data_from_database(list_coords):
    connection = None
    result = []

    try:
        connection = psycopg2.connect(dbname="eirs", user="postgres", password="1234", host="localhost")
        cursor = connection.cursor()

        for x in list_coords:
            cursor.execute(f"SELECT * FROM cellular WHERE Широта = '{x.split(';')[0]}' AND Долгота = '{x.split(';')[1]}'")
            result.append(cursor.fetchall())

        return recursive_flatten_generator(result)

    except Exception as error:
        print(error)
    finally:
        if connection is not None:
            connection.close()


def search_coords(E, N):
    ONE_SEC = 0.000278
    coords_list = []

    E = float(E)
    N = float(N)

    temp = ONE_SEC

    E_pos = []
    N_pos = []

    E_pos.append(convert_coords(E, 'E'))
    N_pos.append(convert_coords(N, 'N'))

    for i in range(9):
        E_pos.append(convert_coords(toFixed(E + temp, 6), 'E'))
        temp += ONE_SEC
    temp = ONE_SEC

    for i in range(9):
        E_pos.append(convert_coords(toFixed(E - temp, 6), 'E'))
        temp += ONE_SEC
    temp = ONE_SEC

    for i in range(9):
        N_pos.append(convert_coords(toFixed(N + temp, 6), 'N'))
        temp += ONE_SEC
    temp = ONE_SEC

    for i in range(9):
        N_pos.append(convert_coords(toFixed(N - temp, 6), 'N'))
    temp += ONE_SEC

    for i in N_pos:
        for j in range(len(E_pos)):
            coords_list.append(f'{i};{E_pos[j]}')

    return query_data_from_database(coords_list)