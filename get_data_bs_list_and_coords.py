def base_station_get_from_export_romes(file_txt):
    with open(file_txt, 'r') as file_txt_r:
        return list(set([line[line.strip().find(':') + 1: line.strip().find('/')] for line in file_txt_r if
                         line.startswith('eNodeB') and line.strip().split(';')[13] != 11]))


def bs_lan_lon_from_export_romes(file_txt, DICT_OPERATOR, DICT_FREQ):
    with open(file_txt, 'r') as file_txt_r:
        temp_rows = dict()

        for k in file_txt_r:
            if k.startswith('eNodeB') and k.strip().split(';')[13] != '11':
                t = f"{k[k.strip().find(':') + 1: k.strip().find('/')]}_{k.strip().split(';')[13]}_{k.strip().split(';')[16]}"
                temp_rows[t] = temp_rows.get(t, []) + [
                    f"{k.strip().split(';')[1]};{k.strip().split(';')[2]};{k.strip().split(';')[4]};{k.strip().split(';')[5]}"]

        temp_rows_2 = {k: sorted(v, key=lambda x: float(x.split(';')[2]))[0] for k, v in temp_rows.items()}

        #temp_rows_3 = {f"{k.split('_')[0]}_{DICT_OPERATOR[k.split('_')[1]]}_{DICT_FREQ[k.split('_')[2]]}": v for k, v in
        #               temp_rows_2.items() if v.split(';')[0] != '0'}

        temp_rows_3 = {}
        for k, v in temp_rows_2.items():
            try:
                if v.split(';')[0] != '0':
                    temp_rows_3[f"{k.split('_')[0]}_{DICT_OPERATOR[k.split('_')[1]]}_{DICT_FREQ[k.split('_')[2]]}"] = v
            except KeyError:
                pass

        return temp_rows_3