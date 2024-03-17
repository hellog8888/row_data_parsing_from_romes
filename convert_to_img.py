from PIL import Image, ImageDraw, ImageFont


def convert_to_img(file_name, path_to_save=''):
    path_to_save_file = path_to_save

    width = 1830
    height = 180
    bg_color = (255, 255, 255)

    font_size = 30
    font = ImageFont.truetype("arial.ttf", font_size)
    text_color = (0, 0, 0)

    h_Data = 'Date'
    h_Time = 'Time'
    h_EARFCN = 'EARFCN'
    h_Frequency = 'Frequency'
    h_PCI = 'PCI'
    h_MCC = 'MCC'
    h_MNC = 'MNC'
    h_TAC = 'TAC'
    h_CI = 'CI'
    h_eNodeB_ID = 'eNodeB-ID'
    h_Power = 'Power'
    h_MIB_Bandwidth_MHz = 'MIB_Bandwidth(MHz)'
    header_to_img = f'{h_Data.center(15)} | {h_Time.center(10)} | {h_EARFCN.center(0)} | {h_Frequency.center(0)} | {h_PCI.center(0)} | {h_MCC.center(0)} | {h_MNC.center(0)} | {h_TAC.center(6)} | {h_CI.center(17)} | {h_eNodeB_ID.center(0)} | {h_Power.center(0)} | {h_MIB_Bandwidth_MHz.center(0)}'
    line_separator = '-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------'

    with open(file_name, 'r') as file:
        for line in file.readlines():
            image = Image.new('RGB', (width, height), color=bg_color)
            draw = ImageDraw.Draw(image)

            # смещение по размеру изображения
            draw.text((30, 47), f'{header_to_img}\n{line_separator}\n{line}', fill=text_color, font=font)

            image.save(f'{path_to_save_file}_{line.split("|")[3].strip()}.png')