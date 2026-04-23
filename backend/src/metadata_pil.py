import os
import csv
import numpy as np
from PIL import Image, ImageStat

def get_image_metadata_and_save_csv(folder_path, output_csv_path):
    inventory = []
    valid_extensions = ('.jpg', '.jpeg', '.png', '.tiff')
    
    # Encabezados basados en schema + Metadatos Visuales
    headers = [
        "filename", "format", "width", "height", "aspect_ratio", 
        "size_kb", "brightness", "contrast", "main_color_rgb",
        "manual_author", "subject", "context", "notes"
    ]

    for filename in os.listdir(folder_path):
        if filename.lower().endswith(valid_extensions):
            file_path = os.path.join(folder_path, filename)
            try:
                with open(file_path, 'rb') as f:
                    with Image.open(f) as img:
                        # Convertir a escala de grises para brillo y contraste
                        stat = ImageStat.Stat(img.convert('L'))
                        brightness = round(stat.mean[0], 2) # 0 (negro) a 255 (blanco)
                        contrast = round(stat.stddev[0], 2)
                        
                        # Obtener color predominante (redimensionando para velocidad)
                        img_small = img.resize((1, 1), resample=Image.Resampling.BILINEAR)
                        main_color = img_small.getpixel((0, 0))

                        info = {
                            "filename": filename,
                            "format": img.format,
                            "width": img.width,
                            "height": img.height,
                            "aspect_ratio": round(img.width / img.height, 2),
                            "size_kb": round(os.path.getsize(file_path) / 1024, 2),
                            "brightness": brightness,
                            "contrast": contrast,
                            "main_color_rgb": f"RGB{main_color}",
                            "manual_author": "", # Columnas para tu llenado manual
                            "subject": "",
                            "context": "",
                            "notes": ""
                        }
                        inventory.append(info)
            except Exception as e:
                print(f"Error en {filename}: {e}")

    # Escribir el CSV con el delimitador ";" que usas en tu ejemplo
    with open(output_csv_path, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers, delimiter=';')
        writer.writeheader()
        writer.writerows(inventory)
    
    return inventory