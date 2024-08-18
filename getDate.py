import sqlite3  
import cv2
import numpy as np

def export_data_column(db3_file, table_name, column_name, topic_id):  
    conn = sqlite3.connect(db3_file)  
    cursor = conn.cursor()  
  
    # 构建包含 WHERE 子句的 SQL 查询  
    query = f"SELECT id, {column_name} FROM {table_name} WHERE topic_id = {topic_id};"  
    cursor.execute(query)  
    rows = cursor.fetchall()  
  
    # 打印或处理查询结果  
    for row in rows:  
        record_id, data = row  
        print(f"Processing record ID: {record_id}, data type: {type(data)}")  # 确认数据类型为 bytes
        # print(data[:50])
        # break
        # 将字节数据转换为 NumPy 数组
        np_data = np.frombuffer(data, np.uint8)

        # 查找JPEG文件头
        start_idx = np_data.tobytes().find(b'\xff\xd8')
        end_idx = np_data.tobytes().rfind(b'\xff\xd9') + 2
        
        if start_idx != -1 and end_idx != -1:
            # 提取JPEG数据部分
            jpeg_data = np_data[start_idx:end_idx]
            image = cv2.imdecode(jpeg_data, cv2.IMREAD_COLOR)  
            
            if image is not None:
                output_filename = f'./images/image_{record_id}.png'
                # 转换 BGR 到 RGB
                image_rgb = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
                cv2.imwrite(output_filename, image_rgb)  # 保存为PNG 
                print(f"Image saved successfully as {output_filename}, size: {image.shape[1]}x{image.shape[0]}")
            else:
                print(f"Failed to decode image for record ID {record_id}")
        else:
            print(f"JPEG markers not found for record ID {record_id}")
  
    conn.close()  
  
# 使用示例  
db3_file = 'mini_0.db3'  
topic_id = 13  
print(f"Extracting data from {db3_file} where topic_id = {topic_id}")  
export_data_column(db3_file, 'messages', 'data', topic_id)
