import sqlite3
import struct
import numpy as np
import os 
import cv2
import time
from tqdm import tqdm

def parse_pointcloud2(data):
    header_size = 56  # ROS2中Header的长度
    point_step = 16   # 假设每个点包含XYZ和intensity（4个float）
    num_points = (len(data) - header_size) // point_step
    points = []
    for i in range(num_points):
        offset = header_size + i * point_step
        # 假设点云数据是小端（<）并包含4个float字段：x, y, z, intensity
        intensity, x, y, z  = struct.unpack_from('<ffff', data, offset)
        points.append([x, y, z, intensity])
    
    return np.array(points)

def save_pcd(points, filename):
    num_points = points.shape[0]
    header = (
        f"# .PCD v0.7 - Point Cloud Data file format\n"
        f"VERSION 0.7\n"
        f"FIELDS x y z intensity\n"
        f"SIZE 4 4 4 4\n"
        f"TYPE F F F I\n"
        f"COUNT 1 1 1 1\n"
        f"WIDTH {num_points}\n"
        f"HEIGHT 1\n"
        f"VIEWPOINT 0 0 0 1 0 0 0\n"
        f"POINTS {num_points}\n"
        f"DATA ascii\n"
    )
    
    with open(filename, 'w') as f:
        f.write(header)
        for point in points:
            f.write(f"{point[0]} {point[1]} {point[2]} {point[3]}\n")

def export_lidar_data(cursor, table_name, column_name, column_stamp, topic_id, lidar_name, out_file='./pointcloud'):
    lidar_name = lidar_name.split("/")[-1]
    output_file = os.path.join(out_file, lidar_name)
    # 检查目录是否存在，如果不存在则创建  
    if not os.path.exists(output_file):  
        os.makedirs(output_file)  

    query = f"SELECT id, {column_name}, {column_stamp} FROM {table_name} WHERE topic_id = {topic_id};"
    cursor.execute(query)
    rows = cursor.fetchall()

    for row in tqdm(rows, desc=f'Processing Lidar {lidar_name}'):
        record_id, data, timestamp = row
        if isinstance(data, bytes):
            points = parse_pointcloud2(data)
            if points.size > 0:
                output_filename = os.path.join(output_file, f'{lidar_name}_{timestamp}.pcd')
                save_pcd(points, output_filename)
            else:
                print(f"无法解析记录ID {record_id} 的点云")
        else:
            print(f"记录ID {record_id} 的数据不是字节格式。")

def export_data_column(cursor, table_name, column_name, column_stamp, topic_id, carm_name, out_file = './images'):
    
    carm_name = carm_name.split("/")[-1]
    output_file = os.path.join(out_file, carm_name)
    # 检查目录是否存在，如果不存在则创建  
    if not os.path.exists(output_file):  
        os.makedirs(output_file)  

    query = f"SELECT id, {column_name}, {column_stamp} FROM {table_name} WHERE topic_id = {topic_id};"
    cursor.execute(query)
    rows = cursor.fetchall()  
  
    for row in tqdm(rows, desc=f'Processing Camera {carm_name}'):  
        record_id, data, timestamp = row
        np_data = np.frombuffer(data, np.uint8)

        start_idx = np_data.tobytes().find(b'\xff\xd8')
        end_idx = np_data.tobytes().rfind(b'\xff\xd9') + 2
        
        if start_idx != -1 and end_idx != -1:
            jpeg_data = np_data[start_idx:end_idx]
            image = cv2.imdecode(jpeg_data, cv2.IMREAD_COLOR)  
  
            if image is not None:
                output_filename = os.path.join(output_file, f'{carm_name}_{timestamp}.png')
                image_rgb = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
                cv2.imwrite(output_filename, image_rgb)
            else:
                print(f"Failed to decode image for record ID {record_id}")
        else:
            print(f"JPEG markers not found for record ID {record_id}")
  
def get_topic_id(cursor, topic_name_list):
    results_dict = {}
    for topic_name in topic_name_list:
        query = "SELECT id FROM topics WHERE name = ?;"
        cursor.execute(query, (topic_name,))
        result = cursor.fetchone()
        results_dict[topic_name] = result[0]
    
    if results_dict:
        return results_dict
    else:
        raise ValueError(f"无法找到指定的topic_name: {topic_name}")
    
# 使用示例
start_time = time.time()


db3_file = f'./004/004_0.db3'
topic_name_list =[
            '/rslidar_points_right', 
            '/rslidar_points_left',
            '/rslidar_points_top',
            '/rslidar_points_prev',
            '/rslidar_points_back',
            '/image0',
            '/image1',
            '/image2',
            '/image3',
            '/image4',
            '/image5',
            '/image6',
            ]

conn = sqlite3.connect(db3_file)  
cursor = conn.cursor()  

topic_id_dict = get_topic_id(cursor, topic_name_list)
print(topic_id_dict)
for sensor_topic_name, sensor_topic_id in topic_id_dict.items():
    if 'image' in sensor_topic_name:
        export_data_column(cursor, 'messages', 'data','timestamp', sensor_topic_id, sensor_topic_name, out_file='./calib_lidar2img/004/images' )
    else:
        export_lidar_data(cursor, 'messages', 'data','timestamp', sensor_topic_id, sensor_topic_name, out_file='./calib_lidar2img/004/pointclouds')

conn.close()

end_time = time.time()
print(f"Total time taken: {end_time - start_time:.2f} seconds")
