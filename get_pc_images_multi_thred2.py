import sqlite3
import numpy as np
import os 
import cv2
import time
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
from numba import jit

# 使用 Numba JIT 编译器加速该函数
@jit(nopython=True)
def parse_pointcloud2(data):
    header_size = 56  # ROS2中Header的长度
    point_step = 16   # 假设每个点包含XYZ和intensity（4个float）
    num_points = (len(data) - header_size) // point_step
    points = np.empty((num_points, 4), dtype=np.float32)
    
    for i in range(num_points):
        offset = header_size + i * point_step
        # 手动解析4个float字段：x, y, z, intensity
        intensity = np.frombuffer(data[offset:offset+4], dtype=np.float32)[0]
        x = np.frombuffer(data[offset+4:offset+8], dtype=np.float32)[0]
        y = np.frombuffer(data[offset+8:offset+12], dtype=np.float32)[0]
        z = np.frombuffer(data[offset+12:offset+16], dtype=np.float32)[0]
        
        points[i, 0] = x
        points[i, 1] = y
        points[i, 2] = z
        points[i, 3] = intensity
    
    return points

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

def export_lidar_data(db3_file, table_name, column_name, column_stamp, topic_id, lidar_name, out_file='./pointcloud'):
    conn = sqlite3.connect(db3_file)
    cursor = conn.cursor()

    lidar_name = lidar_name.split("/")[-1]
    output_file = os.path.join(out_file, lidar_name)
    if not os.path.exists(output_file):  
        os.makedirs(output_file)

    offset = 0
    batch_size = 100  # 每次从数据库中读取1000条记录

    while True:
        query = f"SELECT id, {column_name}, {column_stamp} FROM {table_name} WHERE topic_id = {topic_id} LIMIT {batch_size} OFFSET {offset};"
        cursor.execute(query)
        rows = cursor.fetchall()
        if not rows:
            break
        offset += batch_size

        for row in rows:
            record_id, data, timestamp = row
            if isinstance(data, bytes):
                points = parse_pointcloud2(data)
                if points.size > 0:
                    output_filename = os.path.join(output_file, f'{lidar_name}_{timestamp}.pcd')
                    save_pcd(points, output_filename)
    conn.close()

def export_data_column(db3_file, table_name, column_name, column_stamp, topic_id, carm_name, out_file = './images'):
    conn = sqlite3.connect(db3_file)
    cursor = conn.cursor()

    carm_name = carm_name.split("/")[-1]
    output_file = os.path.join(out_file, carm_name)
    if not os.path.exists(output_file):  
        os.makedirs(output_file)

    offset = 0
    batch_size = 100  # 每次从数据库中读取1000条记录

    while True:
        query = f"SELECT id, {column_name}, {column_stamp} FROM {table_name} WHERE topic_id = {topic_id} LIMIT {batch_size} OFFSET {offset};"
        cursor.execute(query)
        rows = cursor.fetchall()
        if not rows:
            break
        offset += batch_size

        for row in rows:
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
    conn.close()

def get_topic_id(db3_file, topic_name_list):
    results_dict = {}
    conn = sqlite3.connect(db3_file)
    cursor = conn.cursor()

    for topic_name in topic_name_list:
        query = "SELECT id FROM topics WHERE name = ?;"
        cursor.execute(query, (topic_name,))
        result = cursor.fetchone()
        results_dict[topic_name] = result[0]
    
    conn.close()
    if results_dict:
        return results_dict
    else:
        raise ValueError(f"无法找到指定的topic_name: {topic_name}")
    
# 使用示例
start_time = time.time()

db3_file = f'./003/003_0.db3'
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

# 每个线程中独立获取topic_id
topic_id_dict = get_topic_id(db3_file, topic_name_list)

with ThreadPoolExecutor() as executor:
    futures = []
    for sensor_topic_name, sensor_topic_id in topic_id_dict.items():
        if 'image' in sensor_topic_name:
            futures.append(executor.submit(export_data_column, db3_file, 'messages', 'data','timestamp', sensor_topic_id, sensor_topic_name, './calib_lidar2img/003/images'))
        else:
            futures.append(executor.submit(export_lidar_data, db3_file, 'messages', 'data','timestamp', sensor_topic_id, sensor_topic_name, './calib_lidar2img/003/pointclouds'))
    
    for future in tqdm(futures, desc="Processing all topics"):
        future.result()

end_time = time.time()
print(f"Total time taken: {end_time - start_time:.2f} seconds")
