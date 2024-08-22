# 从ros2 录制的数据包里面，解析出图像和点云数据，并找到时间戳最近的不同传感器帧，如果超过不同传感器之间的时间戳差值超过阈值，就跳过。
# 同时设置了保存时间间隔，每隔0.33秒保存一次。
import sqlite3
import numpy as np
import os
import cv2
import time
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
from numba import jit

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
        f"TYPE F F F F\n"
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

def save_image(image, filename):
    cv2.imwrite(filename, image)

def extract_timestamps(cursor, table_name, column_stamp, topic_id):
    query = f"SELECT {column_stamp} FROM {table_name} WHERE topic_id = {topic_id};"
    cursor.execute(query)
    timestamps = [row[0] for row in cursor.fetchall()]
    return sorted(timestamps)  # 返回排序后的时间戳列表

def find_closest_timestamp(target_timestamp, timestamps, threshold):
    closest_timestamp = None
    min_diff = float('inf')
    
    for timestamp in timestamps:
        diff = abs(target_timestamp - timestamp)
        print(f"diff: {diff}")
        if diff < min_diff:
            min_diff = diff
            closest_timestamp = timestamp

    return closest_timestamp if min_diff <= threshold else None

def export_sensor_data(cursor, table_name, column_name, column_stamp, topic_id, timestamp, sensor_type):
    query = f"SELECT {column_name} FROM {table_name} WHERE topic_id = {topic_id} AND {column_stamp} = {timestamp};"
    cursor.execute(query)
    data = cursor.fetchone()
    
    if data and isinstance(data[0], bytes):
        if sensor_type == 'lidar':
            return parse_pointcloud2(data[0])
        elif sensor_type == 'camera':
            np_data = np.frombuffer(data[0], np.uint8)
            start_idx = np_data.tobytes().find(b'\xff\xd8')
            end_idx = np_data.tobytes().rfind(b'\xff\xd9') + 2
            
            if start_idx != -1 and end_idx != -1:
                jpeg_data = np_data[start_idx:end_idx]
                image = cv2.imdecode(jpeg_data, cv2.IMREAD_COLOR)
                if image is not None:
                    image_rgb = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
                    return image_rgb
    return None

def get_topic_id(db3_file, topic_name_list):
    results_dict = {}
    conn = sqlite3.connect(db3_file)
    cursor = conn.cursor()

    for topic_name in topic_name_list:
        query = "SELECT id FROM topics WHERE name = ?;"
        cursor.execute(query, (topic_name,))
        result = cursor.fetchone()
        if result:
            results_dict[topic_name] = result[0]
        else:
            raise ValueError(f"无法找到指定的topic_name: {topic_name}")
    
    conn.close()
    return results_dict

def process_sensor_data(db3_file, topic_name_list, save_folder, time_threshold=500000000, save_interval=0.33):
    conn = sqlite3.connect(db3_file)
    cursor = conn.cursor()

    topic_id_dict = get_topic_id(db3_file, topic_name_list)
    print(topic_id_dict)
    top_timestamps = extract_timestamps(cursor, 'messages', 'timestamp', topic_id_dict['/rslidar_points_top'])
    print(f"共有{len(top_timestamps)}个时间戳")
    print(top_timestamps)
    last_save_time = 0    
    for top_timestamp in top_timestamps:
        if top_timestamp - last_save_time < save_interval * 1e9:
            continue
        closest_timestamps = {}

        all_within_threshold = True  # 是否所有topic的时间戳都在阈值范围内
        combined_points = []

        # 遍历所有topic，找到与top_timestamp最接近的时间戳
        for topic_name, topic_id in topic_id_dict.items():
            print(f"正在处理{topic_name}")
            if topic_name == '/rslidar_points_top':
                closest_timestamps['/rslidar_points_top'] = top_timestamp
                # continue
            closest_timestamp = find_closest_timestamp(top_timestamp, extract_timestamps(cursor, 'messages', 'timestamp', topic_id), time_threshold)
            print(f"top_timestamp: {top_timestamp}, closest_timestamp: {closest_timestamp}")
            if closest_timestamp is None:
                all_within_threshold = False
                break
            closest_timestamps[topic_name] = closest_timestamp
        
        if all_within_threshold:
            print("here is closet timestamps: ",closest_timestamps)
            
            # 将同一时间戳的数据保存到一个文件夹中，以top_timestamp为文件名
            output_folder_same_frame = os.path.join(save_folder, str(top_timestamp))
            os.makedirs(output_folder_same_frame, exist_ok=True)
            for topic_name, timestamp in closest_timestamps.items():
                sensor_type = 'lidar' if 'rslidar' in topic_name else 'camera'
                sensor_data = export_sensor_data(cursor, 'messages', 'data', 'timestamp', topic_id_dict[topic_name], timestamp, sensor_type)
                sensor_name = topic_name.split('/')[-1]

                if sensor_type == 'lidar' and sensor_data is not None:
                    combined_points.append(sensor_data)
                    # output_folder = os.path.join(save_folder, sensor_name)
                    # os.makedirs(output_folder, exist_ok=True)
                    output_filename = os.path.join(output_folder_same_frame, f'{sensor_name}_{timestamp}.pcd')
                    save_pcd(sensor_data, output_filename)
                elif sensor_type == 'camera' and sensor_data is not None:
                    # output_folder = os.path.join(save_folder, sensor_name)
                    # os.makedirs(output_folder, exist_ok=True)
                    output_filename = os.path.join(output_folder_same_frame, f'{sensor_name}_{timestamp}.png')
                    save_image(sensor_data, output_filename)

            if combined_points:
                combined_points = np.vstack(combined_points)  # 合并所有点云数据
                # output_folder = os.path.join(save_folder, 'combined_pointclouds')
                # os.makedirs(output_folder, exist_ok=True)
                combined_filename = os.path.join(output_folder_same_frame, 'combined_pointclouds_'+f'{top_timestamp}.pcd')
                save_pcd(combined_points, combined_filename)
            
            last_save_time = top_timestamp

    conn.close()

# 使用示例
db3_file = './004/004_0.db3'
topic_name_list = [
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
save_folder = './calib_lidar2img/004_3'

# 处理数据
process_sensor_data(db3_file, topic_name_list, save_folder, time_threshold=30000000, save_interval=0.33)  # 50ms的时间阈值
