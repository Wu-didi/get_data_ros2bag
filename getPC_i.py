import sqlite3
import struct
import numpy as np

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

def export_lidar_data(db3_file, table_name, column_name, topic_id):
    conn = sqlite3.connect(db3_file)
    cursor = conn.cursor()

    query = f"SELECT id, {column_name} FROM {table_name} WHERE topic_id = {topic_id};"
    cursor.execute(query)
    rows = cursor.fetchall()

    for row in rows:
        record_id, data = row
        print(f"正在处理记录ID: {record_id}, 数据类型: {type(data)}")
        if isinstance(data, bytes):
            print("data is bytes ")
            points = parse_pointcloud2(data)
            print(f"从记录ID {record_id} 中解析出 {points.shape[0]} 个点")
            
            if points.size > 0:
                output_filename = f'./pointclouds/pointcloud_{topic_id}_{record_id}.pcd'
                save_pcd(points, output_filename)
                print(f"点云成功保存为 {output_filename}, 点数: {points.shape[0]}")
            else:
                print(f"无法解析记录ID {record_id} 的点云")
        else:
            print(f"记录ID {record_id} 的数据不是字节格式。")
    
    conn.close()

# 使用示例
db3_file = 'mini_0.db3'
topic_id = 1  # 这是激光雷达数据
export_lidar_data(db3_file, 'messages', 'data', topic_id)
