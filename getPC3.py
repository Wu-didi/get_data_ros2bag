import sqlite3
import struct
import numpy as np
import open3d as o3d  # 可视化和处理点云（需要安装 open3d）

def parse_pointcloud2(data):
    header_size = 56  # ROS2中Header的长度，通常是56字节
    point_step = 16   # 每个点的数据长度（假设为XYZI）
    num_points = (len(data) - header_size) // point_step
    
    points = []
    for i in range(num_points):
        offset = header_size + i * point_step
        # 假设点云数据是小端（<）并包含4个float字段：x, y, z, intensity
        x, y, z, intensity = struct.unpack_from('<ffff', data, offset)
        points.append([x, y, z, intensity])
    
    return np.array(points)

def export_lidar_data(db3_file, table_name, column_name, topic_id):
    conn = sqlite3.connect(db3_file)
    cursor = conn.cursor()

    query = f"SELECT id, {column_name} FROM {table_name} WHERE topic_id = {topic_id};"
    cursor.execute(query)
    rows = cursor.fetchall()

    for row in rows:
        record_id, data = row
        print(f"Processing record ID: {record_id}, data type: {type(data)}")

        if isinstance(data, bytes):
            points = parse_pointcloud2(data)
            print(f"Parsed {points.shape[0]} points from record ID {record_id}")
            
            if points.size > 0:
                # 使用 Open3D 保存点云数据
                point_cloud = o3d.geometry.PointCloud()
                point_cloud.points = o3d.utility.Vector3dVector(points[:, :3])  # 使用 XYZ 坐标
                
                output_filename = f'./pointclouds/pointcloud_{topic_id}_{record_id}.pcd'
                o3d.io.write_point_cloud(output_filename, point_cloud)
                print(f"Point cloud saved successfully as {output_filename}, points count: {points.shape[0]}")
            else:
                print(f"Failed to parse point cloud for record ID {record_id}")
        else:
            print(f"Data for record ID {record_id} is not in bytes format.")
    
    conn.close()

# 使用示例
db3_file = 'mini_0.db3'
topic_id = 3  # 这是激光雷达数据
export_lidar_data(db3_file, 'messages', 'data', topic_id)
