import sqlite3
import struct
import numpy as np
import open3d as o3d

def parse_fixed_format_pointcloud(data, point_step, num_points):
    points = []
    for i in range(num_points):
        offset = i * point_step
        x, y, z, intensity = struct.unpack_from('ffff', data, offset)
        points.append([x, y, z])
    return np.array(points)

def export_lidar_data(db3_file, table_name, column_name, topic_id):
    conn = sqlite3.connect(db3_file)
    cursor = conn.cursor()

    query = f"SELECT id, {column_name} FROM {table_name} WHERE topic_id = {topic_id};"
    cursor.execute(query)
    rows = cursor.fetchall()

    for row in rows:
        record_id, data = row
        print(f"Processing record ID: {record_id}, data type: {type(data)}, data size: {len(data)} bytes")

        # 假设点云数据的格式是固定的
        header_size = 56
        point_step = 16  # 4个float32: x, y, z, intensity
        num_points = (len(data) - header_size) // point_step

        points = parse_fixed_format_pointcloud(data[header_size:], point_step, num_points)

        if points.size > 0:
            point_cloud = o3d.geometry.PointCloud()
            point_cloud.points = o3d.utility.Vector3dVector(points)
            
            output_filename = f'pointcloud_{record_id}.pcd'
            o3d.io.write_point_cloud(output_filename, point_cloud)
            print(f"Point cloud saved successfully as {output_filename}, points count: {points.shape[0]}")
        else:
            print(f"Failed to parse point cloud for record ID {record_id}")
    
    conn.close()

# 使用示例
db3_file = 'mini_0.db3'
topic_id = 3  # 这是激光雷达数据
export_lidar_data(db3_file, 'messages', 'data', topic_id)

