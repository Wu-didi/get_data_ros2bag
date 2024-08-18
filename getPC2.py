import sqlite3
import struct
import numpy as np
import open3d as o3d

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
            points = parse_pointcloud2(data)
            print(f"从记录ID {record_id} 中解析出 {points.shape[0]} 个点")
            
            if points.size > 0:
                # 使用 Open3D 保存点云数据
                point_cloud = o3d.geometry.PointCloud()
                point_cloud.points = o3d.utility.Vector3dVector(points[:, :3])  # 仅使用 XYZ 坐标
                
                output_filename = f'./pointclouds/pointcloud_{topic_id}_{record_id}.pcd'
                o3d.io.write_point_cloud(output_filename, point_cloud)
                print(f"点云成功保存为 {output_filename}, 点数: {points.shape[0]}")
            else:
                print(f"无法解析记录ID {record_id} 的点云")
        else:
            print(f"记录ID {record_id} 的数据不是字节格式。")
    
    conn.close()


def get_topic_id(db3_file, topic_name):
    conn = sqlite3.connect(db3_file)
    cursor = conn.cursor()

    # 从 `topic` 表中获取指定 `topic_name` 的 `topic_id`
    query = "SELECT id FROM topics WHERE name = ?;"
    cursor.execute(query, (topic_name,))
    result = cursor.fetchone()
    
    conn.close()

    if result:
        return result[0]
    else:
        raise ValueError(f"无法找到指定的topic_name: {topic_name}")
    
    
    
    
# 使用示例
db3_file = 'mini_0.db3'
topic_name =[
            '/rslidar_points_right', 
            '/rslidar_points_left',
            '/rslidar_points_top',
            '/rslidar_points_prev',
            '/rslidar_points_back',
            # '/image0',
            # '/image1',
            # '/image2',
            # '/image3',
            # '/image4',
            # '/image5',
            # '/image6',
            ]

# 获取对应topic的ID
try:
    topic_id = get_topic_id(db3_file, topic_name)
    print(f"找到topic_id: {topic_id} 对应topic_name: {topic_name}")
    
    # 导出点云数据
    export_lidar_data(db3_file, 'messages', 'data', topic_id)
except ValueError as e:
    print(e)