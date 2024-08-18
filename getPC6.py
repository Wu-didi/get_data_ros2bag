import sqlite3
import struct
import numpy as np
import open3d as o3d

def parse_pointcloud2(data, height, width, point_step, fields):
    points = []
    for row in range(height):
        for col in range(width):
            offset = row * width * point_step + col * point_step
            point = []
            for field in fields:
                field_offset = offset + field['offset']
                if field['datatype'] == 7:  # FLOAT32
                    value = struct.unpack_from('f', data, field_offset)[0]
                # 其他数据类型可以在这里扩展
                point.append(value)
            points.append(point)
    
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

        header_size = 56  # 确定的头部长度
        height, width, point_step, row_step = 15750, 5, 16, 80
        
        fields = [
            {'name': 'x', 'offset': 0, 'datatype': 7, 'count': 1},
            {'name': 'y', 'offset': 4, 'datatype': 7, 'count': 1},
            {'name': 'z', 'offset': 8, 'datatype': 7, 'count': 1},
            {'name': 'intensity', 'offset': 12, 'datatype': 7, 'count': 1}
        ]
        
        # 解析点云数据
        points = parse_pointcloud2(data[header_size:], height, width, point_step, fields)
        print(f"Parsed points shape: {points.shape}")
        
        if points.size > 0:
            point_cloud = o3d.geometry.PointCloud()
            point_cloud.points = o3d.utility.Vector3dVector(points[:, 1:4])  # 使用 XYZ 坐标
            
            # 将 intensity 转换为颜色（灰度）
            intensity_normalized = (points[:, 0:1] - points[:, 0:1].min()) / (points[:, 0:1].max() - points[:, 0:1].min())
            colors = np.repeat(intensity_normalized, 3, axis=1)  # 将 intensity 重复三次，作为 RGB 三个通道的值
            point_cloud.colors = o3d.utility.Vector3dVector(colors)

            output_filename = f'pointcloud_{record_id}.pcd'
            o3d.io.write_point_cloud(output_filename, point_cloud)
            print(f"Point cloud saved successfully as {output_filename}, points count: {points.shape[0]}")
        else:
            print(f"Failed to parse point cloud for record ID {record_id}")
    
    conn.close()

# 使用示例
db3_file = 'mini_0.db3'  # 你的 .db3 文件路径
topic_id = 1  # 激光雷达数据对应的 topic_id
export_lidar_data(db3_file, 'messages', 'data', topic_id)
