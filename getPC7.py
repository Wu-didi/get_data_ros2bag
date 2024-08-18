import sqlite3
import numpy as np
import open3d as o3d

def extract_pointcloud_from_data(data, num_points):
    # 获取最后 m 个点的数据
    point_data = data[-num_points*16:]
    
    # 将二进制数据转换为 float32 数组，并 reshape 成 (n, 4) 形状
    points = np.frombuffer(point_data, dtype=np.float32).reshape(-1, 4)
    print(points)
    return points

def export_lidar_data(db3_file, table_name, column_name, topic_id, num_points):
    conn = sqlite3.connect(db3_file)
    cursor = conn.cursor()

    query = f"SELECT id, {column_name} FROM {table_name} WHERE topic_id = {topic_id};"
    cursor.execute(query)
    rows = cursor.fetchall()

    for row in rows:
        record_id, data = row
        print(f"Processing record ID: {record_id}, data type: {type(data)}, data size: {len(data)} bytes")

        points = extract_pointcloud_from_data(data, num_points)
        print(f"Parsed points shape: {points.shape}")
        
        if points.size > 0:
            point_cloud = o3d.geometry.PointCloud()
            point_cloud.points = o3d.utility.Vector3dVector(points[:, :3])  # 使用 XYZ 坐标
            
            # 将 intensity 转换为颜色（灰度）
            # intensity_normalized = (points[:, 3] - points[:, 3].min()) / (points[:, 3].max() - points[:, 3].min())
            # colors = np.repeat(intensity_normalized[:, np.newaxis], 3, axis=1)  # 将 intensity 重复三次，作为 RGB 三个通道的值
            # point_cloud.colors = o3d.utility.Vector3dVector(colors)

            output_filename = f'pointcloud_{record_id}.pcd'
            o3d.io.write_point_cloud(output_filename, point_cloud)
            print(f"Point cloud saved successfully as {output_filename}, points count: {points.shape[0]}")
        else:
            print(f"Failed to parse point cloud for record ID {record_id}")
    
    conn.close()

# 使用示例
db3_file = 'mini_0.db3'  # 你的 .db3 文件路径
topic_id = 2  # 激光雷达数据对应的 topic_id
num_points = 10000  # 你预计每帧点云的点数，根据你的数据调整
export_lidar_data(db3_file, 'messages', 'data', topic_id, num_points)
