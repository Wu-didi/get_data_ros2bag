import open3d as o3d
import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D

def get_pcd(filename_list):
    x,y,z = [],[],[]
    # 读取点云文件
    for filename in filename_list:
        point_cloud = o3d.io.read_point_cloud(filename)

        # 获取点云的点坐标
        points = np.asarray(point_cloud.points)

        # 打印每个点的坐标
        for i, point in enumerate(points):
            # 不等于nan的点
            if np.isnan(point).any():
                continue
            if point[0] >5:
                continue
            print(f"Point {i}: x={point[0]}, y={point[1]}, z={point[2]}")
            x.append(point[0])
            y.append(point[1])
            z.append(point[2])
    return x,y,z




def plot_point_cloud(x, y, z, color='r', marker='o',s=1, xlabel='X Axis', ylabel='Y Axis', zlabel='Z Axis'):
    """
    Plots a 3D point cloud.

    Parameters:
    x (list or array-like): X coordinates of the points.
    y (list or array-like): Y coordinates of the points.
    z (list or array-like): Z coordinates of the points.
    color (str): Color of the points. Default is 'r' (red).
    marker (str): Marker style for the points. Default is 'o'.
    xlabel (str): Label for the X axis. Default is 'X Axis'.
    ylabel (str): Label for the Y axis. Default is 'Y Axis'.
    zlabel (str): Label for the Z axis. Default is 'Z Axis'.

    Returns:
    None
    """
    # Create a 3D plot
    fig = plt.figure()
    ax = fig.add_subplot(111, projection='3d')

    # Plot the points
    ax.scatter(x, y, z, c=color, marker=marker)

    # Labels for axes
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.set_zlabel(zlabel)

    # Show plot
    plt.show()

# Example usage:
# x = [1, 2, 3, 4, 5]
# y = [2, 3, 4, 5, 6]
# z = [5, 6, 7, 8, 9]
# plot_point_cloud(x, y, z)


# 使用示例
pcd_filename = 'pointcloud_27.pcd'  # 替换为实际保存的文件名
pcd_filename_list = ['./pointclouds/pointcloud_1_16.pcd','./pointclouds/pointcloud_2_28.pcd','./pointclouds/pointcloud_3_17.pcd']
x,y,z = get_pcd(pcd_filename_list)
plot_point_cloud(x, y, z,  s=1)

