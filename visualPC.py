import open3d as o3d
import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D

def plot_point_cloud(x, y, z, color='r', marker='o', xlabel='X Axis', ylabel='Y Axis', zlabel='Z Axis'):
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

def visualize_point_cloud(pcd_filename):
    # 读取点云文件
    point_cloud = o3d.io.read_point_cloud(pcd_filename)
    
    if len(point_cloud.points) == 0:
        print("Point cloud is empty!")
        return

    # 转换点云点为numpy数组
    points = np.asarray(point_cloud.points)
    
    # 过滤掉包含NaN值的点
    valid_mask = ~np.isnan(points).any(axis=1)
    filtered_points = points[valid_mask]

    print(f"Number of valid points: {len(filtered_points)}")
    # 打印每个点的坐标
    for i, point in enumerate(filtered_points):
        print(f"Point {i}: x={point[0]}, y={point[1]}, z={point[2]}")
    
    # 做一个过滤，如果x轴大于5的点，就不显示
    filtered_points = filtered_points[filtered_points[:, 0] <= 5]
    
        
    if len(filtered_points) > 0:
        # 将点云中心移动到原点附近
        point_cloud.points = o3d.utility.Vector3dVector(filtered_points)

        # 将点云的颜色设置为红色
        point_cloud.paint_uniform_color([1, 0, 0])

        # 创建可视化窗口并设置点的大小
        vis = o3d.visualization.Visualizer()
        vis.create_window()
        vis.add_geometry(point_cloud)

        # 设置点的大小（点云显示的尺寸）
        opt = vis.get_render_option()
        opt.point_size = 5.0  # 将点的大小设置为5.0

        # 获取控制视角的对象
        ctr = vis.get_view_control()
        ctr.set_zoom(0.8)  # 调整缩放比例

        # 运行可视化
        vis.run()
        vis.destroy_window()
    else:
        print("No valid points available after filtering out NaN values.")



# 使用示例
pcd_filename = 'pointclouds/pointcloud_1_16.pcd'  # 替换为实际的PCD文件名
visualize_point_cloud(pcd_filename)
