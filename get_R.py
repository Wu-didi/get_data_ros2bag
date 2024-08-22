# 根据输入的欧拉角计算出旋转矩阵




import numpy as np

rx, ry, rz = 0.0, -0.4, -105.2
  
# 将角度从度数转换为弧度
rx_rad = np.deg2rad(rx)
ry_rad = np.deg2rad(ry)
rz_rad = np.deg2rad(rz)

# 计算各个轴的旋转矩阵
R_x = np.array([[1, 0, 0],
                [0, np.cos(rx_rad), -np.sin(rx_rad)],
                [0, np.sin(rx_rad), np.cos(rx_rad)]])

R_y = np.array([[np.cos(ry_rad), 0, np.sin(ry_rad)],
                [0, 1, 0],
                [-np.sin(ry_rad), 0, np.cos(ry_rad)]])

R_z = np.array([[np.cos(rz_rad), -np.sin(rz_rad), 0],
                [np.sin(rz_rad), np.cos(rz_rad), 0],
                [0, 0, 1]])

# 计算总的旋转矩阵
R = np.dot(R_z, np.dot(R_y, R_x))


# 计算旋转矩阵的逆矩阵
# R_inv = np.linalg.inv(R)

print("旋转矩阵 R: \n", R)
# print("\n旋转矩阵的逆矩阵 R_inv: \n", R_inv)
