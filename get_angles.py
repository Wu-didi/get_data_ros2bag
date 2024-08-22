# 根据输入的旋转矩阵，计算对应的欧拉角（Z-Y-X顺序）。

import numpy as np

def rotation_matrix_to_euler_angles(R):
    """
    将旋转矩阵转换为欧拉角（Z-Y-X顺序）。
    参数:
        R: 3x3的旋转矩阵
    返回:
        欧拉角（roll, pitch, yaw），单位为弧度
    """
    assert R.shape == (3, 3), "输入的旋转矩阵必须是3x3的矩阵"
    
    sy = np.sqrt(R[0, 0] ** 2 + R[1, 0] ** 2)

    singular = sy < 1e-6

    if not singular:
        roll = np.arctan2(R[2, 1], R[2, 2])
        pitch = np.arctan2(-R[2, 0], sy)
        yaw = np.arctan2(R[1, 0], R[0, 0])
    else:
        roll = np.arctan2(-R[1, 2], R[1, 1])
        pitch = np.arctan2(-R[2, 0], sy)
        yaw = 0

    return roll, pitch, yaw

import numpy as np

def rotation_matrix_to_euler_angles_xyz(R):
    """
    将旋转矩阵转换为欧拉角（X-Y-Z顺序）。
    参数:
        R: 3x3的旋转矩阵
    返回:
        欧拉角（roll, pitch, yaw），单位为弧度
    """
    assert R.shape == (3, 3), "输入的旋转矩阵必须是3x3的矩阵"
    
    # 判断奇异性
    sy = np.sqrt(R[0, 0] ** 2 + R[0, 1] ** 2)
    
    singular = sy < 1e-6

    if not singular:
        roll = np.arctan2(R[2, 1], R[2, 2])
        pitch = np.arctan2(-R[2, 0], sy)
        yaw = np.arctan2(R[1, 0], R[0, 0])
    else:
        roll = np.arctan2(-R[1, 2], R[1, 1])
        pitch = np.arctan2(-R[2, 0], sy)
        yaw = 0

    return roll, pitch, yaw



# 示例旋转矩阵
# R = np.array([[0.866, -0.5, 0],
#               [0.5, 0.866, 0],
#               [0, 0, 1]])

## ========================================prev================================================================
R = np.array([[0.999986,0.00523596,0],
             [-0.00523568,0.999931,-0.0104718],
             [-5.48299e-05,0.0104716,0.999945]])
# Roll (X-axis rotation): 0.010471793179343243
# Pitch (Y-axis rotation): 5.482991605021908e-05
# Yaw (Z-axis rotation): -0.005235705458568137
# ----
# Roll (X-axis rotation): 0.010471793179343243
# Pitch (Y-axis rotation): 5.482991596983675e-05
# Yaw (Z-axis rotation): -0.005235705458568137

##========================================= left================================================================
# R = np.array([[0.99768,-0.0679143,0.00476084],
#               [0.0680631,0.99658,-0.046869],
#               [-0.00156148,0.0470843,0.99889]])
# Roll (X-axis rotation): 0.047101757773838245
# Pitch (Y-axis rotation): 0.001561480317401298
# Yaw (Z-axis rotation): 0.06811583052446617

##========================================= right================================================================
# R = np.array([[0.995562,0.0941083,0],
#               [-0.0941083,0.995562,0],
#               [0,0,1]])

# Roll (X-axis rotation): 0.0
# Pitch (Y-axis rotation): -0.0
# Yaw (Z-axis rotation): -0.09424776301714366

# 计算欧拉角
# roll, pitch, yaw = rotation_matrix_to_euler_angles(R)

# print("Roll (X-axis rotation):", roll)
# print("Pitch (Y-axis rotation):", pitch)
# print("Yaw (Z-axis rotation):", yaw)


##========================================= back================================================================
# R = np.array([[0.99991533,0.01025854,0.008806088],
# [-0.01004018,0.00059326,-0.02668502],
# [-0.00833135,0.02660183,0.99961163]
# ])
# Roll (X-axis rotation): 0.026605885724489985
# Pitch (Y-axis rotation): 0.008331424238516962
# Yaw (Z-axis rotation): -0.010040692741228534

# 0.99991533,0.01025854,0.008806088,0.04187360
# -0.01004018,0.00059326,-0.02668502,0.00163908
# -0.00833135,0.02660183,0.99961163,0.04877515


##=========================================back 24 =============================================================
R = np.array([[0.999989,-0.00467932,4.02439e-05],
[0.00467949,0.999952,-0.00859997],
[0,0.00860007,0.999963]])
# Roll (X-axis rotation): 0.0086001761763938
# Pitch (Y-axis rotation): -0.0
# Yaw (Z-axis rotation): 0.004679507317702825
# 计算欧拉角
roll, pitch, yaw = rotation_matrix_to_euler_angles_xyz(R)

print("Roll (X-axis rotation):", roll)
print("Pitch (Y-axis rotation):", pitch)
print("Yaw (Z-axis rotation):", yaw)