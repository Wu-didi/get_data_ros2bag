import pandas as pd

# 读取txt文件中的n数据
with open('can.txt', 'r') as file:
    lines = file.readlines()
# 解析每一行，去掉换行符并按空格分隔
# for line in lines:
#     if line.strip().split()[16] == '03':
#         print(line.strip().split()[51:59])  
    


parsed_data = [line.strip().split() for line in lines]
print(parsed_data)
# 将解析后的数据转换为DataFrame
df = pd.DataFrame(parsed_data)

# 保存为Excel文件
df.to_excel('output.xlsx', index=False, header=False)

print("数据已成功保存为output.xlsx")
