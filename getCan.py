import sqlite3
import struct



def export_data_column(db3_file, table_name, column_name, topic_id):
    conn = sqlite3.connect(db3_file)
    cursor = conn.cursor()
  
    query = f"SELECT id, {column_name} FROM {table_name} WHERE topic_id = {topic_id};"
    cursor.execute(query)
    rows = cursor.fetchall()
  
    for row in rows:
        record_id, data = row
        # 假设ROS2消息的前24个字节是Header（12字节的时间戳，4字节的序列号，8字节的frame_id），调整偏移量以获取CAN数据
        offset = 24  # 这是一个常见的偏移量，具体情况需根据消息的定义来调整
        # print(data)
        # print(data[offset:])
        # break
        data_hex = ' '.join(f'{byte:02X}' for byte in data) if isinstance(data, (bytes, bytearray)) else data
        # 写成csv文件, 以空格分隔
        # with open("can.csv", "a") as f:
        print(data_hex) 
        # 写入txt文件
        # with open("can.txt", "a") as f:
        #     # f.write("----------------"*20 + "\n")
        #     f.write(data_hex + "\n")
        
        # print("----------------"*20)
        # print(data_hex)

  
    conn.close()

# 使用示例
db3_file = 'canOnly_1.db3'
topic_id = 1
print(f"从 {db3_file} 中提取 topic_id 为 {topic_id} 的数据")
export_data_column(db3_file, 'messages', 'data', topic_id)
