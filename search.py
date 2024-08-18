import sqlite3
import cv2
import numpy as np
import os

class Db3ImageExtractor:
    def __init__(self, db3_file, table_name='messages', column_name='data', output_folder='./images'):
        self.db3_file = db3_file
        self.table_name = table_name
        self.column_name = column_name
        self.output_folder = output_folder
        self.conn = sqlite3.connect(db3_file)
        self.cursor = self.conn.cursor()
        
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
    
    def __del__(self):
        self.conn.close()

    def extract_and_save_images(self, primary_topic_id, other_topic_ids):
        primary_rows = self._get_records_for_topic(primary_topic_id)
        
        for primary_row in primary_rows:
            primary_timestamp, primary_record_id, primary_data = primary_row
            print(f"Processing primary record ID: {primary_record_id}, timestamp: {primary_timestamp}")
            
            # 保存主相机的图片
            self._save_image(primary_data, f'{primary_timestamp}_{primary_topic_id}.png')

            # 为每个其他的topic_id找到时间戳最接近的记录并保存
            for other_topic_id in other_topic_ids:
                other_row = self._get_closest_record_for_topic(other_topic_id, primary_timestamp)
                
                if other_row:
                    other_timestamp, other_record_id, other_data, time_diff = other_row
                    print(f"Matching record for topic {other_topic_id} found: record ID {other_record_id}, time difference: {time_diff}")
                    self._save_image(other_data, f'{primary_timestamp}_{other_topic_id}.png')
                else:
                    print(f"No matching record found for topic {other_topic_id}")
    
    def _get_records_for_topic(self, topic_id):
        query = f"SELECT timestamp, id, {self.column_name} FROM {self.table_name} WHERE topic_id = {topic_id};"
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def _get_closest_record_for_topic(self, topic_id, primary_timestamp):
        query = f"""
        SELECT timestamp, id, {self.column_name}, ABS(timestamp - {primary_timestamp}) as time_diff 
        FROM {self.table_name} 
        WHERE topic_id = {topic_id}
        ORDER BY time_diff ASC LIMIT 1;
        """
        self.cursor.execute(query)
        return self.cursor.fetchone()

    def _save_image(self, data, output_filename):
        np_data = np.frombuffer(data, np.uint8)
        
        start_idx = np_data.tobytes().find(b'\xff\xd8')
        end_idx = np_data.tobytes().rfind(b'\xff\xd9') + 2
        
        if start_idx != -1 and end_idx != -1:
            jpeg_data = np_data[start_idx:end_idx]
            image = cv2.imdecode(jpeg_data, cv2.IMREAD_COLOR)
            
            if image is not None:
                output_path = os.path.join(self.output_folder, output_filename)
                image_rgb = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
                cv2.imwrite(output_path, image_rgb)
                print(f"Image saved successfully as {output_path}, size: {image.shape[1]}x{image.shape[0]}")
            else:
                print(f"Failed to decode image for {output_filename}")
        else:
            print(f"JPEG markers not found for {output_filename}")

# 使用示例
db3_file = 'mini_0.db3'
primary_topic_id = 13
other_topic_ids = [15, 16, 17, 18, 19]

extractor = Db3ImageExtractor(db3_file)
extractor.extract_and_save_images(primary_topic_id, other_topic_ids)
