import pymysql

class database():
    def __init__(self):
        super().__init__()
        self.conn = pymysql.connect(host="database-1.csxyhdnwgd0j.us-east-2.rds.amazonaws.com", 
                               user="admin", port=3306, passwd="TeamAWSome2024$", database = "portfolios")
