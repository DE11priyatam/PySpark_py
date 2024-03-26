


def add(a, b):
    return a + b

def multi(a,b):
    return a*b

def subt(a,b):
    return a-b

# COMMAND ----------

import unittest 

#Creating Class for Unit Testing
class test_class(unittest.TestCase):
    def test_add(self):
          self.assertEqual(10, add(7, 3))
            
    def test_multi(self):
          self.assertEqual(25,multi(5,5))
            
    @unittest.skip("OBSELETE METHOD")
    def test_multi(self):
          self.assertEqual(25,multi(5,5))
    
# create a test suite  using loadTestsFromTestCase()
suite = unittest.TestLoader().loadTestsFromTestCase(test_class)
#Running test cases using Test Cases Suit.
p = (unittest.TextTestRunner(verbosity=2).run(suite))



df_ol = spark.read.option("header", "true").csv("/FileStore/datasets/sales/orderlist.csv")
df_od = spark.read.option("header", "true").csv("/FileStore/datasets/sales/orderdetails.csv")
df_st = spark.read.option("header", "true").csv("/FileStore/datasets/sales/salestarget.csv")



from pyspark.sql.functions import col, round



df_join = df_ol\
        .join(df_od, df_ol["Order ID"]==df_od["Order ID"], "inner")\
        .withColumn("Profit", col("Profit").cast("decimal(10,2)"))\
        .withColumn("Amount", col("Amount").cast("decimal(10,2)"))\
        .select(df_ol["Order ID"], "Amount", "Profit", "Quantity", "Category", "State", "City")\
        .limit(50)
display(df_join)


df_ut = df_join.limit(50)

display(df_ut)



def state_avg_profit(df):
    return df.groupBy("State").mean("Profit").withColumn("avg(Profit)", round(col("avg(Profit)"), 2)).orderBy("State")


def state_total_sales(df):
    return df.groupBy("State").sum("Amount").withColumn("sum(Amount)", round(col("sum(Amount)"), 2)).orderBy("State")



df_avg_profit = state_avg_profit(df_ut)
display(df_avg_profit)

df_total_sales = state_total_sales(df_ut)
display(df_total_sales)



avg_sale_guj = float(df_avg_profit.where(col("State")=="Arizona" ).collect()[0]["avg(Profit)"])
total_sale_guj = float(df_total_sales.where(col("State")=="Arizona" ).collect()[0]["sum(Amount)"])



import unittest 

#Creating Class for Unit Testing
class sales_unit_class(unittest.TestCase):
    
    def test_avg_profit(self):
        expected_avg_sales = -225.60 # This is the value which is expected we calculated using excel
        self.assertEqual(expected_avg_sales, avg_sale_guj)
    
    def test_record_count(self):
        # this is the expected sale, I have put wrong value to fail this test. Correct value should be 1782.0
        #If you will change it to correct value, it will pass the unit test.
        expected_total_sales = 1789.0 
        self.assertEqual(expected_total_sales, total_sale_guj)
    
# create a test suite  for test_class using  loadTestsFromTestCase()
suite = unittest.TestLoader().loadTestsFromTestCase(sales_unit_class)
#Running test cases using Test Cases Suit..
unittest.TextTestRunner(verbosity=2).run(suite)




