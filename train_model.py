import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# ==========================================
# 1. Initialize Spark Session with RAM Optimization
# ==========================================
spark = SparkSession.builder \
    .appName("TrafficAccidentModelTraining") \
    .master("local[*]") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("🚀 Reading Dataset safely into PySpark...")
csv_path = "/opt/spark/work-dir/data/US_Accidents_March23.csv"

# Selected columns
selected_cols = ['Severity', 'Start_Lat', 'Start_Lng', 'Temperature(F)', 'Humidity(%)', 'Pressure(in)', 'Visibility(mi)', 'Wind_Speed(mph)']
feature_cols = ['Start_Lat', 'Start_Lng', 'Temperature(F)', 'Humidity(%)', 'Pressure(in)', 'Visibility(mi)', 'Wind_Speed(mph)']

# ==========================================
# 2. Read CSV & Cast Columns Safely
# ==========================================
raw_df = spark.read.csv(csv_path, header=True) \
    .select(selected_cols) \
    .limit(1000000)  # Safe limit to train on 1 Million rows with zero OOM issues

# Cast features to double & handle missing values
df = raw_df
for col_name in feature_cols:
    df = df.withColumn(col_name, col(col_name).cast("double"))

df = df.fillna(0.0, subset=feature_cols).dropna(subset=['Severity'])

print(f"📊 Dataset Ready! Processing records for training...")

# ==========================================
# 3. Feature Engineering & String Indexer
# ==========================================
# Assemble numeric features into vector
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
assembled_data = assembler.transform(df)

# Index target label safely
indexer = StringIndexer(inputCol="Severity", outputCol="label", handleInvalid="skip")
final_data = indexer.fit(assembled_data).transform(assembled_data)

# ==========================================
# 4. Train / Test Split
# ==========================================
train_data, test_data = final_data.randomSplit([0.8, 0.2], seed=42)

# ==========================================
# 5. Train Random Forest Classifier
# ==========================================
print("🤖 Training Random Forest Model...")
rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=15)
model = rf.fit(train_data)

# ==========================================
# 6. Evaluate Model
# ==========================================
predictions = model.transform(test_data)
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print(f"✅ Model Accuracy: {accuracy * 100:.2f}%")

# ==========================================
# 7. Save Model
# ==========================================
output_dir = "/opt/spark/work-dir/saved_models/rf_accident_model"
model.write().overwrite().save(output_dir)
print(f"💾 Model saved successfully in '{output_dir}'")

spark.stop()