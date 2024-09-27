# Inherit from spark-base
FROM spark-base:latest

# Set environment variables for Spark Worker
ENV SPARK_WORKER_UI_PORT=8081
ENV SPARK_WORKER_LOG_DIR=/var/logs/spark
ENV SPARK_MASTER=spark://spark-master:7077

# Copy the start-worker script
COPY start-worker.sh .

# Give executable permission to the start script
RUN chmod +x start-worker.sh

# Expose the UI port for the worker
EXPOSE 8081

# Command to run the Spark Worker
CMD ["./start-worker.sh"]
