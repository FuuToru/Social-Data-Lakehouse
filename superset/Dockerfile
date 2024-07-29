FROM amancevice/superset

# Copy the configuration file
COPY superset_config.py /app/pythonpath/superset_config.py

# Copy entrypoint.sh to the correct location
COPY entrypoint.sh /entrypoint.sh

# Set permissions using root user
USER root
RUN chmod +x /entrypoint.sh

# Switch back to the superset user
USER superset

ENTRYPOINT ["/entrypoint.sh"]
