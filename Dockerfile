FROM python:3.11-slim

# Install Mosquitto MQTT broker
RUN apt-get update && apt-get install -y \
    mosquitto \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all project files
COPY . .

# Expose ports:
# PORT (env var) = HTTP dashboard (Railway assigns)
# 1883 = MQTT TCP
# 8765 = WebSocket server (for browser dashboard)
# 9001 = MQTT over WebSocket
EXPOSE 8080 1883 8765 9001

# Start the main app (Mosquitto + HTTP server + WebSocket server)
CMD ["python", "app.py"]
