FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the backend directory
COPY backend/ .
# Copy the data directory
COPY data/ /app/data/

# Create a script to run all Python services
RUN echo '#!/bin/bash\n\
echo "Starting producer..."\n\
python producer.py &\n\
echo "Starting processor..."\n\
python processor.py &\n\
echo "Starting uploader..."\n\
python uploader.py &\n\
echo "Starting aimodel..."\n\
python aimodel.py &\n\
echo "All services started. Waiting..."\n\
wait' > /app/run_services.sh

RUN chmod +x /app/run_services.sh

# Expose the port
EXPOSE 5000

# Run all services
CMD ["/app/run_services.sh"] 