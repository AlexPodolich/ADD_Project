FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN pip install -e .

RUN mkdir -p /app/data

ENV PYTHONPATH=/app

RUN echo '#!/bin/bash\n\
echo "Starting rabbitmq setup..."\n\
python -m backend.rabbitmqSetup &\n\
echo "Starting producer..."\n\
python -m backend.producer &\n\
echo "Starting processor..."\n\
python -m backend.processor &\n\
echo "Starting uploader..."\n\
python -m backend.uploader &\n\
echo "Starting aimodel..."\n\
python -m backend.aimodel &\n\
echo "All services started. Waiting..."\n\
wait' > /app/run_services.sh

RUN chmod +x /app/run_services.sh

CMD ["/app/run_services.sh"] 